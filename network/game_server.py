import json
import logging
import os
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock, Thread, current_thread

from config import Config
from network.game_session import GameSession
from network.packet_factory import PacketFactory
from server import (
    entity_authority,
    entity_store,
    etcd_authority,
    lobby_state,
    replicated_events,
    replicated_roster,
    shared_projection,
)
from server.scene_data import _session_list as session_list, lock_session, remove_recorder
import server.notice_sync as notice_sync

logger = logging.getLogger(__name__)


class GameServer:
    MAX_CONNECTIONS = 512
    ACCEPT_TIMEOUT = 0.5

    def __init__(self):
        self.port = Config.GAME_SERVER_PORT
        self.server_socket = None
        self.running = False
        self.startup_error = None
        self.notice_thread = None
        self._started = Event()
        self._stopped = Event()
        self._stop_lock = Lock()
        self._maintenance_stop = Event()
        self._maintenance_thread = None
        self._last_snapshot_at = 0.0
        self._session_pool = ThreadPoolExecutor(
            max_workers=Config.SESSION_POOL_MAX_WORKERS, thread_name_prefix="session"
        )
        self.host_id = Config.LOBBY_HOST_ID
        self.epoch = 1
        self.lease = lobby_state.LobbyHostLease(
            self.host_id, self.epoch, Config.LOBBY_LEASE_SECONDS
        )
        self.authority = None
        self.authority_credential = None
        self.event_log = None
        self.roster_replicator = None
        self._snapshot_writes_allowed = True

    def start(self):
        """Bind and serve until stop() is called.  Instances are single-use."""
        try:
            with self._stop_lock:
                if self._started.is_set() or self._stopped.is_set():
                    raise RuntimeError("GameServer instance cannot be started twice")
                PacketFactory.initialize_handlers(Config.HANDLERS_PACKAGE)
                if Config.LOBBY_ETCD_ENDPOINT:
                    self._snapshot_writes_allowed = False
                    client = etcd_authority.EtcdClient(
                        Config.LOBBY_ETCD_ENDPOINT,
                        ca_cert=Config.LOBBY_ETCD_CA_CERT,
                        cert=Config.LOBBY_ETCD_CERT,
                        key=Config.LOBBY_ETCD_KEY,
                        username=Config.LOBBY_ETCD_USERNAME,
                        password=Config.LOBBY_ETCD_PASSWORD,
                    )
                    self.authority = etcd_authority.ChannelAuthority(
                        client, lease_seconds=Config.LOBBY_LEASE_SECONDS
                    )
                    self.authority_credential = self.authority.acquire(
                        Config.LOBBY_CHANNEL_ID,
                        self.host_id,
                        Config.LOBBY_CONFIG_GENERATION,
                    )
                    self.authority_credential = self.authority.commit_noop(
                        self.authority_credential
                    )
                    self.epoch = self.authority_credential.app_term
                    self.lease = lobby_state.LobbyHostLease(
                        self.host_id, self.epoch, Config.LOBBY_LEASE_SECONDS
                    )
                    snapshot_data = self.authority.load_snapshot(
                        self.authority_credential.channel_id
                    )
                    snapshot_source = "etcd"
                    if snapshot_data is None and Config.LOBBY_STATE_PATH:
                        saved = lobby_state.load_lobby_state(Config.LOBBY_STATE_PATH)
                        if saved is not None:
                            snapshot_data = saved
                            snapshot_source = Config.LOBBY_STATE_PATH
                    if snapshot_data is None:
                        shared_projection.store.set_authority(
                            Config.LOBBY_CONFIG_GENERATION, self.epoch
                        )
                    else:
                        if isinstance(snapshot_data, (bytes, bytearray)):
                            try:
                                snapshot_data = json.loads(snapshot_data)
                            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                                raise lobby_state.LobbyStateError(
                                    "etcd lobby snapshot encoding is invalid"
                                ) from error
                        restored_epoch = self._restore_recovery_state(
                            snapshot_data, self.authority_credential
                        )
                        logger.info(
                            "Restored lobby snapshot from %s at data epoch %s "
                            "under authority epoch %s",
                            snapshot_source,
                            restored_epoch,
                            self.epoch,
                        )
                    self._snapshot_writes_allowed = True
                    owner = entity_authority.active()
                    if owner is not None:
                        owner.configure_store(
                            entity_store.EntityOwnershipStore(
                                client, Config.LOBBY_CHANNEL_ID
                            ),
                            lambda: self.authority_credential,
                        )
                    self.event_log = replicated_events.ReplicatedEventLog(
                        self.authority, lambda: self.authority_credential
                    )
                    replicated_events.activate(self.event_log)
                    self.roster_replicator = replicated_roster.RosterReplicator(
                        self.authority,
                        lambda: self.authority_credential,
                        paused=Config.LINK_OTHER_SERVER,
                    )
                    replicated_roster.activate(self.roster_replicator)
                else:
                    if Config.LOBBY_STATE_PATH:
                        saved = lobby_state.load_lobby_state(Config.LOBBY_STATE_PATH)
                        if saved is not None:
                            lobby_state.restore_lobby_state(saved, current_epoch=0)
                            self.epoch = int(saved["epoch"]) + 1
                            self.lease = lobby_state.LobbyHostLease(
                                self.host_id,
                                self.epoch,
                                Config.LOBBY_LEASE_SECONDS,
                            )
                            logger.info(
                                "Restored lobby snapshot from %s at epoch %s",
                                Config.LOBBY_STATE_PATH,
                                self.epoch,
                            )
                    shared_projection.store.set_authority(1, self.epoch)
                lobby_state.activate_host_lease(self.lease)
                self._discard_stale_sessions()

                self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                if os.name == "nt" and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                    # Windows SO_REUSEADDR can let a second process hijack a
                    # listening port.  Exclusive bind still releases cleanly
                    # when this listener and its accepted sockets are closed.
                    self.server_socket.setsockopt(
                        socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1
                    )
                else:
                    self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                # Do not use SO_REUSEPORT: two processes must never silently
                # share a game port after an accidental double start.
                self.server_socket.settimeout(self.ACCEPT_TIMEOUT)
                self.server_socket.bind((Config.GAME_SERVER_IP, self.port))
                self.server_socket.listen(min(self.MAX_CONNECTIONS, 1024))
                self.server_socket.setblocking(True)
                # settimeout() is retained for accept; it does not make accepted
                # sockets blocking, so reset their mode explicitly.
                self.running = True
                logger.info("Game server started on port %s", self.server_socket.getsockname()[1])
                logger.info("For help, type 'help'")
                self.notice_thread = notice_sync.init()
                self._maintenance_stop.clear()
                self._maintenance_thread = Thread(
                    target=self._maintenance_loop,
                    daemon=True,
                    name="lobby-host-maintenance",
                )
                self._maintenance_thread.start()

            self._started.set()
            while self.running:
                try:
                    client_socket, address = self.server_socket.accept()
                except socket.timeout:
                    self._expire_queued_sessions()
                    continue
                except OSError:
                    if self.running:
                        raise
                    break

                if not self.running:
                    client_socket.close()
                    break
                client_socket.setblocking(True)
                logger.debug("New connection from %s", address)

                try:
                    session = GameSession(client_socket, address)
                except Exception:
                    logger.exception("Unable to initialize session from %s", address)
                    client_socket.close()
                    continue

                with lock_session:
                    local_count = sum(
                        1 for item in session_list if not getattr(item, "remote", False)
                    )
                    if local_count >= self.MAX_CONNECTIONS:
                        reject = True
                    else:
                        session_list.append(session)
                        reject = False
                if reject:
                    logger.warning("Connection limit reached; rejecting %s", address)
                    session.close()
                    continue

                try:
                    self._session_pool.submit(session.run)
                except RuntimeError:
                    with lock_session:
                        session_list[:] = [item for item in session_list if item is not session]
                    session.close()
                    if self.running:
                        raise
                    break
        except Exception as error:
            self.startup_error = str(error)
            self.running = False
            logger.exception("Failed to start server: %s", error)
            self._started.set()
            self.stop()
        finally:
            # Also releases wait_started() when stop races with startup.
            self._started.set()
            if not self.running and not self._stopped.is_set():
                self.stop()

    @staticmethod
    def _restore_recovery_state(state, credential):
        state = lobby_state.validate_lobby_state(state)
        if state.get("channel_id") != credential.channel_id:
            raise lobby_state.LobbyStateError(
                "lobby snapshot channel does not match channel authority"
            )
        if state.get("config_generation", 1) != credential.config_generation:
            raise lobby_state.LobbyStateError(
                "lobby snapshot config generation does not match channel authority"
            )
        current = shared_projection.store.roster_metadata()
        if (
            current["config_generation"] > credential.config_generation
            or current["authority_epoch"] > credential.app_term
        ):
            raise lobby_state.LobbyStateError(
                "lobby snapshot would move channel authority backwards"
            )
        if (
            state["epoch"] > credential.app_term
            or state.get("authority_epoch", state["epoch"]) > credential.app_term
        ):
            raise lobby_state.LobbyStateError(
                "lobby snapshot is newer than channel authority"
            )
        lobby_state.restore_lobby_state(
            state,
            current_epoch=0,
            authority_epoch=credential.app_term,
        )
        return state["epoch"]

    def _maintenance_loop(self):
        interval = min(
            Config.LOBBY_LEASE_SECONDS / 3.0,
            Config.LOBBY_SNAPSHOT_INTERVAL_SEC,
        )
        while not self._maintenance_stop.wait(interval):
            try:
                if self.authority is not None:
                    self.authority_credential = self.authority.renew(
                        self.authority_credential
                    )
                self.lease.renew()
            except Exception:
                logger.exception("Lobby host lease renewal failed")
                self.running = False
                return
            if (
                Config.LOBBY_STATE_PATH
                and time.monotonic() - self._last_snapshot_at
                >= Config.LOBBY_SNAPSHOT_INTERVAL_SEC
            ):
                try:
                    for projection in shared_projection.store.prune_restored(
                        Config.LOBBY_RESTORED_PROJECTION_TTL_SEC
                    ):
                        remove_recorder(
                            projection.scene_id,
                            projection.channel_id,
                            projection.player_id,
                        )
                    snapshot = lobby_state.capture_lobby_state(
                        self.host_id, self.epoch, Config.LOBBY_CHANNEL_ID
                    )
                    lobby_state.persist_lobby_state(
                        Config.LOBBY_STATE_PATH,
                        snapshot,
                    )
                    if self.authority is not None:
                        self.authority.commit_snapshot(
                            self.authority_credential,
                            json.dumps(
                                snapshot, ensure_ascii=False, separators=(",", ":")
                            ).encode(),
                        )
                    self._last_snapshot_at = time.monotonic()
                except Exception:
                    logger.exception("Unable to persist periodic lobby snapshot")

    def _expire_queued_sessions(self):
        now = time.monotonic()
        with lock_session:
            expired = [
                session
                for session in session_list
                if not getattr(session, "remote", False)
                and not getattr(session, "run_started", False)
                and now - getattr(session, "created_at", now) >= GameSession.AUTH_TIMEOUT
            ]
            for session in expired:
                session_list[:] = [item for item in session_list if item is not session]
        for session in expired:
            logger.debug("Expiring queued session %s", session.address)
            session.close()

    def _discard_stale_sessions(self):
        with lock_session:
            stale = list(session_list)
            session_list.clear()
        for session in stale:
            try:
                session.close()
            except Exception:
                logger.exception("Unable to close stale session")

    def stop(self):
        """Idempotent, serialized shutdown; all owned workers finish before return."""
        with self._stop_lock:
            if self._stopped.is_set():
                return
            self.running = False
            self._started.set()
            self._maintenance_stop.set()
            maintenance_thread = self._maintenance_thread
            self._maintenance_thread = None
            if (
                maintenance_thread is not None
                and maintenance_thread is not current_thread()
            ):
                maintenance_thread.join()
            first_error = None

            if self._snapshot_writes_allowed and (
                Config.LOBBY_STATE_PATH or self.authority is not None
            ):
                try:
                    snapshot = lobby_state.capture_lobby_state(
                        self.host_id, self.epoch, Config.LOBBY_CHANNEL_ID
                    )
                    if Config.LOBBY_STATE_PATH:
                        lobby_state.persist_lobby_state(
                            Config.LOBBY_STATE_PATH,
                            snapshot,
                        )
                    if self.authority is not None:
                        self.authority.commit_snapshot(
                            self.authority_credential,
                            json.dumps(
                                snapshot, ensure_ascii=False, separators=(",", ":")
                            ).encode(),
                        )
                except Exception as error:
                    first_error = error
                    logger.exception("Unable to persist lobby snapshot")

            listener = self.server_socket
            self.server_socket = None
            if listener is not None:
                try:
                    listener.close()
                except OSError:
                    pass

            try:
                with lock_session:
                    sessions = list(session_list)
                for session in sessions:
                    try:
                        session.close()
                    except Exception:
                        logger.exception("Unable to close session")
            except Exception as error:
                first_error = error
                logger.exception("Session shutdown failed")

            try:
                self._session_pool.shutdown(wait=True, cancel_futures=True)
            except Exception as error:
                first_error = first_error or error
                logger.exception("Session pool shutdown failed")

            # No handler may write to the database after notice_sync performs
            # its final logout save.
            try:
                PacketFactory.shutdown()
            except Exception as error:
                first_error = first_error or error
                logger.exception("Packet pool shutdown failed")

            if self.notice_thread is not None:
                try:
                    notice_sync.stop()
                    self.notice_thread.join()
                except Exception as error:
                    first_error = first_error or error
                    logger.exception("Notice sync shutdown failed")

            try:
                with lock_session:
                    remaining = list(session_list)
                    session_list.clear()
                for session in remaining:
                    try:
                        session.close()
                    except Exception:
                        logger.exception("Unable to remove residual session")
            except Exception as error:
                first_error = first_error or error
                logger.exception("Session cleanup failed")
            finally:
                self._stopped.set()

            lobby_state.deactivate_host_lease(self.lease)
            if self.event_log is not None:
                replicated_events.deactivate(self.event_log)
            if self.roster_replicator is not None:
                replicated_roster.deactivate(self.roster_replicator)
            owner = entity_authority.active()
            if owner is not None:
                owner.configure_store(None, None)
            if self.authority is not None and self.authority_credential is not None:
                try:
                    self.authority.release(self.authority_credential)
                except Exception:
                    first_error = first_error or RuntimeError(
                        "Unable to release channel authority lease"
                    )
                    logger.exception("Unable to release channel authority lease")
            if first_error is not None:
                raise RuntimeError("GameServer shutdown completed with errors") from first_error

    def wait_started(self, timeout=None):
        if not self._started.wait(timeout):
            return False
        if self.startup_error:
            raise RuntimeError(self.startup_error)
        return self.running

    @property
    def stopped(self):
        return self._stopped.is_set()
