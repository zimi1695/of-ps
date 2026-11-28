"""Lobby snapshot persistence and host lease primitives."""

import base64
import json
import math
import os
import time
import uuid
from contextlib import contextmanager
from threading import Lock, RLock

from google.protobuf.message import DecodeError
from proto.net_pb2 import PlayerRecorderData
from server import shared_projection
from server.scene_data import export_scene_snapshots, import_scene_snapshots

STATE_VERSION = 1


class LobbyStateError(ValueError):
    pass


class StaleEpochError(LobbyStateError):
    pass


def capture_lobby_state(host_id, epoch, channel_id=None):
    if channel_id is not None and (
        not isinstance(channel_id, str) or not channel_id
    ):
        raise LobbyStateError("lobby state channel_id is invalid")
    scene_snapshots = {}
    with shared_projection.store.locked():
        for scene_id, channels in export_scene_snapshots().items():
            scene_snapshots[str(scene_id)] = {}
            for scene_channel_id, players in channels.items():
                scene_snapshots[str(scene_id)][str(scene_channel_id)] = {
                    str(player_id): base64.b64encode(payload).decode("ascii")
                    for player_id, payload in players.items()
                }
        projections = {
            str(player_id): projection
            for player_id, projection in shared_projection.store.snapshot().items()
        }
    state = {
        "version": STATE_VERSION,
        "host_id": str(host_id),
        "epoch": int(epoch),
        "captured_at": time.time(),
        **shared_projection.store.roster_metadata(),
        "projections": projections,
        "scene_snapshots": scene_snapshots,
    }
    if channel_id is not None:
        state["channel_id"] = channel_id
    return state


def validate_lobby_state(state):
    if not isinstance(state, dict):
        raise LobbyStateError("lobby state must be an object")
    if state.get("version") != STATE_VERSION:
        raise LobbyStateError("unsupported lobby state version")
    if not isinstance(state.get("host_id"), str) or not state["host_id"]:
        raise LobbyStateError("lobby state host_id is invalid")
    if "channel_id" in state and (
        not isinstance(state["channel_id"], str) or not state["channel_id"]
    ):
        raise LobbyStateError("lobby state channel_id is invalid")
    epoch = state.get("epoch")
    if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 1:
        raise LobbyStateError("lobby state epoch is invalid")
    if not isinstance(state.get("projections"), dict):
        raise LobbyStateError("lobby state projections are invalid")
    if not isinstance(state.get("scene_snapshots"), dict):
        raise LobbyStateError("lobby state scene snapshots are invalid")
    for field, default, minimum in (
        ("config_generation", 1, 1),
        ("authority_epoch", int(state["epoch"]), 1),
        ("roster_seq", 0, 0),
    ):
        value = state.get(field, default)
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < minimum
        ):
            raise LobbyStateError(f"lobby state {field} is invalid")
    return state


def restore_lobby_state(
    state, current_epoch=0, allow_equal=False, authority_epoch=None
):
    validate_lobby_state(state)
    if authority_epoch is not None and (
        isinstance(authority_epoch, bool)
        or not isinstance(authority_epoch, int)
        or authority_epoch < 1
    ):
        raise LobbyStateError("current authority epoch is invalid")
    epoch = int(state["epoch"])
    if epoch < current_epoch or (epoch == current_epoch and not allow_equal):
        raise StaleEpochError(
            f"lobby state epoch {epoch} is stale; current epoch is {current_epoch}"
        )

    projections = state["projections"]
    try:
        for player_id, projection in projections.items():
            if not isinstance(projection, dict):
                raise LobbyStateError("projection must be an object")
            if projection.get("player_id") != int(player_id):
                raise LobbyStateError("projection player_id mismatch")
            for field in ("scene_id", "channel_id", "char_id", "version"):
                value = projection.get(field, 0)
                if isinstance(value, bool) or not isinstance(value, int):
                    raise LobbyStateError(f"projection {field} is invalid")
            if projection["version"] < 1:
                raise LobbyStateError("projection version is invalid")
            for field in ("position", "rotation"):
                vector = projection.get(field)
                if not isinstance(vector, (list, tuple)) or len(vector) != 3:
                    raise LobbyStateError(f"projection {field} is invalid")
                if any(
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                    for value in vector
                ):
                    raise LobbyStateError(f"projection {field} is invalid")
            updated_at = projection.get("updated_at", 0.0)
            if (
                isinstance(updated_at, bool)
                or not isinstance(updated_at, (int, float))
                or not math.isfinite(updated_at)
                or updated_at < 0
            ):
                raise LobbyStateError("projection updated_at is invalid")
            if not isinstance(projection.get("restored", True), bool):
                raise LobbyStateError("projection restored flag is invalid")
    except (TypeError, ValueError) as error:
        if isinstance(error, LobbyStateError):
            raise
        raise LobbyStateError("projection data is invalid") from error

    decoded_scene_snapshots = {}
    try:
        for scene_id, channels in state["scene_snapshots"].items():
            decoded_scene_snapshots[int(scene_id)] = {}
            for channel_id, players in channels.items():
                decoded_players = decoded_scene_snapshots[int(scene_id)][
                    int(channel_id)
                ] = {}
                for player_id, payload in players.items():
                    recorder = PlayerRecorderData()
                    recorder.ParseFromString(base64.b64decode(payload, validate=True))
                    decoded_players[int(player_id)] = recorder.SerializeToString()
    except (TypeError, ValueError, DecodeError) as error:
        raise LobbyStateError("lobby state scene snapshot encoding is invalid") from error

    shared_projection.store.restore(
        projections,
        roster_seq=state.get("roster_seq", 0),
        config_generation=state.get("config_generation", 1),
        authority_epoch=(
            state.get("authority_epoch", epoch)
            if authority_epoch is None
            else authority_epoch
        ),
    )
    import_scene_snapshots(decoded_scene_snapshots)
    return epoch


_state_path_locks = {}
_state_path_locks_guard = Lock()


def _state_path_lock(target):
    with _state_path_locks_guard:
        return _state_path_locks.setdefault(target, RLock())


@contextmanager
def _locked_state_file(target):
    with _state_path_lock(target):
        lock_path = target + ".lock"
        with open(lock_path, "a+b") as lock_handle:
            lock_handle.seek(0, os.SEEK_END)
            if lock_handle.tell() == 0:
                lock_handle.write(b"\0")
                lock_handle.flush()
            lock_handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(lock_handle.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                lock_handle.seek(0)
                if os.name == "nt":
                    msvcrt.locking(lock_handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def _write_lobby_state(target, state):
    temporary = target + ".tmp-" + uuid.uuid4().hex
    try:
        with open(temporary, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def persist_lobby_state(path, state):
    validate_lobby_state(state)
    target = os.path.abspath(path)
    os.makedirs(os.path.dirname(target), exist_ok=True)
    with _locked_state_file(target):
        current = load_lobby_state(target)
        if current is not None:
            if state["epoch"] < current["epoch"] or (
                state["epoch"] == current["epoch"]
                and state["host_id"] != current["host_id"]
            ):
                raise StaleEpochError(
                    f"lobby state epoch {state['epoch']} from {state['host_id']} "
                    f"cannot replace epoch {current['epoch']} from {current['host_id']}"
                )
        _write_lobby_state(target, state)


def load_lobby_state(path):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            state = json.load(handle)
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as error:
        raise LobbyStateError("unable to read lobby state") from error
    return validate_lobby_state(state)


def prepare_lobby_failover(path, new_host_id, expected_epoch=None):
    if not isinstance(new_host_id, str) or not new_host_id:
        raise LobbyStateError("new host_id is invalid")
    target = os.path.abspath(path)
    os.makedirs(os.path.dirname(target), exist_ok=True)
    with _locked_state_file(target):
        state = load_lobby_state(target)
        if state is None:
            raise LobbyStateError("lobby state does not exist")
        if expected_epoch is not None and state["epoch"] != int(expected_epoch):
            raise StaleEpochError(
                f"lobby state epoch {state['epoch']} does not match {expected_epoch}"
            )
        state["host_id"] = new_host_id
        state["epoch"] += 1
        state["failover_at"] = time.time()
        _write_lobby_state(target, state)
        return state


class LobbyHostLease:
    def __init__(self, host_id, epoch=1, lease_seconds=10.0, clock=time.monotonic):
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.host_id = str(host_id)
        self.epoch = int(epoch)
        self.lease_seconds = float(lease_seconds)
        self._clock = clock
        self._lock = Lock()
        self._lease_until = self._clock() + self.lease_seconds

    def renew(self, epoch=None):
        with self._lock:
            if epoch is not None and int(epoch) != self.epoch:
                raise StaleEpochError("cannot renew a lease with a stale epoch")
            current = self._clock()
            if current >= self._lease_until:
                raise StaleEpochError("cannot renew an expired host lease")
            self._lease_until = current + self.lease_seconds

    def is_valid(self, now=None):
        with self._lock:
            current = self._clock() if now is None else now
            return current < self._lease_until

    def assert_valid(self, now=None):
        if not self.is_valid(now):
            raise StaleEpochError("host lease has expired")

    def acquire_if_expired(self, host_id, now=None):
        with self._lock:
            current = self._clock() if now is None else now
            if current < self._lease_until:
                return False
            self.host_id = str(host_id)
            self.epoch += 1
            self._lease_until = current + self.lease_seconds
            return True

    def snapshot(self):
        with self._lock:
            return {
                "host_id": self.host_id,
                "epoch": self.epoch,
                "lease_until": self._lease_until,
            }


_active_lease = None
_active_lease_lock = Lock()


def activate_host_lease(lease):
    global _active_lease
    with _active_lease_lock:
        _active_lease = lease


def deactivate_host_lease(lease):
    global _active_lease
    with _active_lease_lock:
        if _active_lease is lease:
            _active_lease = None


def host_lease_is_valid():
    with _active_lease_lock:
        lease = _active_lease
    return lease is None or lease.is_valid()


def assert_host_authority():
    with _active_lease_lock:
        lease = _active_lease
    if lease is not None:
        lease.assert_valid()
