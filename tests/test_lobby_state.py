import base64
import copy
import json
import multiprocessing
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from config import Config
from network.game_server import GameServer
from network.game_session import GameSession
from network.msg_id import MsgId
from network.packet_factory import PacketFactory
from proto.net_pb2 import (
    CharacterRecorderData,
    PacketHead,
    PlayerRecorderData,
    Vector3,
)
from server import (
    etcd_authority,
    lobby_state,
    replicated_events,
    replicated_roster,
    scene_data,
    shared_projection,
)
from server import notice_sync
from utils.metrics import metrics


class FakeSocket:
    def setsockopt(self, *args):
        pass

    def shutdown(self, *args):
        pass

    def close(self):
        pass


def recorder(x, y=0, z=0):
    data = PlayerRecorderData()
    record = data.char_recorder_data_lst.add()
    record.char_id = 1
    record.pos.CopyFrom(Vector3(x=x, y=y, z=z))
    return data


def _persist_worker(path, state, start, results):
    try:
        start.wait(5)
        lobby_state.persist_lobby_state(path, state)
        results.put("persisted")
    except Exception as error:
        results.put(type(error).__name__)


def _failover_worker(path, start, results):
    try:
        start.wait(5)
        lobby_state.prepare_lobby_failover(path, "host-b", expected_epoch=7)
        results.put("failed-over")
    except Exception as error:
        results.put(type(error).__name__)


class LobbyStateTests(unittest.TestCase):
    def setUp(self):
        shared_projection.store.clear()
        scene_data.clear_scene_snapshots()
        metrics.reset()

    def tearDown(self):
        shared_projection.store.clear()
        scene_data.clear_scene_snapshots()
        metrics.reset()

    def populate(self):
        shared_projection.store.update(
            shared_projection.SharedProjection(
                player_id=1000000,
                scene_id=1,
                channel_id=2,
                char_id=7,
                position=(1.0, 2.0, 3.0),
                rotation=(4.0, 5.0, 6.0),
                action_id=99,
                emoji_id=88,
                version=3,
            )
        )
        scene_data.up_recorder(1, 2, 1000000, recorder(1, 2, 3))

    def test_capture_restore_roundtrip(self):
        self.populate()
        state = lobby_state.capture_lobby_state("host-a", 4)
        shared_projection.store.clear()
        scene_data.clear_scene_snapshots()

        epoch = lobby_state.restore_lobby_state(state, current_epoch=3)
        self.assertEqual(epoch, 4)
        projection = shared_projection.store.get(1000000)
        self.assertEqual(projection.position, (1.0, 2.0, 3.0))
        self.assertEqual(projection.rotation, (4.0, 5.0, 6.0))
        self.assertEqual(projection.action_id, 99)
        self.assertEqual(projection.emoji_id, 88)
        self.assertEqual(projection.version, 3)
        self.assertTrue(projection.restored)
        snapshot = scene_data.get_scene_snapshot(1, 2)
        self.assertIn(1000000, snapshot)
        self.assertEqual(snapshot[1000000].char_recorder_data_lst[0].pos.x, 1)

    def test_capture_records_channel_for_etcd_recovery(self):
        state = lobby_state.capture_lobby_state("host-a", 4, "public")
        self.assertEqual(state["channel_id"], "public")
        state["channel_id"] = ""
        with self.assertRaises(lobby_state.LobbyStateError):
            lobby_state.validate_lobby_state(state)

    def test_restore_can_preserve_roster_baseline_with_new_authority_epoch(self):
        self.populate()
        state = lobby_state.capture_lobby_state("host-a", 4, "public")
        baseline = state["roster_seq"]
        shared_projection.store.clear()
        scene_data.clear_scene_snapshots()

        lobby_state.restore_lobby_state(
            state, current_epoch=0, authority_epoch=9
        )
        metadata = shared_projection.store.roster_metadata()
        self.assertEqual(metadata["authority_epoch"], 9)
        self.assertEqual(metadata["roster_seq"], baseline)
        self.assertIsNotNone(shared_projection.store.get(1000000))

    def test_stale_epoch_is_rejected_without_mutating_state(self):
        self.populate()
        state = lobby_state.capture_lobby_state("host-a", 2)
        with self.assertRaises(lobby_state.StaleEpochError):
            lobby_state.restore_lobby_state(state, current_epoch=3)
        self.assertIsNotNone(shared_projection.store.get(1000000))

    def test_persist_and_load_atomic_json(self):
        self.populate()
        state = lobby_state.capture_lobby_state("host-a", 5)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lobby.json"
            lobby_state.persist_lobby_state(path, state)
            loaded = lobby_state.load_lobby_state(path)
        self.assertEqual(loaded["epoch"], 5)
        self.assertEqual(loaded["host_id"], "host-a")
        self.assertIn("1000000", loaded["projections"])

    def test_automatic_path_rejects_stale_epoch_without_writing(self):
        self.populate()
        state = lobby_state.capture_lobby_state("host-a", 7)
        stale = lobby_state.capture_lobby_state("host-a", 6)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lobby.json"
            lobby_state.persist_lobby_state(path, state)
            with self.assertRaises(lobby_state.StaleEpochError):
                lobby_state.persist_lobby_state(path, stale)
            loaded = lobby_state.load_lobby_state(path)
        self.assertEqual(loaded["epoch"], 7)
        self.assertEqual(loaded["host_id"], "host-a")

    def test_same_epoch_cannot_replace_another_host(self):
        state = lobby_state.capture_lobby_state("host-a", 7)
        other = lobby_state.capture_lobby_state("host-b", 7)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lobby.json"
            lobby_state.persist_lobby_state(path, state)
            with self.assertRaises(lobby_state.StaleEpochError):
                lobby_state.persist_lobby_state(path, other)
            self.assertEqual(lobby_state.load_lobby_state(path)["host_id"], "host-a")

    def test_cross_process_failover_cannot_be_overwritten_by_stale_snapshot(self):
        self.populate()
        state = lobby_state.capture_lobby_state("host-a", 7)
        stale = lobby_state.capture_lobby_state("host-a", 7)
        context = multiprocessing.get_context("spawn")
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "lobby.json")
            lobby_state.persist_lobby_state(path, state)
            start = context.Event()
            results = context.Queue()
            workers = [
                context.Process(
                    target=_persist_worker, args=(path, stale, start, results)
                ),
                context.Process(target=_failover_worker, args=(path, start, results)),
            ]
            for worker in workers:
                worker.start()
            start.set()
            for worker in workers:
                worker.join(timeout=10)
                if worker.is_alive():
                    worker.terminate()
                    worker.join()
                    self.fail("state lock worker did not finish")
            outcomes = [results.get(timeout=2) for _ in workers]
            loaded = lobby_state.load_lobby_state(path)
        self.assertIn("failed-over", outcomes)
        self.assertEqual(loaded["epoch"], 8)
        self.assertEqual(loaded["host_id"], "host-b")

    def test_corrupt_state_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lobby.json"
            path.write_text("{not-json", encoding="utf-8")
            with self.assertRaises(lobby_state.LobbyStateError):
                lobby_state.load_lobby_state(path)

    def test_invalid_projection_vector_is_rejected(self):
        self.populate()
        state = lobby_state.capture_lobby_state("host-a", 1)
        state["projections"]["1000000"]["position"] = [1.0, 2.0]
        with self.assertRaises(lobby_state.LobbyStateError):
            lobby_state.restore_lobby_state(state, current_epoch=0)

    def test_non_finite_projection_vector_is_rejected(self):
        self.populate()
        state = lobby_state.capture_lobby_state("host-a", 1)
        state["projections"]["1000000"]["position"] = [1.0, float("nan"), 3.0]
        with self.assertRaises(lobby_state.LobbyStateError):
            lobby_state.restore_lobby_state(state, current_epoch=0)

    def test_corrupt_scene_payload_is_rejected_without_mutating_snapshot(self):
        self.populate()
        state = lobby_state.capture_lobby_state("host-a", 1)
        state["scene_snapshots"]["1"]["2"]["1000000"] = base64.b64encode(
            b"not-a-protobuf"
        ).decode("ascii")
        with self.assertRaises(lobby_state.LobbyStateError):
            lobby_state.restore_lobby_state(state, current_epoch=0)
        self.assertIn(1000000, scene_data.get_scene_snapshot(1, 2))

    def test_game_server_stop_persists_snapshot_when_configured(self):
        self.populate()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lobby.json"
            with patch.object(Config, "LOBBY_STATE_PATH", str(path)), patch.object(
                Config, "LOBBY_HOST_ID", "host-test"
            ), patch.object(Config, "LOBBY_LEASE_SECONDS", 5.0):
                server = GameServer()
                server.stop()
            loaded = lobby_state.load_lobby_state(path)
        self.assertEqual(loaded["host_id"], "host-test")
        self.assertIn("1000000", loaded["projections"])

    def test_prepare_failover_increments_epoch_and_preserves_projection(self):
        self.populate()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lobby.json"
            lobby_state.persist_lobby_state(
                path, lobby_state.capture_lobby_state("host-a", 7)
            )
            state = lobby_state.prepare_lobby_failover(
                path, "host-b", expected_epoch=7
            )
            loaded = lobby_state.load_lobby_state(path)
        self.assertEqual(state["epoch"], 8)
        self.assertEqual(loaded["host_id"], "host-b")
        self.assertIn("1000000", loaded["projections"])

    def test_prepare_failover_rejects_unexpected_epoch_without_writing(self):
        self.populate()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lobby.json"
            lobby_state.persist_lobby_state(
                path, lobby_state.capture_lobby_state("host-a", 7)
            )
            with self.assertRaises(lobby_state.StaleEpochError):
                lobby_state.prepare_lobby_failover(
                    path, "host-b", expected_epoch=6
                )
            loaded = lobby_state.load_lobby_state(path)
        self.assertEqual(loaded["epoch"], 7)
        self.assertEqual(loaded["host_id"], "host-a")

    def test_expired_host_lease_rejects_session_packet_before_handler(self):
        now = [100.0]
        lease = lobby_state.LobbyHostLease(
            "host-a", 1, 1.0, clock=lambda: now[0]
        )
        lobby_state.activate_host_lease(lease)
        now[0] = 102.0
        session = GameSession(FakeSocket(), ("synthetic", 1))
        session.verified = True
        head = PacketHead(msg_id=MsgId.PlayerSceneRecordReq, packet_id=1, body_len=0)
        payload = head.SerializeToString()
        packet = len(payload).to_bytes(2, "big") + payload
        session._recv_buf[: len(packet)] = packet
        session._recv_len = len(packet)
        try:
            with patch("network.game_session.PacketFactory.process_packet") as process:
                session._process_buffer()
            process.assert_not_called()
            self.assertFalse(session.running)
            self.assertEqual(
                metrics.snapshot()["counters"]["ofps.lobby.stale_host_requests_total"],
                1,
            )
        finally:
            session.close()
            lobby_state.deactivate_host_lease(lease)

    def test_maintenance_loop_renews_lease(self):
        renewed = threading.Event()
        with patch.object(Config, "LOBBY_LEASE_SECONDS", 0.2), patch.object(
            Config, "LOBBY_SNAPSHOT_INTERVAL_SEC", 100.0
        ):
            server = GameServer()
            original_renew = server.lease.renew

            def renew():
                original_renew()
                renewed.set()

            server.lease.renew = renew
            thread = threading.Thread(target=server._maintenance_loop)
            thread.start()
            try:
                self.assertTrue(renewed.wait(1.0))
                self.assertTrue(server.lease.is_valid())
            finally:
                server._maintenance_stop.set()
                thread.join(timeout=1.0)
                server.stop()


class GameServerEtcdRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.old_sessions = list(scene_data._session_list)
        scene_data._session_list.clear()
        shared_projection.store.clear()
        scene_data.clear_scene_snapshots()
        metrics.reset()

    def tearDown(self):
        scene_data._session_list[:] = self.old_sessions
        shared_projection.store.clear()
        scene_data.clear_scene_snapshots()
        metrics.reset()

    def populate(self):
        shared_projection.store.update(
            shared_projection.SharedProjection(
                player_id=1000000,
                scene_id=1,
                channel_id=2,
                char_id=7,
                position=(1.0, 2.0, 3.0),
                rotation=(4.0, 5.0, 6.0),
                action_id=99,
                emoji_id=88,
                version=3,
            )
        )
        scene_data.up_recorder(1, 2, 1000000, recorder(1, 2, 3))

    def credential(self, term=2):
        acquired = SimpleNamespace(
            channel_id="public",
            host_id="host-b",
            app_term=term,
            config_generation=1,
            lease_id=7,
            key_version=3,
            key="channels/public/host",
            noop_committed=False,
            key_revision=3,
        )
        ready = SimpleNamespace(**{**vars(acquired), "noop_committed": True})
        return acquired, ready

    @contextmanager
    def configured_authority(self, authority, state_path=None):
        with patch.object(Config, "GAME_SERVER_PORT", 0), patch.object(
            Config, "LOBBY_ETCD_ENDPOINT", "http://127.0.0.1:2379"
        ), patch.object(Config, "LOBBY_STATE_PATH", state_path), patch.object(
            Config, "LOBBY_HOST_ID", "host-b"
        ), patch.object(Config, "LOBBY_CHANNEL_ID", "public"), patch.object(
            Config, "LOBBY_CONFIG_GENERATION", 1
        ), patch.object(Config, "LOBBY_LEASE_SECONDS", 10.0), patch.object(
            Config, "LOBBY_SNAPSHOT_INTERVAL_SEC", 100.0
        ), patch.object(Config, "LINK_OTHER_SERVER", True), patch.object(
            etcd_authority, "EtcdClient"
        ), patch.object(
            etcd_authority, "ChannelAuthority", return_value=authority
        ), patch.object(
            PacketFactory, "initialize_handlers"
        ), patch.object(
            PacketFactory, "shutdown"
        ), patch.object(
            notice_sync, "init", return_value=None
        ), patch.object(
            notice_sync, "stop"
        ):
            yield

    @contextmanager
    def running_server(self, authority, state_path=None):
        with self.configured_authority(authority, state_path):
            server = GameServer()
            server.ACCEPT_TIMEOUT = 0.02
            thread = threading.Thread(target=server.start, daemon=True)
            thread.start()
            try:
                self.assertTrue(server.wait_started(2.0))
                yield server
            finally:
                if not server.stopped:
                    server.stop()
                thread.join(timeout=2.0)
                self.assertFalse(thread.is_alive())

    def test_new_leader_restores_etcd_snapshot_before_local_fallback(self):
        self.populate()
        state = lobby_state.capture_lobby_state("host-a", 1, "public")
        baseline = state["roster_seq"]
        shared_projection.store.clear()
        scene_data.clear_scene_snapshots()
        acquired, ready = self.credential()
        authority = Mock()
        authority.acquire.return_value = acquired
        authority.commit_noop.return_value = ready
        authority.load_snapshot.return_value = json.dumps(state).encode()

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "lobby.json")
            with patch.object(
                lobby_state, "load_lobby_state", return_value=state
            ) as load_local:
                with self.running_server(authority, path) as server:
                    self.assertEqual(server.epoch, 2)
                    self.assertEqual(server.lease.epoch, 2)
                    self.assertIsNotNone(shared_projection.store.get(1000000))
                    self.assertEqual(scene_data._session_list, [])
                    metadata = shared_projection.store.roster_metadata()
                    self.assertEqual(metadata["authority_epoch"], 2)
                    self.assertEqual(metadata["roster_seq"], baseline)
                    self.assertTrue(server.roster_replicator._paused)
                    self.assertIs(
                        replicated_roster.active(), server.roster_replicator
                    )
                    self.assertIs(replicated_events.active_log(), server.event_log)
                    authority.commit_roster.assert_not_called()
                    authority.commit_snapshot.assert_not_called()
                    self.assertEqual(
                        [call[0] for call in authority.method_calls[:3]],
                        ["acquire", "commit_noop", "load_snapshot"],
                    )
                    load_local.assert_not_called()
                load_local.assert_called_once()

    def test_local_snapshot_is_used_only_when_etcd_snapshot_is_absent(self):
        self.populate()
        state = lobby_state.capture_lobby_state("host-a", 1, "public")
        shared_projection.store.clear()
        scene_data.clear_scene_snapshots()
        acquired, ready = self.credential()
        authority = Mock()
        authority.acquire.return_value = acquired
        authority.commit_noop.return_value = ready
        authority.load_snapshot.return_value = None

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "lobby.json")
            with patch.object(
                lobby_state, "load_lobby_state", return_value=state
            ) as load_local:
                with self.running_server(authority, path):
                    self.assertIsNotNone(shared_projection.store.get(1000000))
                    self.assertEqual(
                        shared_projection.store.roster_metadata()["authority_epoch"],
                        2,
                    )
                    load_local.assert_called_once_with(path)
                self.assertEqual(load_local.call_count, 2)

    def test_recovery_rejects_version_channel_and_authority_rollback(self):
        self.populate()
        state = lobby_state.capture_lobby_state("host-a", 1, "public")
        _, credential = self.credential()
        before = shared_projection.store.snapshot()
        cases = (
            ("version", lobby_state.STATE_VERSION + 1),
            ("channel_id", "other"),
            ("config_generation", 2),
            ("epoch", credential.app_term + 1),
            ("authority_epoch", credential.app_term + 1),
        )
        for field, value in cases:
            with self.subTest(field=field):
                candidate = copy.deepcopy(state)
                candidate[field] = value
                with self.assertRaises(lobby_state.LobbyStateError):
                    GameServer._restore_recovery_state(candidate, credential)
                self.assertEqual(shared_projection.store.snapshot(), before)

    def test_server_without_leadership_does_not_load_or_overwrite_snapshot(self):
        self.populate()
        local_state = lobby_state.capture_lobby_state("host-a", 1, "public")
        shared_projection.store.clear()
        scene_data.clear_scene_snapshots()
        authority = Mock()
        authority.acquire.side_effect = etcd_authority.NotLeaderError("live host")

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "lobby.json")
            with patch.object(
                lobby_state, "load_lobby_state", return_value=local_state
            ) as load_local:
                with self.configured_authority(authority, path):
                    server = GameServer()
                    thread = threading.Thread(target=server.start, daemon=True)
                    thread.start()
                    with self.assertRaises(RuntimeError):
                        server.wait_started(2.0)
                    thread.join(timeout=2.0)
                    self.assertFalse(thread.is_alive())
                    self.assertTrue(server.stopped)
                load_local.assert_not_called()
        authority.commit_noop.assert_not_called()
        authority.load_snapshot.assert_not_called()
        authority.commit_snapshot.assert_not_called()
        self.assertEqual(shared_projection.store.snapshot(), {})


class LobbyHostLeaseTests(unittest.TestCase):
    def test_renew_extends_validity(self):
        now = [100.0]
        lease = lobby_state.LobbyHostLease(
            "host-a", 1, 10.0, clock=lambda: now[0]
        )
        now[0] = 109.0
        lease.renew()
        self.assertTrue(lease.is_valid())
        now[0] = 118.0
        self.assertTrue(lease.is_valid())

    def test_expired_lease_can_be_acquired_with_new_epoch(self):
        now = [100.0]
        lease = lobby_state.LobbyHostLease(
            "host-a", 1, 10.0, clock=lambda: now[0]
        )
        now[0] = 111.0
        self.assertFalse(lease.is_valid())
        self.assertTrue(lease.acquire_if_expired("host-b"))
        self.assertEqual(lease.host_id, "host-b")
        self.assertEqual(lease.epoch, 2)
        self.assertTrue(lease.is_valid())

    def test_stale_epoch_cannot_renew(self):
        lease = lobby_state.LobbyHostLease("host-a", 2, 10.0)
        with self.assertRaises(lobby_state.StaleEpochError):
            lease.renew(epoch=1)

    def test_expired_lease_cannot_be_renewed(self):
        now = [100.0]
        lease = lobby_state.LobbyHostLease(
            "host-a", 1, 1.0, clock=lambda: now[0]
        )
        now[0] = 102.0
        with self.assertRaises(lobby_state.StaleEpochError):
            lease.renew()
        self.assertFalse(lease.is_valid())

    def test_expired_lease_fails_assertion(self):
        now = [100.0]
        lease = lobby_state.LobbyHostLease(
            "host-a", 1, 1.0, clock=lambda: now[0]
        )
        now[0] = 102.0
        with self.assertRaises(lobby_state.StaleEpochError):
            lease.assert_valid()


if __name__ == "__main__":
    unittest.main()
