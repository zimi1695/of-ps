import base64
import json
import unittest

from server.etcd_authority import (
    AuthorityError,
    ChannelAuthority,
    EtcdClient,
    NotLeaderError,
)
from server.replicated_events import ReplicatedEventLog
from utils.metrics import metrics


class FakeEtcd:
    def __init__(self):
        self.values = {}
        self.leases = {}
        self.next_lease = 1
        self.next_revision = 1
        self.next_version = 1

    def range(self, key):
        value = self.values.get(key)
        return dict(value) if value else None

    def grant_lease(self, seconds):
        lease_id = self.next_lease
        self.next_lease += 1
        self.leases[lease_id] = int(seconds)
        return lease_id, int(seconds)

    def keepalive(self, lease_id):
        if self.leases.get(lease_id, 0) <= 0:
            raise NotLeaderError("expired")
        return self.leases[lease_id]

    def time_to_live(self, lease_id):
        return self.leases.get(lease_id, 0)

    def revoke(self, lease_id):
        self.leases[lease_id] = 0
        for key in list(self.values):
            if self.values[key]["lease"] == lease_id:
                del self.values[key]

    def compare_and_put(
        self, key, expected_version, value, lease_id=0, authority_compare=None
    ):
        current = self.values.get(key)
        if (current["version"] if current else 0) != expected_version:
            raise NotLeaderError("version changed")
        if authority_compare:
            authority_key, authority_version = authority_compare
            if self.values[authority_key]["mod_revision"] != authority_version:
                raise NotLeaderError("authority changed")
        if lease_id and self.leases.get(lease_id, 0) <= 0:
            raise NotLeaderError("lease expired")
        version = current["version"] + 1 if current else 1
        revision = self.next_revision
        self.next_revision += 1
        self.values[key] = {
            "value": value,
            "version": version,
            "lease": lease_id,
            "mod_revision": revision,
        }
        return {"succeeded": True}


class FakeResponse:
    def __init__(self, payload):
        self.payload = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return self.payload


class FakeOpener:
    def __init__(self, payload):
        self.payload = payload
        self.requests = []

    def open(self, request, timeout):
        self.requests.append((request, timeout))
        return FakeResponse(self.payload)


class EtcdClientTests(unittest.TestCase):
    def test_credentials_require_tls_outside_loopback(self):
        with self.assertRaises(AuthorityError):
            EtcdClient("http://etcd.example.test:2379", username="user", password="secret")

    def test_transaction_uses_base64_and_authority_comparison(self):
        opener = FakeOpener({"succeeded": True})
        client = EtcdClient("http://127.0.0.1:2379", opener=opener)
        client.compare_and_put(
            "state",
            2,
            b"value",
            7,
            authority_compare=("host", 3),
        )
        request, timeout = opener.requests[0]
        payload = json.loads(request.data)
        self.assertEqual(request.full_url, "http://127.0.0.1:2379/v3/kv/txn")
        self.assertEqual(timeout, 3.0)
        self.assertEqual(len(payload["compare"]), 2)
        self.assertEqual(
            base64.b64decode(payload["compare"][0]["key"]), b"state"
        )
        self.assertEqual(payload["compare"][1]["target"], "MOD")
        self.assertEqual(payload["compare"][1]["modRevision"], "3")
        self.assertEqual(
            payload["success"][0]["request_put"]["value"],
            base64.b64encode(b"value").decode(),
        )


    def test_absent_key_uses_create_revision_comparison(self):
        opener = FakeOpener({"succeeded": True})
        client = EtcdClient("http://127.0.0.1:2379", opener=opener)
        client.compare_and_put("new-key", 0, b"value")
        request, _ = opener.requests[0]
        payload = json.loads(request.data)
        self.assertEqual(
            payload["compare"],
            [{
                "target": "CREATE",
                "key": base64.b64encode(b"new-key").decode(),
                "createRevision": "0",
            }],
        )

    def test_compare_and_delete_uses_version_and_authority_comparison(self):
        opener = FakeOpener({"succeeded": True})
        client = EtcdClient("http://127.0.0.1:2379", opener=opener)
        client.compare_and_delete(
            "entity",
            4,
            authority_compare=("host", 3),
        )
        request, timeout = opener.requests[0]
        payload = json.loads(request.data)
        self.assertEqual(request.full_url, "http://127.0.0.1:2379/v3/kv/txn")
        self.assertEqual(timeout, 3.0)
        self.assertEqual(len(payload["compare"]), 2)
        self.assertEqual(
            base64.b64decode(payload["compare"][0]["key"]), b"entity"
        )
        self.assertEqual(payload["compare"][0]["version"], "4")
        self.assertEqual(payload["compare"][1]["target"], "MOD")
        self.assertEqual(payload["compare"][1]["modRevision"], "3")
        self.assertEqual(
            base64.b64decode(payload["success"][0]["request_delete_range"]["key"]),
            b"entity",
        )


class ChannelAuthorityTests(unittest.TestCase):
    def setUp(self):
        self.client = FakeEtcd()
        self.authority = ChannelAuthority(self.client, lease_seconds=10)

    def test_acquire_commits_noop_and_allows_authorized_state(self):
        credential = self.authority.acquire("lobby", "host-a", 1)
        self.assertFalse(credential.noop_committed)
        with self.assertRaises(NotLeaderError):
            self.authority.commit_state(credential, "channels/lobby/roster", b"{}")
        ready = self.authority.commit_noop(credential)
        self.assertTrue(ready.noop_committed)
        self.authority.commit_state(ready, "channels/lobby/roster", b"{}")
        self.assertEqual(
            self.authority.verify("lobby", "host-a", 1, 1), ("host-a", 1, 1)
        )

    def test_live_host_cannot_be_stolen(self):
        self.authority.acquire("lobby", "host-a", 1)
        with self.assertRaises(NotLeaderError):
            self.authority.acquire("lobby", "host-b", 1)

    def test_expired_host_is_replaced_with_higher_application_term(self):
        first = self.authority.acquire("lobby", "host-a", 1)
        self.client.leases[first.lease_id] = 0
        second = self.authority.acquire("lobby", "host-b", 1)
        self.assertEqual(second.app_term, 2)
        self.assertNotEqual(first.lease_id, second.lease_id)
        self.assertEqual(
            self.authority.verify("lobby", "host-b", 2, 1),
            ("host-b", 2, 1),
        )

    def test_revoked_host_key_still_increments_persistent_term(self):
        first = self.authority.acquire("lobby", "host-a", 1)
        term_record = self.client.values["channels/lobby/app-term"]
        self.assertEqual(term_record["value"], b"1")
        self.assertEqual(term_record["lease"], 0)
        self.client.revoke(first.lease_id)
        self.assertIsNone(self.client.range(first.key))
        second = self.authority.acquire("lobby", "host-b", 1)
        self.assertEqual(second.app_term, 2)
        self.assertEqual(self.client.values["channels/lobby/app-term"]["value"], b"2")

    def test_same_host_refresh_keeps_persistent_term(self):
        first = self.authority.acquire("lobby", "host-a", 1)
        refreshed = self.authority.acquire("lobby", "host-a", 1)
        self.assertEqual(refreshed.app_term, first.app_term)
        self.assertNotEqual(refreshed.lease_id, first.lease_id)
        self.assertGreater(refreshed.key_version, first.key_version)
        self.assertEqual(self.client.values["channels/lobby/app-term"]["value"], b"1")

    def test_stale_credential_cannot_overwrite_new_authority(self):
        first = self.authority.acquire("lobby", "host-a", 1)
        first = self.authority.commit_noop(first)
        self.client.leases[first.lease_id] = 0
        second = self.authority.acquire("lobby", "host-b", 1)
        second = self.authority.commit_noop(second)
        with self.assertRaises(NotLeaderError):
            self.authority.commit_state(first, "channels/lobby/roster", b"old")
        self.authority.commit_state(second, "channels/lobby/roster", b"new")

    def test_recreated_host_key_cannot_reuse_old_fencing_revision(self):
        first = self.authority.commit_noop(
            self.authority.acquire("lobby", "host-a", 1)
        )
        self.client.revoke(first.lease_id)
        second = self.authority.commit_noop(
            self.authority.acquire("lobby", "host-b", 1)
        )
        self.assertEqual(first.key_version, second.key_version)
        self.assertLess(first.key_revision, second.key_revision)
        with self.assertRaises(NotLeaderError):
            self.authority.commit_state(first, "channels/lobby/stale", b"stale")

    def test_config_generation_change_rejects_old_credential(self):
        credential = self.authority.acquire("lobby", "host-a", 1)
        with self.assertRaises(NotLeaderError):
            self.authority.acquire("lobby", "host-a", 2)
        self.assertEqual(
            self.authority.verify("lobby", config_generation=1),
            ("host-a", 1, 1),
        )

    def test_renew_rejects_stale_identity(self):
        credential = self.authority.acquire("lobby", "host-a", 1)
        self.client.leases[credential.lease_id] = 0
        with self.assertRaises(NotLeaderError):
            self.authority.renew(credential)

    def test_roster_and_snapshot_survive_authority_lease_expiry(self):
        credential = self.authority.commit_noop(
            self.authority.acquire("lobby", "host-a", 1)
        )
        self.authority.commit_roster(credential, b"roster")
        self.authority.commit_snapshot(credential, b"snapshot")

        roster_key = "channels/lobby/roster"
        snapshot_key = "channels/lobby/snapshot"
        self.assertEqual(self.client.values[roster_key]["lease"], 0)
        self.assertEqual(self.client.values[snapshot_key]["lease"], 0)
        self.assertEqual(self.authority.load_roster("lobby"), b"roster")
        self.assertEqual(self.authority.load_snapshot("lobby"), b"snapshot")

        self.client.revoke(credential.lease_id)
        self.assertEqual(self.authority.load_roster("lobby"), b"roster")
        self.assertEqual(self.authority.load_snapshot("lobby"), b"snapshot")

    def test_persistent_state_still_requires_current_authority(self):
        first = self.authority.commit_noop(
            self.authority.acquire("lobby", "host-a", 1)
        )
        self.authority.commit_roster(first, b"old-roster")
        self.authority.commit_snapshot(first, b"old-snapshot")
        self.client.leases[first.lease_id] = 0
        second = self.authority.commit_noop(
            self.authority.acquire("lobby", "host-b", 1)
        )

        with self.assertRaises(NotLeaderError):
            self.authority.commit_roster(first, b"stale-roster")
        with self.assertRaises(NotLeaderError):
            self.authority.commit_snapshot(first, b"stale-snapshot")
        self.assertEqual(self.authority.load_roster("lobby"), b"old-roster")
        self.assertEqual(self.authority.load_snapshot("lobby"), b"old-snapshot")

        self.authority.commit_roster(second, b"new-roster")
        self.authority.commit_snapshot(second, b"new-snapshot")
        self.assertEqual(self.authority.load_roster("lobby"), b"new-roster")
        self.assertEqual(self.authority.load_snapshot("lobby"), b"new-snapshot")

    def test_event_log_keys_survive_authority_lease_expiry(self):
        credential = self.authority.commit_noop(
            self.authority.acquire("lobby", "host-a", 1)
        )
        log = ReplicatedEventLog(self.authority, credential)
        log.append(7, 8, 2, "chat", 9, b"payload", b"response")
        event_key = log._key(7, 8, 2, "chat", 9)
        self.assertEqual(self.client.values[event_key]["lease"], 0)
        self.client.revoke(credential.lease_id)
        self.assertEqual(log.recover_response(7, 8, 2, "chat", 9), b"response")

    def test_event_log_uses_renewed_credential(self):
        holder = {
            "credential": self.authority.commit_noop(
                self.authority.acquire("lobby", "host-a", 1)
            )
        }
        log = ReplicatedEventLog(self.authority, lambda: holder["credential"])
        log.append(7, 8, 2, "chat", 1, b"first", b"response-1")
        holder["credential"] = self.authority.renew(holder["credential"])
        log.append(7, 8, 2, "chat", 2, b"second", b"response-2")
        self.assertEqual(
            log.recover_response(7, 8, 2, "chat", 2),
            b"response-2",
        )

    def test_replicated_event_replay_returns_original_response(self):
        metrics.reset()
        credential = self.authority.commit_noop(
            self.authority.acquire("lobby", "host-a", 1)
        )
        log = ReplicatedEventLog(self.authority, credential)
        args = (7, 8, 2, "chat", 9)
        first = log.append(*args, b"payload", b"response")
        second = log.append(*args, b"different", b"different")
        self.assertEqual(first, b"response")
        self.assertEqual(second, b"response")
        self.assertEqual(log.recover_response(*args), b"response")
        self.assertEqual(
            metrics.snapshot()["counters"]["ofps.events.replayed_total"], 1
        )


if __name__ == "__main__":
    unittest.main()
