import unittest
import json

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from proto.net_pb2 import EntityOwnershipTransfer
from server import entity_authority, shared_projection
from server.entity_store import EntityOwnershipStore
from server.etcd_authority import AuthorityCredential, NotLeaderError


class FakeEtcd:
    def __init__(self):
        self.values = {}
        self.next_version = 1

    def range(self, key):
        value = self.values.get(key)
        return dict(value) if value else None

    def compare_and_delete(self, key, expected_version, authority_compare=None):
        current = self.values.get(key)
        if (current["version"] if current else 0) != expected_version:
            raise NotLeaderError("version changed")
        if authority_compare:
            authority_key, authority_version = authority_compare
            authority = self.values.get(authority_key)
            if authority is None or authority["mod_revision"] != authority_version:
                raise NotLeaderError("authority changed")
        self.values.pop(key, None)
        return {"succeeded": True}

    def time_to_live(self, lease_id):
        return 10 if lease_id in {1, 2, 3} else 0

    def compare_and_put(
        self, key, expected_version, value, lease_id=0, authority_compare=None
    ):
        current = self.values.get(key)
        if (current["version"] if current else 0) != expected_version:
            raise NotLeaderError("version changed")
        if authority_compare:
            authority_key, authority_version = authority_compare
            authority = self.values.get(authority_key)
            if authority is None or authority["mod_revision"] != authority_version:
                raise NotLeaderError("authority changed")
        version = current["version"] + 1 if current else self.next_version
        if current is None:
            self.next_version += 1
        self.values[key] = {
            "value": value,
            "version": version,
            "lease": lease_id,
            "mod_revision": version,
        }
        return {"succeeded": True}


def public_bytes(private_key):
    return private_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)


class EntityStoreTests(unittest.TestCase):
    def setUp(self):
        shared_projection.store.clear()
        self.client = FakeEtcd()
        self.store = EntityOwnershipStore(self.client, "lobby")
        self.key_a = Ed25519PrivateKey.generate()
        self.key_b = Ed25519PrivateKey.generate()
        self.credential_a = AuthorityCredential(
            "lobby", "host-a", 1, 1, 1, 1, "channels/lobby/host", True,
            key_revision=1,
        )
        self.credential_b = AuthorityCredential(
            "lobby", "host-b", 2, 1, 2, 2, "channels/lobby/host", True,
            key_revision=2,
        )
        self.client.values[self.credential_a.key] = {
            "value": json.dumps({
                "host_id": "host-a", "app_term": 1, "config_generation": 1
            }).encode(),
            "version": 1, "lease": 1, "mod_revision": 1
        }
        self.authority_a = entity_authority.EntityAuthority("host-a", self.key_a)
        self.authority_a.configure_store(self.store, lambda: self.credential_a)
        self.authority_b = entity_authority.EntityAuthority("host-b", self.key_b)
        self.authority_b.configure_store(self.store, lambda: self.credential_b)

    def test_claim_is_persistent_and_visible_to_other_host(self):
        domain = entity_authority.entity_domain("gather", 1, 2)
        self.authority_a.assert_owner(domain, 3, require_shared=True)
        stored = self.store.load(domain, 3)
        self.assertEqual(stored["transfer"].new_owner_id, "host-a")
        self.assertEqual(stored["transfer"].new_owner_public_key, public_bytes(self.key_a))
        with self.assertRaises(entity_authority.OwnershipError):
            self.authority_b.assert_owner(domain, 3, require_shared=True)

    def test_signed_transfer_moves_ownership(self):
        domain = entity_authority.entity_domain("drop", 1, 2)
        self.authority_a.assert_owner(domain, 4, require_shared=True)
        self.authority_a.transfer(domain, 4, "host-b", public_bytes(self.key_b))
        self.authority_b.configure_store(self.store, lambda: None)
        moved = self.authority_b.assert_owner(domain, 4, require_shared=True)
        self.assertEqual((moved.owner_id, moved.owner_generation), ("host-b", 2))
        with self.assertRaises(entity_authority.OwnershipError):
            self.authority_a.assert_owner(domain, 4, require_shared=True)

    def test_new_leader_can_take_over_but_stale_leader_cannot(self):
        domain = entity_authority.entity_domain("monster", 1, 2) + ":5"
        self.authority_a.assert_owner(domain, 6, require_shared=True)
        self.client.values[self.credential_b.key] = {
            "value": json.dumps({
                "host_id": "host-b", "app_term": 2, "config_generation": 1
            }).encode(),
            "version": 2, "lease": 2, "mod_revision": 2
        }
        with self.assertRaises(entity_authority.OwnershipError):
            self.authority_a.assert_owner(domain, 6, require_shared=True)
        self.authority_b.assert_owner(domain, 6, require_shared=True)
        stored = self.store.load(domain, 6)
        self.assertEqual(stored["transfer"].new_owner_id, "host-b")
        self.assertEqual(stored["transfer"].authority_epoch, 2)
        self.client.values[self.credential_b.key]["version"] = 3
        with self.assertRaises(entity_authority.OwnershipError):
            self.authority_b.assert_owner(domain, 7, require_shared=True)

    def test_same_host_refreshes_ownership_in_new_authority_term(self):
        domain = entity_authority.entity_domain("gather", 1, 2)
        self.authority_a.assert_owner(domain, 14, require_shared=True)
        renewed = AuthorityCredential(
            "lobby", "host-a", 2, 1, 3, 2, "channels/lobby/host", True,
            key_revision=2,
        )
        self.client.values[renewed.key] = {
            "value": json.dumps({
                "host_id": "host-a", "app_term": 2, "config_generation": 1
            }).encode(),
            "version": 2, "lease": 3, "mod_revision": 2,
        }
        self.authority_a.configure_store(self.store, lambda: renewed)
        current = self.authority_a.assert_owner(domain, 14, require_shared=True)
        self.assertEqual((current.authority_epoch, current.owner_generation), (2, 2))

    def test_owner_can_release_temporary_entity_record(self):
        domain = entity_authority.entity_domain("drop", 1, 2, 1)
        self.authority_a.assert_owner(domain, 12, require_shared=True)
        self.assertTrue(self.authority_a.release(domain, 12))
        self.assertIsNone(self.store.load(domain, 12))
        self.assertFalse(self.authority_a.release(domain, 12))

    def test_non_owner_cannot_release_entity_record(self):
        domain = entity_authority.entity_domain("drop", 1, 2, 1)
        self.authority_a.assert_owner(domain, 13, require_shared=True)
        with self.assertRaises(entity_authority.OwnershipError):
            self.authority_b.release(domain, 13)
        self.assertIsNotNone(self.store.load(domain, 13))

    def test_stored_payload_must_match_requested_entity_key(self):
        domain = entity_authority.entity_domain("gather", 1, 2)
        self.authority_a.assert_owner(domain, 10, require_shared=True)
        wrong_key = self.store._key(domain, 11)
        self.client.values[wrong_key] = dict(self.client.values[self.store._key(domain, 10)])
        with self.assertRaises(entity_authority.OwnershipError):
            self.store.load(domain, 11)

    def test_corrupted_stored_signature_is_rejected(self):
        domain = entity_authority.entity_domain("gather", 1, 2)
        self.authority_a.assert_owner(domain, 9, require_shared=True)
        key = self.store._key(domain, 9)
        record = self.client.values[key]
        value = json.loads(record["value"])
        value["new_owner_id"] = "forged-host"
        record["value"] = json.dumps(value).encode()
        with self.assertRaises(entity_authority.OwnershipError):
            self.store.load(domain, 9)

    def test_forged_new_owner_key_is_rejected(self):
        domain = entity_authority.entity_domain("gather", 1, 2)
        self.authority_a.assert_owner(domain, 8, require_shared=True)
        current = self.store.load(domain, 8)
        transfer = current["transfer"]
        forged = EntityOwnershipTransfer()
        forged.CopyFrom(transfer)
        forged.new_owner_id = "host-b"
        forged.new_owner_generation = 2
        forged.transfer_seq = 2
        forged.new_owner_public_key = public_bytes(self.key_b)
        with self.assertRaises(entity_authority.OwnershipError):
            self.store.transfer(forged, current["version"])


if __name__ == "__main__":
    unittest.main()
