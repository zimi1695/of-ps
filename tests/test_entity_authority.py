import unittest

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from proto.net_pb2 import EntityOwnershipTransfer
from server.entity_authority import (
    EntityAuthority,
    OwnershipError,
    OwnershipRegistry,
    StaleOwnershipError,
    activate,
    active,
    deactivate,
    entity_domain,
    sign_transfer,
)
from server import shared_projection


class EntityAuthorityTests(unittest.TestCase):
    def setUp(self):
        self.private = Ed25519PrivateKey.generate()
        self.public = self.private.public_key()
        self.registry = OwnershipRegistry()

    def transfer(self, **overrides):
        values = {
            "domain": "scene:1:channel:2:item:3",
            "authority_epoch": 5,
            "entity_id": 6,
            "prior_owner_generation": 1,
            "prior_owner_id": "host-a",
            "new_owner_generation": 2,
            "new_owner_id": "host-b",
            "transfer_seq": 1,
            "issued_at_us": 100,
        }
        values.update(overrides)
        return sign_transfer(EntityOwnershipTransfer(**values), self.private)

    def test_same_authority_epoch_allows_multiple_newer_transfers(self):
        first = self.registry.apply(self.transfer(), self.public)
        second = self.registry.apply(
            self.transfer(
                prior_owner_id="host-b",
                prior_owner_generation=2,
                new_owner_generation=3,
                new_owner_id="host-c",
                transfer_seq=2,
                issued_at_us=1,
            ),
            self.public,
        )
        self.assertEqual(first.order_key(), (5, 2, 1))
        self.assertEqual(second.order_key(), (5, 3, 2))
        self.assertEqual(second.owner_id, "host-c")

    def test_stale_authority_epoch_is_rejected(self):
        self.registry.apply(self.transfer(), self.public)
        with self.assertRaises(StaleOwnershipError):
            self.registry.apply(
                self.transfer(
                    authority_epoch=4,
                    prior_owner_id="host-b",
                    prior_owner_generation=2,
                    new_owner_generation=3,
                    transfer_seq=2,
                ),
                self.public,
            )

    def test_wrong_prior_owner_is_rejected(self):
        self.registry.apply(self.transfer(), self.public)
        with self.assertRaises(OwnershipError):
            self.registry.apply(
                self.transfer(
                    prior_owner_id="host-x",
                    prior_owner_generation=2,
                    new_owner_generation=3,
                    transfer_seq=2,
                ),
                self.public,
            )

    def test_duplicate_or_older_transfer_is_rejected(self):
        transfer = self.transfer()
        self.registry.apply(transfer, self.public)
        with self.assertRaises(StaleOwnershipError):
            self.registry.apply(transfer, self.public)

    def test_bad_signature_is_rejected(self):
        transfer = self.transfer()
        transfer.signature = b"forged"
        with self.assertRaises(OwnershipError):
            self.registry.apply(transfer, self.public)

    def test_owner_generation_must_increase(self):
        with self.assertRaises(OwnershipError):
            self.registry.apply(
                self.transfer(new_owner_generation=1), self.public
            )

    def test_entity_id_and_domain_are_validated(self):
        with self.assertRaises(OwnershipError):
            self.registry.apply(self.transfer(entity_id=0), self.public)
        with self.assertRaises(OwnershipError):
            self.registry.apply(self.transfer(domain="../../host"), self.public)

    def test_authority_claims_and_transfers_real_entity_domain(self):
        shared_projection.store.clear()
        authority = EntityAuthority("host-a", self.private)
        domain = entity_domain("gather", 1, 2)
        first = authority.assert_owner(domain, 3)
        self.assertEqual((first.owner_id, first.owner_generation), ("host-a", 1))
        transfer = authority.transfer(domain, 3, "host-b")
        peer = EntityAuthority("host-b", Ed25519PrivateKey.generate())
        moved = peer.receive(transfer, self.public)
        self.assertEqual(moved.owner_id, "host-b")
        with self.assertRaises(OwnershipError):
            authority.assert_owner(domain, 3)
        peer.assert_owner(domain, 3)

    def test_authority_snapshot_rejects_newer_epoch(self):
        authority = EntityAuthority("host-a", self.private)
        authority.assert_owner(entity_domain("drop", 1, 2), 3)
        snapshot = authority.snapshot()
        authority.restore(snapshot)
        self.assertEqual(authority.snapshot(), snapshot)
        snapshot[0]["authority_epoch"] = shared_projection.store.roster_metadata()[
            "authority_epoch"
        ] + 1
        with self.assertRaises(StaleOwnershipError):
            authority.restore(snapshot)

    def test_active_authority_lifecycle(self):
        authority = activate("host-a", self.private)
        self.assertIs(active(), authority)
        deactivate(authority)
        self.assertIsNone(active())


if __name__ == "__main__":
    unittest.main()
