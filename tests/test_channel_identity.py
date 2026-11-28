import base64
import copy
import time
import unittest

from server import channel_identity as ci


def _sign(payload, identity):
    envelope = copy.deepcopy(payload)
    envelope["signature"] = base64.b64encode(
        identity.sign(ci.canonical_json(payload))).decode("ascii")
    return envelope


def _invite(channel, inviter, invitee, previous=None, expires_at=None, nonce=None):
    payload = {
        "type": "genesis_invite", "signer_backend": inviter.backend,
        "channel_id": channel.channel_id, "invite_id": nonce or "invite-1",
        "inviter_node_id": inviter.node_id,
        "inviter_public_key": inviter.public_key,
        "invitee_node_id": invitee.node_id, "invitee_public_key": invitee.public_key,
        "previous_invite_id": previous or channel._invites[inviter.node_id]["invite_id"],
        "nonce": nonce or secrets_hex(), "expires_at": expires_at or int(time.time()) + 60,
    }
    return _sign(payload, inviter)


def secrets_hex():
    import secrets
    return secrets.token_hex(8)


class CanonicalJsonTests(unittest.TestCase):
    def test_canonical_order_and_unicode(self):
        self.assertEqual(ci.canonical_json({"z": 1, "é": "雪"}),
                         ci.canonical_json({"é": "雪", "z": 1}))
        self.assertNotIn(b"\\u", ci.canonical_json({"é": "雪"}))

    def test_invalid_json_values_rejected(self):
        for value in (float("nan"), float("inf"), {1: "bad"}):
            with self.subTest(value=value), self.assertRaises(ci.IdentityError):
                ci.canonical_json(value)
        with self.assertRaises(ci.IdentityError):
            ci.parse_canonical_json(b'{"a":1,"a":2}')
        with self.assertRaises(ci.IdentityError):
            ci.parse_canonical_json(b'{"a":NaN}')


class HmacChannelTests(unittest.TestCase):
    def setUp(self):
        self.secret = b"x" * 32
        self.genesis = ci.identity_from_secret(self.secret, "genesis")
        self.member = ci.identity_from_secret(self.secret, "member")
        self.other = ci.identity_from_secret(self.secret, "other")
        self.channel = ci.ChannelIdentity.create(self.genesis, "public-lobby")
        self.channel.join(_invite(self.channel, self.genesis, self.member))

    def message(self, identity="default", **overrides):
        identity = self.member if identity == "default" else identity
        values = {"message_id": "m1", "nonce": "n1", "body": {"text": "hello"}}
        values.update(overrides)
        return self.channel.sign_message(identity=identity, **values)

    def test_stable_node_ids_and_genesis(self):
        same = ci.identity_from_secret(self.secret, "genesis")
        self.assertEqual(same.node_id, self.genesis.node_id)
        self.assertNotEqual(same.node_id, self.member.node_id)
        self.assertNotEqual(ci.identity_from_secret(self.secret, "changed").node_id,
                            self.genesis.node_id)
        invite = self.channel.genesis_invite()
        self.assertIsNone(invite["previous_invite_id"])
        self.assertEqual(invite["inviter_node_id"], invite["invitee_node_id"])

    def test_hmac_fallback_is_explicitly_shared_secret(self):
        self.assertEqual(
            ci.BACKEND, "ed25519" if ci.Ed25519PrivateKey is not None else None
        )
        if ci.BACKEND == "hmac-sha256":
            # The same secret plus public label is enough to impersonate a peer.
            verifier = ci.identity_from_secret(self.secret, "member")
            self.assertTrue(verifier.verify(b"message", self.member.sign(b"message")))
            self.assertFalse(ci.identity_from_secret(b"z" * 32, "member").verify(
                b"message", self.member.sign(b"message")))

    def test_valid_member_message_and_replay_by_each_key(self):
        envelope = self.message()
        self.assertEqual(self.channel.verify_message(envelope), self.member.node_id)
        with self.assertRaises(ci.ReplayError):
            self.channel.verify_message(envelope)
        with self.assertRaises(ci.ReplayError):
            self.channel.verify_message(self.message(message_id="m2", nonce="n1"))
        with self.assertRaises(ci.ReplayError):
            self.channel.verify_message(self.message(message_id="m1", nonce="n2"))

    def test_forged_signature_and_tampered_body_rejected(self):
        forged = self.message()
        raw = bytearray(base64.b64decode(forged["signature"]))
        raw[0] ^= 1
        forged["signature"] = base64.b64encode(raw).decode("ascii")
        with self.assertRaises(ci.IdentityError):
            self.channel.verify_message(forged)
        tampered = self.message()
        tampered["body"]["text"] = "forged"
        with self.assertRaises(ci.IdentityError):
            self.channel.verify_message(tampered)
        self.assertNotIn("m1", self.channel._message_ids)

    def test_unknown_sender_revoked_sender_and_channel_binding(self):
        stranger = self.channel.sign_message(self.other, "x", "x", sent_at=1) \
            if self.other.node_id in self.channel.members() else None
        self.assertIsNone(stranger)
        for field, value in (("sender_node_id", self.other.node_id),
                             ("channel_id", "another-channel")):
            envelope = self.message()
            envelope[field] = value
            with self.subTest(field=field), self.assertRaises(ci.IdentityError):
                self.channel.verify_message(envelope)
        self.channel.revoke(self.member.node_id)
        self.assertFalse(self.channel.is_member(self.member.node_id))
        with self.assertRaises(ci.RevokedMemberError):
            self.channel.verify_message(self.message())

    def test_non_genesis_cannot_revoke_or_revoke_genesis(self):
        with self.assertRaises(ci.IdentityError):
            self.channel.revoke(self.member.node_id, actor=self.member.node_id)
        with self.assertRaises(ci.IdentityError):
            self.channel.revoke(self.genesis.node_id)

    def test_invite_forgery_wrong_channel_replay_and_expiry(self):
        invite = _invite(self.channel, self.genesis, self.other, nonce="new-invite")
        bad = copy.deepcopy(invite)
        bad["inviter_public_key"] = self.other.public_key
        with self.assertRaises(ci.IdentityError):
            self.channel.join(bad)
        wrong_channel = copy.deepcopy(invite)
        wrong_channel["channel_id"] = "wrong"
        with self.assertRaises(ci.IdentityError):
            self.channel.join(wrong_channel)
        with self.assertRaises(ci.IdentityError):
            self.channel.join(_invite(self.channel, self.genesis, self.other,
                                      expires_at=int(time.time()) - 1, nonce="expired"))
        self.channel.join(invite)
        with self.assertRaises(ci.IdentityError):
            self.channel.join(invite)

    def test_revoked_member_cannot_invite(self):
        self.channel.revoke(self.member.node_id)
        with self.assertRaises(ci.RevokedMemberError):
            self.channel.join(_invite(self.channel, self.member, self.other,
                                      nonce="after-revoke"))

    def test_state_restore_preserves_membership_and_replay(self):
        envelope = self.message()
        self.channel.verify_message(envelope)
        snapshot = self.channel.export_state()
        restored = ci.ChannelIdentity.restore(snapshot, self.genesis.node_id, self.genesis)
        self.assertEqual(restored.members(), self.channel.members())
        with self.assertRaises(ci.ReplayError):
            restored.verify_message(envelope)
        self.assertEqual(restored.verify_message(self.message(message_id="m2", nonce="n2")),
                         self.member.node_id)
        tampered = copy.deepcopy(snapshot)
        tampered["revoked"] = [self.member.node_id]
        with self.assertRaises(ci.IdentityError):
            ci.ChannelIdentity.restore(tampered, self.genesis.node_id, self.genesis)
        with self.assertRaises(ci.IdentityError):
            ci.ChannelIdentity.restore(snapshot, self.member.node_id, self.genesis)

    def test_restore_fails_closed_without_hmac_secret(self):
        snapshot = self.channel.export_state()
        if ci.BACKEND == "hmac-sha256":
            with self.assertRaises(ci.IdentityError):
                ci.ChannelIdentity.restore(snapshot, self.genesis.node_id)


@unittest.skipIf(ci.Ed25519PrivateKey is None, "cryptography is not installed")
class Ed25519ChannelTests(unittest.TestCase):
    def test_two_independent_keys_can_participate(self):
        genesis = ci.Ed25519Identity.generate()
        member = ci.Ed25519Identity.generate()
        channel = ci.ChannelIdentity.create(genesis, "ed-channel")
        channel.join(_invite(channel, genesis, member))
        message = channel.sign_message(
            {"text": "hello"}, "m", "n", sent_at=1, identity=member
        )
        self.assertEqual(channel.verify_message(message), member.node_id)
        # A restored channel verifies without any private key.
        restored = ci.ChannelIdentity.restore(channel.export_state(), genesis.node_id)
        with self.assertRaises(ci.ReplayError):
            restored.verify_message(message)


if __name__ == "__main__":
    unittest.main()
