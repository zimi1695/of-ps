import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey

from config import Config
from network import remote_link
from network.secure_channel import HandshakeError
from proto.net_pb2 import LinkMessage, LinkNegotiate
from server import link_identity
from utils.metrics import metrics


class LinkIdentityTests(unittest.TestCase):
    def test_signature_binds_identity_channel_and_ephemeral_key(self):
        private = link_identity.generate_private_key()
        message = LinkNegotiate(
            type=LinkNegotiate.PROPOSE,
            conv=7,
            server_name="peer",
            is_public=True,
            channel_id="public",
            nonce=8,
            issued_at_us=time.time_ns() // 1000,
            ephemeral_public=b"x" * 32,
        )
        link_identity.sign_negotiation(message, private)
        node_id = message.node_id
        self.assertEqual(
            link_identity.verify_negotiation(
                message, [node_id], "public"
            ),
            node_id,
        )
        message.server_name = "forged"
        with self.assertRaises(link_identity.LinkIdentityError):
            link_identity.verify_negotiation(message, [node_id], "public")

    def test_link_message_signature_survives_relay_metadata_change(self):
        private = link_identity.generate_private_key()
        node_id = link_identity.node_id_for_public_key(
            link_identity.public_key_bytes(private)
        )
        message = LinkMessage(
            id="message-1",
            origin="peer",
            type=LinkMessage.DATA,
            payload=b"payload",
        )
        link_identity.sign_link_message(message, private)
        message.relay_to.append("next-hop")
        self.assertEqual(
            link_identity.verify_link_message(message, [node_id]), node_id
        )
        message.payload = b"forged"
        with self.assertRaises(link_identity.LinkIdentityError):
            link_identity.verify_link_message(message, [node_id])

    def test_untrusted_stale_and_wrong_channel_are_rejected(self):
        private = link_identity.generate_private_key()
        message = LinkNegotiate(
            type=LinkNegotiate.PROPOSE,
            conv=7,
            server_name="peer",
            channel_id="public",
            nonce=8,
            issued_at_us=time.time_ns() // 1000,
            ephemeral_public=b"x" * 32,
        )
        link_identity.sign_negotiation(message, private)
        with self.assertRaises(link_identity.LinkIdentityError):
            link_identity.verify_negotiation(message, ["other"], "public")
        with self.assertRaises(link_identity.LinkIdentityError):
            link_identity.verify_negotiation(
                message, [message.node_id], "other"
            )
        with self.assertRaises(link_identity.LinkIdentityError):
            link_identity.verify_negotiation(
                message,
                [message.node_id],
                "public",
                now_us=message.issued_at_us + 60_000_000,
            )

    def test_identity_file_is_encrypted_and_stable(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "identity.bin"
            first = link_identity.load_or_create_identity(path)
            second = link_identity.load_or_create_identity(path)
            self.assertEqual(
                link_identity.public_key_bytes(first),
                link_identity.public_key_bytes(second),
            )
            self.assertNotIn(
                link_identity.serialize_private_key(first), path.read_bytes()
            )


class LinkNegotiationTests(unittest.TestCase):
    def setUp(self):
        self.old_links = dict(remote_link._links)
        self.old_convs = dict(remote_link._conv_to_addr)
        self.old_nonces = dict(remote_link._seen_nonces)
        self.old_identity = remote_link._identity_private
        self.old_node_id = remote_link._node_id
        remote_link._links.clear()
        remote_link._conv_to_addr.clear()
        remote_link._seen_nonces.clear()
        remote_link._stopped.clear()
        metrics.reset()

    def tearDown(self):
        remote_link._links.clear()
        remote_link._links.update(self.old_links)
        remote_link._conv_to_addr.clear()
        remote_link._conv_to_addr.update(self.old_convs)
        remote_link._seen_nonces.clear()
        remote_link._seen_nonces.update(self.old_nonces)
        remote_link._identity_private = self.old_identity
        remote_link._node_id = self.old_node_id
        metrics.reset()

    def signed_propose(self):
        local = link_identity.generate_private_key()
        peer = link_identity.generate_private_key()
        remote_link._identity_private = local
        remote_link._node_id = link_identity.node_id_for_public_key(
            link_identity.public_key_bytes(local)
        )
        message = LinkNegotiate(
            type=LinkNegotiate.PROPOSE,
            conv=77,
            server_name="peer",
            is_public=True,
            channel_id="public",
            nonce=99,
            issued_at_us=time.time_ns() // 1000,
            ephemeral_public=peer.public_key().public_bytes_raw(),
        )
        link_identity.sign_negotiation(message, peer)
        self.peer_private = peer
        return message

    def test_two_sides_derive_matching_directional_keys(self):
        first = X25519PrivateKey.generate()
        second = X25519PrivateKey.generate()
        left = remote_link._Link(("left", 1))
        right = remote_link._Link(("right", 1))
        left.local_ephemeral = first
        left.remote_ephemeral = second.public_key().public_bytes_raw()
        left.remote_node_id = "b" * 64
        right.local_ephemeral = second
        right.remote_ephemeral = first.public_key().public_bytes_raw()
        right.remote_node_id = "a" * 64
        with patch.object(remote_link, "_node_id", "a" * 64), patch.object(
            Config, "LINK_CHANNEL_ID", "public"
        ):
            remote_link._derive_secure_channels(left, 77)
        with patch.object(remote_link, "_node_id", "b" * 64), patch.object(
            Config, "LINK_CHANNEL_ID", "public"
        ):
            remote_link._derive_secure_channels(right, 77)
        self.assertEqual(
            right.secure_receive.decrypt(left.secure_send.encrypt(b"hello")),
            b"hello",
        )
        self.assertEqual(
            left.secure_receive.decrypt(right.secure_send.encrypt(b"reply")),
            b"reply",
        )

    def test_accept_establishes_only_the_waiting_conv(self):
        propose = self.signed_propose()
        accept = LinkNegotiate(
            type=LinkNegotiate.ACCEPT,
            conv=propose.conv,
            server_name=propose.server_name,
            is_public=True,
            channel_id="public",
            nonce=100,
            issued_at_us=time.time_ns() // 1000,
            ephemeral_public=propose.ephemeral_public,
        )
        link_identity.sign_negotiation(accept, self.peer_private)
        addr = ("127.0.0.1", 1)
        link = remote_link._Link(addr)
        link.state = remote_link.CONNECTING
        link.conv = propose.conv
        link.local_ephemeral = X25519PrivateKey.generate()
        remote_link._links[addr] = link
        remote_link._conv_to_addr[propose.conv] = addr
        with patch.object(Config, "LINK_TRUSTED_NODE_IDS", (propose.node_id,)), \
             patch.object(Config, "LINK_CHANNEL_ID", "public"), \
             patch.object(remote_link, "_after_connected") as connected:
            remote_link._on_negotiate(accept.SerializeToString(), addr)
        connected.assert_called_once()
        self.assertEqual(link.state, remote_link.CONNECTED)
        self.assertIsNotNone(link.secure_send)
        self.assertIsNotNone(link.secure_receive)

    def test_late_smaller_propose_does_not_regress_connected_conv(self):
        first = self.signed_propose()
        addr = ("127.0.0.1", 1)
        late = LinkNegotiate(
            type=LinkNegotiate.PROPOSE,
            conv=66,
            server_name=first.server_name,
            is_public=True,
            channel_id="public",
            nonce=101,
            issued_at_us=time.time_ns() // 1000,
            ephemeral_public=first.ephemeral_public,
        )
        link_identity.sign_negotiation(late, self.peer_private)
        with patch.object(Config, "LINK_TRUSTED_NODE_IDS", (first.node_id,)), \
             patch.object(Config, "LINK_CHANNEL_ID", "public"), \
             patch.object(remote_link, "_send_neg") as send, \
             patch.object(remote_link, "_after_connected") as connected:
            remote_link._on_negotiate(first.SerializeToString(), addr)
            remote_link._on_negotiate(late.SerializeToString(), addr)
        connected.assert_called_once()
        link = remote_link._links[addr]
        self.assertEqual((link.state, link.conv), (remote_link.CONNECTED, 77))
        self.assertEqual(send.call_args.args[:2], (addr, LinkNegotiate.CONFLICT))
        self.assertEqual(send.call_args.args[2], 77)

    def test_larger_propose_upgrades_connected_conv(self):
        first = self.signed_propose()
        addr = ("127.0.0.1", 1)
        larger = LinkNegotiate(
            type=LinkNegotiate.PROPOSE,
            conv=88,
            server_name=first.server_name,
            is_public=True,
            channel_id="public",
            nonce=102,
            issued_at_us=time.time_ns() // 1000,
            ephemeral_public=first.ephemeral_public,
        )
        link_identity.sign_negotiation(larger, self.peer_private)
        with patch.object(Config, "LINK_TRUSTED_NODE_IDS", (first.node_id,)), \
             patch.object(Config, "LINK_CHANNEL_ID", "public"), \
             patch.object(remote_link, "_send_neg"), \
             patch.object(remote_link, "_after_connected") as connected:
            remote_link._on_negotiate(first.SerializeToString(), addr)
            remote_link._on_negotiate(larger.SerializeToString(), addr)
        self.assertEqual(connected.call_count, 2)
        link = remote_link._links[addr]
        self.assertEqual((link.state, link.conv), (remote_link.CONNECTED, 88))
        self.assertIsNotNone(link.secure_send)
        self.assertIsNotNone(link.secure_receive)

    def test_conflict_upgrades_connecting_peer_to_larger_conv(self):
        first = self.signed_propose()
        addr = ("127.0.0.1", 1)
        link = remote_link._Link(addr)
        link.state = remote_link.CONNECTING
        link.conv = 66
        link.local_ephemeral = X25519PrivateKey.generate()
        remote_link._links[addr] = link
        remote_link._conv_to_addr[66] = addr
        conflict = LinkNegotiate(
            type=LinkNegotiate.CONFLICT,
            conv=77,
            server_name=first.server_name,
            is_public=True,
            channel_id="public",
            nonce=103,
            issued_at_us=time.time_ns() // 1000,
            ephemeral_public=first.ephemeral_public,
        )
        link_identity.sign_negotiation(conflict, self.peer_private)
        with patch.object(Config, "LINK_TRUSTED_NODE_IDS", (first.node_id,)), \
             patch.object(Config, "LINK_CHANNEL_ID", "public"), \
             patch.object(remote_link, "_send_neg") as send:
            remote_link._on_negotiate(conflict.SerializeToString(), addr)
        self.assertEqual((link.state, link.conv), (remote_link.CONNECTING, 77))
        self.assertEqual(send.call_args.args[:2], (addr, LinkNegotiate.PROPOSE))
        self.assertEqual(send.call_args.args[2], 77)

    def test_unsigned_negotiation_is_rejected_without_link(self):
        message = LinkNegotiate(
            type=LinkNegotiate.PROPOSE,
            conv=77,
            server_name="peer",
        )
        with patch.object(Config, "LINK_TRUSTED_NODE_IDS", ("trusted",)):
            remote_link._on_negotiate(message.SerializeToString(), ("127.0.0.1", 1))
        self.assertEqual(remote_link._links, {})
        self.assertEqual(
            metrics.snapshot()["counters"]["ofps.link.auth_rejections_total"], 1
        )

    def test_valid_negotiation_establishes_authenticated_channel(self):
        message = self.signed_propose()
        with patch.object(Config, "LINK_TRUSTED_NODE_IDS", (message.node_id,)), patch.object(
            Config, "LINK_CHANNEL_ID", "public"
        ), patch.object(remote_link, "_send_neg"), patch.object(
            remote_link, "_after_connected"
        ) as connected:
            remote_link._on_negotiate(message.SerializeToString(), ("127.0.0.1", 1))
            remote_link._on_negotiate(message.SerializeToString(), ("127.0.0.1", 1))
        connected.assert_called_once()
        link = remote_link._links[("127.0.0.1", 1)]
        self.assertEqual(link.state, remote_link.CONNECTED)
        self.assertIsNotNone(link.secure_send)
        self.assertIsNotNone(link.secure_receive)
        with self.assertRaises(HandshakeError):
            remote_link._decrypt_packed(b"plaintext", link.addr)


if __name__ == "__main__":
    unittest.main()
