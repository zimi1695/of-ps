import socket
import threading
import unittest

from network.secure_channel import (
    HandshakeError,
    generate_identity,
    handshake,
    identity_public_bytes,
)


class SecureChannelTests(unittest.TestCase):
    def pair(self, alice_peer=None, bob_peer=None):
        alice = generate_identity()
        bob = generate_identity()
        left, right = socket.socketpair()
        left.settimeout(3)
        right.settimeout(3)
        results = {}
        errors = []

        def run(name, stream, identity, peer, initiator):
            try:
                results[name] = handshake(
                    stream,
                    identity,
                    identity_public_bytes(peer),
                    initiator,
                )
            except Exception as error:
                errors.append(error)

        threads = [
            threading.Thread(
                target=run,
                args=(
                    "alice",
                    left,
                    alice,
                    alice_peer or bob,
                    True,
                ),
            ),
            threading.Thread(
                target=run,
                args=(
                    "bob",
                    right,
                    bob,
                    bob_peer or alice,
                    False,
                ),
            ),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5)
        left.close()
        right.close()
        self.assertFalse(any(thread.is_alive() for thread in threads))
        return results, errors

    def test_handshake_derives_matching_confidential_channels(self):
        channels, errors = self.pair()
        self.assertEqual(errors, [])
        alice = channels["alice"]
        bob = channels["bob"]
        self.assertEqual(bob.decrypt(alice.encrypt(b"hello")), b"hello")
        self.assertEqual(alice.decrypt(bob.encrypt(b"reply")), b"reply")

    def test_tampered_frame_is_rejected(self):
        channels, errors = self.pair()
        self.assertEqual(errors, [])
        frame = bytearray(channels["alice"].encrypt(b"hello"))
        frame[-1] ^= 1
        with self.assertRaises(HandshakeError):
            channels["bob"].decrypt(bytes(frame))

    def test_replayed_frame_is_rejected(self):
        channels, errors = self.pair()
        self.assertEqual(errors, [])
        frame = channels["alice"].encrypt(b"hello")
        self.assertEqual(channels["bob"].decrypt(frame), b"hello")
        with self.assertRaises(HandshakeError):
            channels["bob"].decrypt(frame)

    def test_wrong_peer_identity_is_rejected(self):
        _, errors = self.pair(bob_peer=generate_identity())
        self.assertTrue(errors)
        self.assertIsInstance(errors[0], HandshakeError)


if __name__ == "__main__":
    unittest.main()
