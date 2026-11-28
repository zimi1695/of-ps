import os
import tempfile
import unittest
from pathlib import Path

from server.gateway_state import (
    GatewayStateError,
    PersistentGatewayState,
    encrypted_gateway_state,
)


class PersistentGatewayStateTests(unittest.TestCase):
    def setUp(self):
        self.stored = [None]

        def encrypt(value):
            return b"enc:" + bytes(byte ^ 0x5A for byte in value)

        def decrypt(value):
            if not value.startswith(b"enc:"):
                raise ValueError("bad envelope")
            return bytes(byte ^ 0x5A for byte in value[4:])

        self.state = PersistentGatewayState(
            encrypt=encrypt,
            decrypt=decrypt,
            write=lambda value: self.stored.__setitem__(0, value),
            read=lambda: self.stored[0],
        )

    def test_session_sequence_pending_and_response_survive_reload(self):
        self.state.bind_session(7, 8, 2)
        self.assertEqual(self.state.next_event_seq(7, 8, 2), 1)
        self.assertEqual(self.state.next_event_seq(7, 8, 2), 2)
        self.state.set_pending(7, 8, 2, 2, b"request")
        self.state.complete(7, 8, 2, 2, b"response")
        self.state.set_watermark(2, 3, 4, 5)
        self.state.set_leader_credential({"host_id": "host-a", "app_term": 4})
        restored = PersistentGatewayState(
            encrypt=self.state._encrypt,
            decrypt=self.state._decrypt,
            write=self.state._write,
            read=self.state._read,
        )
        restored.load()
        self.assertEqual(restored.session(7)["session_generation"], 8)
        self.assertEqual(restored.next_event_seq(7, 8, 2), 3)
        self.assertEqual(restored.recover_response(7, 8, 2, 2), b"response")
        self.assertEqual(restored.state.pending, {})
        self.assertEqual(restored.watermark(2), (3, 4, 5))
        self.assertEqual(restored.state.leader_credential["host_id"], "host-a")

    def test_plaintext_persistence_is_rejected(self):
        state = PersistentGatewayState(write=lambda value: None)
        with self.assertRaises(GatewayStateError):
            state.bind_session(1, 1, 1)

    def test_corrupt_state_fails_closed(self):
        self.stored[0] = b"not-encrypted"
        with self.assertRaises(GatewayStateError):
            self.state.load()

    def test_invalid_watermark_is_rejected(self):
        with self.assertRaises(GatewayStateError):
            self.state.set_watermark(1, 0, 1, 0)

    @unittest.skipUnless(os.name == "nt", "DPAPI requires Windows")
    def test_dpapi_gateway_file_is_not_plaintext_and_reloads(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "gateway.bin"
            state = encrypted_gateway_state(path)
            state.bind_session(7, 8, 2)
            self.assertNotIn(b"session_generation", path.read_bytes())
            restored = encrypted_gateway_state(path)
            restored.load()
            self.assertEqual(restored.session(7)["session_generation"], 8)


if __name__ == "__main__":
    unittest.main()
