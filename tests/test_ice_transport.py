import unittest
from unittest.mock import AsyncMock, patch

from network.ice_transport import IceConfigurationError, IceServer, IceTransport


class FakeConnection:
    def __init__(self):
        self.gather_candidates = AsyncMock()
        self.connect = AsyncMock()
        self.close = AsyncMock()
        self.remote = []
        self.sent = []

    def local_candidates(self):
        return [type("Candidate", (), {"to_sdp": lambda self: "candidate:local"})()]

    def add_remote_candidate(self, candidate):
        self.remote.append(candidate)

    def sendto(self, data, component):
        self.sent.append((data, component))

    async def recvfrom(self):
        return b"reply", 1

    def get_default_candidate(self, component):
        return "selected"

    @property
    def remote_candidates(self):
        return {1: "remote"}


class IceTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_standard_stun_and_turn_configuration(self):
        connection = FakeConnection()
        with patch("network.ice_transport.aioice.Connection", return_value=connection) as factory, patch(
            "network.ice_transport.aioice.Candidate.from_sdp",
            side_effect=lambda value: value,
        ):
            transport = IceTransport(
                [
                    IceServer("stun:stun.example.test:3478"),
                    IceServer(
                        "turn:turn.example.test:3478?transport=udp",
                        "user",
                        "secret",
                    ),
                ]
            )
            candidates = await transport.gather()
            await transport.connect(["candidate:remote"])
            await transport.send(b"hello")
            self.assertEqual(await transport.receive(), (b"reply", 1))
            await transport.close()
        self.assertEqual(candidates, ["candidate:local"])
        self.assertEqual(connection.remote, ["candidate:remote"])
        self.assertEqual(connection.sent, [(b"hello", 1)])
        kwargs = factory.call_args.kwargs
        self.assertEqual(kwargs["stun_server"], ("stun.example.test", 3478))
        self.assertEqual(kwargs["turn_server"], ("turn.example.test", 3478))
        self.assertEqual(kwargs["turn_username"], "user")
        self.assertEqual(kwargs["turn_transport"], "udp")

    async def test_relay_only_requires_turn(self):
        with self.assertRaises(IceConfigurationError):
            IceTransport([IceServer("stun:stun.example.test:3478")], relay_only=True)

    async def test_turn_credentials_are_required(self):
        with self.assertRaises(IceConfigurationError):
            IceTransport([IceServer("turn:turn.example.test:3478")])

    async def test_non_standard_scheme_is_rejected(self):
        with self.assertRaises(IceConfigurationError):
            IceTransport([IceServer("http://example.test:3478")])


if __name__ == "__main__":
    unittest.main()
