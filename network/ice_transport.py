"""Standards-based STUN/TURN/ICE transport using aioice."""

from dataclasses import dataclass
from urllib.parse import urlparse

import aioice


class IceConfigurationError(ValueError):
    pass


@dataclass(frozen=True)
class IceServer:
    url: str
    username: str = ""
    credential: str = ""


def _server_parts(server):
    scheme, separator, address = server.url.partition(":")
    parsed = urlparse("//" + address) if separator else urlparse(server.url)
    if scheme not in {"stun", "stuns", "turn", "turns"}:
        raise IceConfigurationError("ICE server URL must use stun or turn")
    if not parsed.hostname or not parsed.port:
        raise IceConfigurationError("ICE server URL must include host and port")
    return parsed._replace(scheme=scheme)


class IceTransport:
    def __init__(self, servers, controlling=True, relay_only=False):
        servers = list(servers)
        if not servers:
            raise IceConfigurationError("at least one STUN or TURN server is required")
        parsed = [_server_parts(server) for server in servers]
        stun = next((item for item in parsed if item.scheme in {"stun", "stuns"}), None)
        turn = next((item for item in parsed if item.scheme in {"turn", "turns"}), None)
        if relay_only and turn is None:
            raise IceConfigurationError("relay-only mode requires a TURN server")
        self.controlling = bool(controlling)
        self.relay_only = relay_only
        self._stun = (stun.hostname, stun.port) if stun else None
        self._turn = (turn.hostname, turn.port) if turn else None
        self._turn_ssl = bool(turn and turn.scheme == "turns")
        self._turn_transport = (turn.query.split("=", 1)[1] if turn and "transport=" in turn.query else "udp")
        if self._turn_transport not in {"udp", "tcp"}:
            raise IceConfigurationError("TURN transport must be udp or tcp")
        self._username = turn_username = next(
            (
                server.username
                for server in servers
                if server.url.partition(":")[0] in {"turn", "turns"}
            ),
            "",
        )
        self._credential = next(
            (
                server.credential
                for server in servers
                if server.url.partition(":")[0] in {"turn", "turns"}
            ),
            "",
        )
        if turn and (not turn_username or not self._credential):
            raise IceConfigurationError("TURN requires username and credential")
        self.connection = aioice.Connection(
            ice_controlling=self.controlling,
            stun_server=self._stun,
            turn_server=self._turn,
            turn_username=self._username if turn else None,
            turn_password=self._credential if turn else None,
            turn_ssl=self._turn_ssl,
            turn_transport=self._turn_transport,
            transport_policy=(
                aioice.TransportPolicy.RELAY
                if relay_only
                else aioice.TransportPolicy.ALL
            ),
        )

    async def gather(self):
        await self.connection.gather_candidates()
        return [candidate.to_sdp() for candidate in self.connection.local_candidates()]

    async def connect(self, remote_candidates):
        for candidate in remote_candidates:
            self.connection.add_remote_candidate(aioice.Candidate.from_sdp(candidate))
        await self.connection.connect()

    async def send(self, data, component=1):
        self.connection.sendto(data, component)

    async def receive(self):
        return await self.connection.recvfrom()

    async def close(self):
        await self.connection.close()
