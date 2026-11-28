"""Etcd v3 channel authority with leased, compare-and-swap writes."""

import base64
from dataclasses import dataclass
import json
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request

from utils import metrics as metrics_registry


class AuthorityError(RuntimeError):
    pass


class NotLeaderError(AuthorityError):
    pass


class _AcquireContention(NotLeaderError):
    pass


def _b64(data):
    return base64.b64encode(data).decode("ascii")


def _unb64(data):
    return base64.b64decode(data)


@dataclass(frozen=True)
class AuthorityCredential:
    channel_id: str
    host_id: str
    app_term: int
    config_generation: int
    lease_id: int
    key_version: int
    key: str
    noop_committed: bool = False
    key_revision: int = 0


class EtcdClient:
    def __init__(
        self,
        endpoint,
        ca_cert=None,
        cert=None,
        key=None,
        username=None,
        password=None,
        timeout=3.0,
        opener=None,
    ):
        self.endpoint = endpoint.rstrip("/")
        parsed = urllib.parse.urlparse(self.endpoint)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("etcd endpoint must be an HTTP(S) URL")
        if username is not None and parsed.scheme != "https" and parsed.hostname not in {
            "127.0.0.1",
            "::1",
            "localhost",
        }:
            raise AuthorityError("etcd credentials require TLS outside loopback")
        self.timeout = float(timeout)
        self._opener = opener or urllib.request.build_opener()
        context = ssl.create_default_context(cafile=ca_cert)
        if cert or key:
            if not cert or not key:
                raise ValueError("client certificate and key must be provided together")
            context.load_cert_chain(cert, key)
        if self.endpoint.startswith("https://"):
            self._opener = urllib.request.build_opener(
                urllib.request.HTTPSHandler(context=context)
            )
        self._headers = {"Content-Type": "application/json"}
        if username is not None:
            token = base64.b64encode(f"{username}:{password or ''}".encode()).decode()
            self._headers["Authorization"] = "Basic " + token

    def _request(self, path, payload):
        request = urllib.request.Request(
            self.endpoint + path,
            data=json.dumps(payload, separators=(",", ":")).encode(),
            headers=self._headers,
            method="POST",
        )
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                raw = response.read()
        except urllib.error.HTTPError as error:
            try:
                detail = error.read().decode("utf-8", errors="replace")[:500]
            except OSError:
                detail = ""
            message = f"etcd request failed: HTTP {error.code}"
            if detail:
                message = f"{message}: {detail}"
            raise AuthorityError(message) from error
        except OSError as error:
            raise AuthorityError("etcd request failed") from error
        try:
            return json.loads(raw) if raw else {}
        except json.JSONDecodeError as error:
            raise AuthorityError("etcd returned invalid JSON") from error

    def range(self, key):
        result = self._request("/v3/kv/range", {"key": _b64(key.encode())})
        if "kvs" not in result:
            return None
        value = result["kvs"][0]
        return {
            "value": _unb64(value["value"]),
            "version": int(value["version"]),
            "lease": int(value.get("lease", 0)),
            "mod_revision": int(value.get("mod_revision", 0)),
        }

    def grant_lease(self, seconds):
        response = self._request(
            "/v3/lease/grant", {"TTL": int(seconds), "ID": 0}
        )
        if "ID" not in response or "TTL" not in response:
            raise AuthorityError("etcd lease grant was rejected")
        return int(response["ID"]), int(response["TTL"])

    def keepalive(self, lease_id):
        response = self._request(
            "/v3/lease/keepalive", {"ID": int(lease_id)}
        )
        if "TTL" not in response or int(response["TTL"]) <= 0:
            raise NotLeaderError("authority lease has expired")
        return int(response["TTL"])

    def time_to_live(self, lease_id):
        response = self._request(
            "/v3/lease/timetolive", {"ID": int(lease_id)}
        )
        if "TTL" not in response:
            raise AuthorityError("etcd lease status is unavailable")
        return int(response["TTL"])

    def revoke(self, lease_id):
        self._request("/v3/lease/revoke", {"ID": int(lease_id)})

    def put(self, key, value, lease_id=0):
        return self._request(
            "/v3/kv/put",
            {"key": _b64(key.encode()), "value": _b64(value), "lease": int(lease_id)},
        )

    def compare_and_put(self, key, expected_version, value, lease_id=0, authority_compare=None):
        if int(expected_version) == 0:
            comparison = {
                "target": "CREATE",
                "key": _b64(key.encode()),
                "createRevision": "0",
            }
        else:
            comparison = {
                "target": "VERSION",
                "key": _b64(key.encode()),
                "version": str(expected_version),
            }
        comparisons = [comparison]
        if authority_compare is not None:
            authority_key, authority_revision = authority_compare
            comparisons.append(
                {
                    "target": "MOD",
                    "key": _b64(authority_key.encode()),
                    "modRevision": str(authority_revision),
                }
            )
        response = self._request(
            "/v3/kv/txn",
            {
                "compare": comparisons,
                "success": [
                    {
                        "request_put": {
                            "key": _b64(key.encode()),
                            "value": _b64(value),
                            "lease": int(lease_id),
                        }
                    }
                ],
                "failure": [],
            },
        )
        if not response.get("succeeded", False):
            raise NotLeaderError("authority version changed")
        return response


    def compare_and_delete(self, key, expected_version, authority_compare=None):
        comparisons = [
            {"target": "VERSION", "key": _b64(key.encode()), "version": str(expected_version)}
        ]
        if authority_compare is not None:
            authority_key, authority_revision = authority_compare
            comparisons.append(
                {
                    "target": "MOD",
                    "key": _b64(authority_key.encode()),
                    "modRevision": str(authority_revision),
                }
            )
        response = self._request(
            "/v3/kv/txn",
            {
                "compare": comparisons,
                "success": [
                    {
                        "request_delete_range": {
                            "key": _b64(key.encode()),
                        }
                    }
                ],
                "failure": [],
            },
        )
        if not response.get("succeeded", False):
            raise NotLeaderError("authority version changed")
        return response


class ChannelAuthority:
    def __init__(self, client, prefix="channels", lease_seconds=10, clock=time.monotonic):
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.client = client
        self.prefix = prefix.strip("/")
        self.lease_seconds = int(lease_seconds)
        self._clock = clock

    def _key(self, channel_id):
        return f"{self.prefix}/{channel_id}/host"

    def _term_key(self, channel_id):
        return f"{self.prefix}/{channel_id}/app-term"

    def _load_term(self, channel_id):
        current = self.client.range(self._term_key(channel_id))
        if current is None:
            return 0
        try:
            term = int(current["value"])
        except (KeyError, TypeError, ValueError) as error:
            raise AuthorityError("channel term record is invalid") from error
        if term < 1:
            raise AuthorityError("channel term record is invalid")
        return term

    def _persist_term(self, channel_id, term):
        key = self._term_key(channel_id)
        current = self.client.range(key)
        current_term = 0
        if current is not None:
            try:
                current_term = int(current["value"])
            except (KeyError, TypeError, ValueError) as error:
                raise AuthorityError("channel term record is invalid") from error
            if current_term < 1:
                raise AuthorityError("channel term record is invalid")
        if current_term > term:
            raise NotLeaderError("channel term advanced concurrently")
        if current_term == term:
            return
        self.client.compare_and_put(
            key,
            current["version"] if current else 0,
            str(term).encode(),
            0,
        )

    @staticmethod
    def _decode(record):
        try:
            value = json.loads(record["value"])
            host_id = value["host_id"]
            app_term = int(value["app_term"])
            config_generation = int(value["config_generation"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise AuthorityError("channel authority record is invalid") from error
        if not isinstance(host_id, str) or not host_id:
            raise AuthorityError("channel authority host is invalid")
        if app_term < 1 or config_generation < 1:
            raise AuthorityError("channel authority versions are invalid")
        return host_id, app_term, config_generation

    def acquire(self, channel_id, host_id, config_generation):
        if not host_id or int(config_generation) < 1:
            raise ValueError("host and config generation are required")
        for _ in range(8):
            try:
                return self._acquire_once(channel_id, host_id, config_generation)
            except _AcquireContention:
                continue
        raise NotLeaderError("channel authority acquisition contended")

    def _acquire_once(self, channel_id, host_id, config_generation):
        channel_id = str(channel_id)
        key = self._key(channel_id)
        current = self.client.range(key)
        if current is not None:
            current_host, current_term, current_generation = self._decode(current)
            if current_generation != int(config_generation):
                raise NotLeaderError("channel config generation changed")
            ttl = self.client.time_to_live(current["lease"]) if current["lease"] else 0
            if current_host == host_id and ttl > 0:
                self._persist_term(channel_id, current_term)
                lease_id, granted_ttl = self.client.grant_lease(self.lease_seconds)
                if granted_ttl <= 0:
                    raise NotLeaderError("etcd did not grant a usable lease")
                value = json.dumps(
                    {
                        "host_id": host_id,
                        "app_term": current_term,
                        "config_generation": current_generation,
                    },
                    separators=(",", ":"),
                ).encode()
                try:
                    self.client.compare_and_put(
                        key, current["version"], value, lease_id
                    )
                except NotLeaderError as error:
                    self.client.revoke(lease_id)
                    raise _AcquireContention(str(error)) from error
                self.client.revoke(current["lease"])
                refreshed = self.client.range(key)
                return AuthorityCredential(
                    channel_id,
                    host_id,
                    current_term,
                    current_generation,
                    lease_id,
                    refreshed["version"],
                    key,
                    key_revision=refreshed["mod_revision"],
                )
            if ttl > 0:
                raise NotLeaderError("channel already has a live host")
            term = max(current_term, self._load_term(channel_id)) + 1
        else:
            term = self._load_term(channel_id) + 1
        lease_id, granted_ttl = self.client.grant_lease(self.lease_seconds)
        if granted_ttl <= 0:
            raise NotLeaderError("etcd did not grant a usable lease")
        value = json.dumps(
            {
                "host_id": host_id,
                "app_term": term,
                "config_generation": int(config_generation),
            },
            separators=(",", ":"),
        ).encode()
        try:
            self._persist_term(channel_id, term)
            self.client.compare_and_put(
                key, current["version"] if current else 0, value, lease_id
            )
        except NotLeaderError as error:
            self.client.revoke(lease_id)
            raise _AcquireContention(str(error)) from error
        stored = self.client.range(key)
        return AuthorityCredential(
            channel_id,
            host_id,
            term,
            int(config_generation),
            lease_id,
            stored["version"],
            key,
            key_revision=stored["mod_revision"],
        )

    def commit_noop(self, credential):
        value = json.dumps(
            {
                "host_id": credential.host_id,
                "app_term": credential.app_term,
                "config_generation": credential.config_generation,
                "committed_at": self._clock(),
            },
            separators=(",", ":"),
        ).encode()
        key = f"{self.prefix}/{credential.channel_id}/noop/{credential.app_term}"
        self.client.compare_and_put(
            key,
            0,
            value,
            0,
            authority_compare=(credential.key, credential.key_revision),
        )
        return AuthorityCredential(
            credential.channel_id,
            credential.host_id,
            credential.app_term,
            credential.config_generation,
            credential.lease_id,
            credential.key_version,
            credential.key,
            True,
            credential.key_revision,
        )

    def renew(self, credential):
        current = self.client.range(credential.key)
        if current is None:
            raise NotLeaderError("channel authority key is gone")
        host_id, term, generation = self._decode(current)
        if (
            host_id != credential.host_id
            or term != credential.app_term
            or generation != credential.config_generation
            or current["version"] != credential.key_version
            or current["mod_revision"] != credential.key_revision
            or current["lease"] != credential.lease_id
        ):
            raise NotLeaderError("channel authority credential is stale")
        if self.client.time_to_live(credential.lease_id) <= 0:
            raise NotLeaderError("channel authority lease has expired")
        lease_id, granted_ttl = self.client.grant_lease(self.lease_seconds)
        if granted_ttl <= 0:
            raise NotLeaderError("etcd did not grant a usable lease")
        self.client.compare_and_put(
            credential.key,
            current["version"],
            current["value"],
            lease_id,
        )
        self.client.revoke(credential.lease_id)
        renewed = self.client.range(credential.key)
        return AuthorityCredential(
            credential.channel_id,
            credential.host_id,
            credential.app_term,
            credential.config_generation,
            lease_id,
            renewed["version"],
            credential.key,
            credential.noop_committed,
            renewed["mod_revision"],
        )

    def verify(self, channel_id, host_id=None, app_term=None, config_generation=None):
        current = self.client.range(self._key(str(channel_id)))
        if current is None:
            raise NotLeaderError("channel authority key does not exist")
        current_host, current_term, current_generation = self._decode(current)
        if host_id is not None and host_id != current_host:
            raise NotLeaderError("channel host mismatch")
        if app_term is not None and int(app_term) != current_term:
            raise NotLeaderError("channel term mismatch")
        if config_generation is not None and int(config_generation) != current_generation:
            raise NotLeaderError("channel config generation mismatch")
        if not current["lease"] or self.client.time_to_live(current["lease"]) <= 0:
            raise NotLeaderError("channel authority lease is not live")
        return current_host, current_term, current_generation

    def commit_state(self, credential, state_key, value):
        if not credential.noop_committed:
            raise NotLeaderError("new leader has not committed its no-op")
        self.client.compare_and_put(
            state_key,
            0,
            value,
            credential.lease_id,
            authority_compare=(credential.key, credential.key_revision),
        )
        metrics_registry.metrics.inc("ofps.authority.state_commits_total")

    def commit_roster(self, credential, value):
        if not credential.noop_committed:
            raise NotLeaderError("new leader has not committed its no-op")
        key = f"{self.prefix}/{credential.channel_id}/roster"
        current = self.client.range(key)
        self.client.compare_and_put(
            key,
            current["version"] if current else 0,
            value,
            0,
            authority_compare=(credential.key, credential.key_revision),
        )
        metrics_registry.metrics.inc("ofps.authority.roster_commits_total")

    def load_roster(self, channel_id):
        current = self.client.range(f"{self.prefix}/{str(channel_id)}/roster")
        return current["value"] if current else None

    def commit_snapshot(self, credential, value):
        if not credential.noop_committed:
            raise NotLeaderError("new leader has not committed its no-op")
        key = f"{self.prefix}/{credential.channel_id}/snapshot"
        current = self.client.range(key)
        self.client.compare_and_put(
            key,
            current["version"] if current else 0,
            value,
            0,
            authority_compare=(credential.key, credential.key_revision),
        )
        metrics_registry.metrics.inc("ofps.authority.snapshot_commits_total")

    def load_snapshot(self, channel_id):
        current = self.client.range(f"{self.prefix}/{str(channel_id)}/snapshot")
        return current["value"] if current else None

    def commit_event(self, credential, event_key, value):
        if not credential.noop_committed:
            raise NotLeaderError("new leader has not committed its no-op")
        try:
            self.client.compare_and_put(
                event_key,
                0,
                value,
                0,
                authority_compare=(credential.key, credential.key_revision),
            )
        except NotLeaderError:
            existing = self.client.range(event_key)
            self.verify(
                credential.channel_id,
                credential.host_id,
                credential.app_term,
                credential.config_generation,
            )
            if existing is None:
                raise
            return False, existing["value"]
        metrics_registry.metrics.inc("ofps.authority.event_commits_total")
        return True, value

    def release(self, credential):
        self.client.revoke(credential.lease_id)
