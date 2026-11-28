"""Gateway recovery state: sessions, watermarks, pending requests and responses."""

from dataclasses import dataclass, field
import base64
import json
from threading import RLock


class GatewayStateError(ValueError):
    pass


@dataclass
class GatewayState:
    sessions: dict = field(default_factory=dict)
    next_event_seq: dict = field(default_factory=dict)
    watermarks: dict = field(default_factory=dict)
    pending: dict = field(default_factory=dict)
    responses: dict = field(default_factory=dict)
    leader_credential: dict = field(default_factory=dict)

    def to_dict(self):
        return {
            "version": 1,
            "sessions": self.sessions,
            "next_event_seq": self.next_event_seq,
            "watermarks": self.watermarks,
            "pending": self.pending,
            "responses": self.responses,
            "leader_credential": self.leader_credential,
        }

    @classmethod
    def from_dict(cls, value):
        if not isinstance(value, dict) or value.get("version") != 1:
            raise GatewayStateError("unsupported gateway state")
        state = cls()
        for name in (
            "sessions",
            "next_event_seq",
            "watermarks",
            "pending",
            "responses",
            "leader_credential",
        ):
            data = value.get(name, {})
            if not isinstance(data, dict):
                raise GatewayStateError(f"invalid gateway {name}")
            setattr(state, name, data)
        return state


def encrypted_gateway_state(path, entropy=b"ofps-gateway-state-v1"):
    from utils import secure_store

    def read():
        try:
            with open(path, "rb") as stream:
                return stream.read()
        except FileNotFoundError:
            return None

    return PersistentGatewayState(
        encrypt=lambda value: secure_store.protect_bytes(value, entropy),
        decrypt=lambda value: secure_store.unprotect_bytes(value, entropy),
        write=lambda value: secure_store.atomic_write(path, value),
        read=read,
    )


class PersistentGatewayState:
    def __init__(self, state=None, encrypt=None, decrypt=None, write=None, read=None):
        self.state = state or GatewayState()
        self._encrypt = encrypt
        self._decrypt = decrypt
        self._write = write
        self._read = read
        self._lock = RLock()

    @staticmethod
    def _key(*parts):
        return json.dumps([str(part) for part in parts], separators=(",", ":"))

    def bind_session(self, member_id, session_generation, channel_id):
        if int(session_generation) < 1:
            raise GatewayStateError("session generation must be positive")
        with self._lock:
            self.state.sessions[self._key(member_id)] = {
                "member_id": int(member_id),
                "session_generation": int(session_generation),
                "channel_id": int(channel_id),
            }
            self.flush()

    def session(self, member_id):
        with self._lock:
            value = self.state.sessions.get(self._key(member_id))
            return dict(value) if value else None

    def next_event_seq(self, member_id, session_generation, channel_id):
        key = self._key(member_id, session_generation, channel_id)
        with self._lock:
            value = int(self.state.next_event_seq.get(key, 0)) + 1
            self.state.next_event_seq[key] = value
            return value

    @staticmethod
    def _pack_blob(value):
        if isinstance(value, bytes):
            return {
                "encoding": "base64",
                "value": base64.b64encode(value).decode("ascii"),
            }
        return value

    @staticmethod
    def _unpack_blob(value):
        if isinstance(value, dict) and value.get("encoding") == "base64":
            return base64.b64decode(value["value"], validate=True)
        return value

    def set_pending(self, member_id, session_generation, channel_id, event_seq, payload):
        key = self._key(member_id, session_generation, channel_id, event_seq)
        with self._lock:
            self.state.pending[key] = self._pack_blob(payload)
            self.flush()

    def complete(self, member_id, session_generation, channel_id, event_seq, response):
        key = self._key(member_id, session_generation, channel_id, event_seq)
        with self._lock:
            self.state.responses[key] = self._pack_blob(response)
            self.state.pending.pop(key, None)
            self.flush()

    def recover_response(self, member_id, session_generation, channel_id, event_seq):
        key = self._key(member_id, session_generation, channel_id, event_seq)
        with self._lock:
            return self._unpack_blob(self.state.responses.get(key))

    def set_watermark(self, channel_id, config_generation, authority_epoch, roster_seq):
        value = (
            int(config_generation),
            int(authority_epoch),
            int(roster_seq),
        )
        if min(value) < 0 or value[0] < 1 or value[1] < 1:
            raise GatewayStateError("invalid channel watermark")
        with self._lock:
            self.state.watermarks[str(channel_id)] = list(value)
            self.flush()

    def watermark(self, channel_id):
        with self._lock:
            value = self.state.watermarks.get(str(channel_id), [1, 1, 0])
            return tuple(value)

    def set_leader_credential(self, credential):
        with self._lock:
            self.state.leader_credential = dict(credential)
            self.flush()

    def flush(self):
        if self._write is None:
            return
        if self._encrypt is None:
            raise GatewayStateError("gateway persistence requires encryption")
        payload = json.dumps(
            self.state.to_dict(), ensure_ascii=False, separators=(",", ":")
        ).encode()
        self._write(self._encrypt(payload))

    def load(self):
        if self._read is None:
            return self.state
        if self._decrypt is None:
            raise GatewayStateError("gateway persistence requires decryption")
        encrypted = self._read()
        if encrypted is None:
            return self.state
        try:
            value = json.loads(self._decrypt(encrypted))
        except Exception as error:
            raise GatewayStateError("unable to decrypt gateway state") from error
        self.state = GatewayState.from_dict(value)
        return self.state
