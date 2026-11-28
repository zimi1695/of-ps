"""Idempotent strong-mode event records stored through channel authority."""

import base64
import hashlib
import json

from threading import RLock

from utils import metrics as metrics_registry

_active_log = None
_active_log_lock = RLock()


def activate(log):
    global _active_log
    with _active_log_lock:
        _active_log = log


def deactivate(log):
    global _active_log
    with _active_log_lock:
        if _active_log is log:
            _active_log = None


def active_log():
    with _active_log_lock:
        return _active_log


class ReplicatedEventLog:
    def __init__(self, authority, credential_provider):
        self.authority = authority
        self._credential_provider = (
            credential_provider
            if callable(credential_provider)
            else lambda: credential_provider
        )

    @property
    def credential(self):
        return self._credential_provider()

    def _key(
        self,
        member_id,
        session_generation,
        channel_id,
        message_type,
        event_seq,
        credential=None,
    ):
        credential = credential or self.credential
        identity = json.dumps(
            [
                int(member_id),
                int(session_generation),
                int(channel_id),
                str(message_type),
                int(event_seq),
            ],
            separators=(",", ":"),
        ).encode()
        digest = hashlib.sha256(identity).hexdigest()
        return f"{self.authority.prefix}/{credential.channel_id}/events/{digest}"

    def append_once(
        self,
        member_id,
        session_generation,
        channel_id,
        message_type,
        event_seq,
        payload,
        response,
    ):
        credential = self.credential
        key = self._key(
            member_id,
            session_generation,
            channel_id,
            message_type,
            event_seq,
            credential=credential,
        )
        value = json.dumps(
            {
                "payload": base64.b64encode(payload).decode("ascii"),
                "response": base64.b64encode(response).decode("ascii"),
            },
            separators=(",", ":"),
        ).encode()
        committed, stored = self.authority.commit_event(credential, key, value)
        if not committed:
            metrics_registry.metrics.inc("ofps.events.replayed_total")
        return base64.b64decode(json.loads(stored)["response"]), committed

    def append(
        self,
        member_id,
        session_generation,
        channel_id,
        message_type,
        event_seq,
        payload,
        response,
    ):
        response, _ = self.append_once(
            member_id,
            session_generation,
            channel_id,
            message_type,
            event_seq,
            payload,
            response,
        )
        return response

    def recover_response(
        self, member_id, session_generation, channel_id, message_type, event_seq
    ):
        key = self._key(
            member_id, session_generation, channel_id, message_type, event_seq
        )
        record = self.authority.client.range(key)
        if record is None:
            return None
        return base64.b64decode(json.loads(record["value"])["response"])
