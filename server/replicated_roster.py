"""Versioned online-roster replication through the channel authority log."""

import json
from threading import Lock, RLock

from server import shared_projection
from server.scene_data import _session_list as session_list, lock_session

_active = None
_active_lock = RLock()


def activate(replicator):
    global _active
    with _active_lock:
        _active = replicator


def deactivate(replicator):
    global _active
    with _active_lock:
        if _active is replicator:
            _active = None


def active():
    with _active_lock:
        return _active


class RosterReplicator:
    def __init__(self, authority, credential_provider, paused=False):
        self.authority = authority
        self._credential_provider = (
            credential_provider
            if callable(credential_provider)
            else lambda: credential_provider
        )
        self._last_members = None
        self._publish_lock = Lock()
        self._paused = bool(paused)
        self._publish_pending = False

    @property
    def credential(self):
        return self._credential_provider()

    def pause(self):
        with self._publish_lock:
            self._paused = True

    def resume(self):
        with self._publish_lock:
            self._paused = False
            self._publish_pending = False
            return self._publish()

    def publish(self):
        with self._publish_lock:
            if self._paused:
                self._publish_pending = True
                return None
            return self._publish()

    def _publish(self):
        credential = self.credential
        with lock_session:
            members = [
                {
                    "player_id": int(session.player_id),
                    "scene_id": int(session.scene_id),
                    "channel_id": int(session.channel_id),
                    "remote": bool(getattr(session, "remote", False)),
                    "server_name": getattr(session, "server_name", ""),
                }
                for session in session_list
                if getattr(session, "running", False)
                and getattr(session, "logged_in", False)
            ]
        members.sort(key=lambda item: item["player_id"])
        if members != self._last_members:
            shared_projection.store.bump_roster()
            self._last_members = members
        metadata = shared_projection.store.roster_metadata()
        value = json.dumps(
            {**metadata, "members": members},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
        current = self.authority.load_roster(credential.channel_id)
        if current is not None:
            try:
                previous = json.loads(current)
                previous_version = (
                    int(previous["config_generation"]),
                    int(previous["authority_epoch"]),
                    int(previous["roster_seq"]),
                )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                raise ValueError("stored roster metadata is invalid") from error
            candidate_version = (
                metadata["config_generation"],
                metadata["authority_epoch"],
                metadata["roster_seq"],
            )
            if candidate_version < previous_version:
                raise ValueError("stale roster cannot replace a newer roster")
        self.authority.commit_roster(credential, value)
        return value
