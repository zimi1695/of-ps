"""Public-channel identities, invitations and replay-safe signed messages.

Generated identities require Ed25519 from ``cryptography`` and fail closed when
it is unavailable. An explicit HMAC-SHA256 helper remains for controlled
compatibility tests, but it is NOT a digital signature: every verifier holds
the signing secret and can impersonate other members. Never use it as the
public-channel trust root.

Node IDs are stable fingerprints of identity public material, not random
session IDs.  Channel membership state and replay history are in memory; use
export_state/restore (protected by utils.secure_store) for durable state.
"""

import base64
import hashlib
import hmac
import json
import secrets
import threading
import time

try:
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives.asymmetric.ed25519 import (
        Ed25519PrivateKey, Ed25519PublicKey,
    )
except ImportError:  # Deliberately no dependency installation or insecure fallback.
    Ed25519PrivateKey = Ed25519PublicKey = None
    InvalidSignature = ImportError


class IdentityError(ValueError):
    """Invalid identity, invitation, message or channel state."""


class ReplayError(IdentityError):
    """A nonce or message ID was already accepted."""


class RevokedMemberError(IdentityError):
    """The sender is not an active channel member."""


def _b64(data):
    return base64.b64encode(data).decode("ascii")


def _unb64(value):
    if not isinstance(value, str):
        raise IdentityError("expected base64 string")
    try:
        return base64.b64decode(value.encode("ascii"), validate=True)
    except (ValueError, UnicodeEncodeError) as exc:
        raise IdentityError("invalid base64") from exc


def _reject_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise IdentityError("duplicate JSON key")
        result[key] = value
    return result


def _check_keys(value):
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise IdentityError("JSON object keys must be strings")
            _check_keys(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _check_keys(item)


def canonical_json(value):
    """Deterministic UTF-8 JSON; rejects NaN, infinity and non-string keys."""
    try:
        _check_keys(value)
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise IdentityError("value is not canonical JSON") from exc


def parse_canonical_json(data):
    try:
        return json.loads(data.decode("utf-8"), object_pairs_hook=_reject_duplicates,
                          parse_constant=lambda value: (_ for _ in ()).throw(
                              IdentityError("non-finite JSON number")))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityError("invalid JSON") from exc


def _node_id(backend, public_material):
    return hashlib.sha256(b"ofps-node-id\0" + backend.encode("ascii") + b"\0"
                          + public_material).hexdigest()


class HmacIdentity:
    """Shared-secret identity; see the module's explicit security boundary."""

    backend = "hmac-sha256"

    def __init__(self, secret, label=None):
        if not isinstance(secret, bytes) or len(secret) < 32:
            raise IdentityError("HMAC secret must be at least 32 bytes")
        if label is not None and not isinstance(label, str):
            raise IdentityError("HMAC label must be a string")
        self._secret = secret
        self._label = label
        self._public_material = hashlib.sha256(
            b"ofps-hmac-public\0" + secret + b"\0" + (label or "").encode("utf-8")
        ).digest()
        self.public_key = _b64(self._public_material)

    @classmethod
    def generate(cls):
        return cls(secrets.token_bytes(32), secrets.token_hex(16))

    @property
    def node_id(self):
        return _node_id(self.backend, self._public_material)

    def sign(self, data):
        return hmac.new(self._secret, b"ofps-signature\0" + data, hashlib.sha256).digest()

    def verify(self, data, signature):
        return hmac.compare_digest(self.sign(data), signature)

    def public_bytes(self):
        return self._public_material


class Ed25519Identity:
    backend = "ed25519"

    def __init__(self, private_key):
        self._private_key = private_key
        self.public_key = _b64(private_key.public_key().public_bytes_raw())

    @classmethod
    def generate(cls):
        return cls(Ed25519PrivateKey.generate())

    @property
    def node_id(self):
        return _node_id(self.backend, _unb64(self.public_key))

    def sign(self, data):
        return self._private_key.sign(data)

    def verify(self, data, signature):
        try:
            Ed25519PublicKey.from_public_bytes(_unb64(self.public_key)).verify(
                signature, data)
            return True
        except (InvalidSignature, ValueError, TypeError):
            return False

    def public_bytes(self):
        return _unb64(self.public_key)


BACKEND = "ed25519" if Ed25519PrivateKey is not None else None


def generate_identity():
    """Generate an Ed25519 identity; missing cryptography fails closed."""
    if BACKEND != "ed25519":
        raise IdentityError("cryptography with Ed25519 support is required")
    return Ed25519Identity.generate()


def identity_from_secret(secret, label=None):
    """Explicit HMAC construction; keep a stable per-node label on restore."""
    return HmacIdentity(secret, label)


def _signed(payload, identity):
    envelope = dict(payload)
    envelope["signature"] = _b64(identity.sign(canonical_json(payload)))
    return envelope


def _verify_envelope(envelope, expected_type):
    if not isinstance(envelope, dict) or envelope.get("type") != expected_type:
        raise IdentityError("unexpected envelope type")
    signature = _unb64(envelope.get("signature"))
    payload = {key: value for key, value in envelope.items() if key != "signature"}
    identity_class = Ed25519Identity if envelope.get("signer_backend") == "ed25519" else HmacIdentity
    if identity_class is Ed25519Identity and Ed25519PrivateKey is None:
        raise IdentityError("Ed25519 backend unavailable")
    node_id = envelope.get("inviter_node_id")
    public_key = envelope.get("inviter_public_key")
    if not isinstance(node_id, str) or not isinstance(public_key, str):
        raise IdentityError("missing signer identity")
    public_material = _unb64(public_key)
    if _node_id(identity_class.backend, public_material) != node_id:
        raise IdentityError("signer identity fingerprint mismatch")
    # HMAC verification needs the actual secret, supplied by ChannelIdentity.
    return payload, signature, identity_class, node_id, public_key


class ChannelIdentity:
    """Thread-safe channel authority, genesis bootstrap and member admission."""

    def __init__(self, identity, channel_id, genesis_invite):
        if not isinstance(channel_id, str) or not channel_id:
            raise IdentityError("channel_id must be a non-empty string")
        self._identity = identity
        self.channel_id = channel_id
        self.genesis_node_id = identity.node_id
        self._members = {}
        self._invites = {}
        self._revoked = set()
        self._nonces = set()
        self._message_ids = set()
        self._lock = threading.RLock()
        self.join(genesis_invite, _bootstrap=True)

    @classmethod
    def create(cls, identity, channel_id, expires_at=None, nonce=None):
        expires_at = int(time.time()) + 86400 if expires_at is None else int(expires_at)
        invite = {
            "type": "genesis_invite", "signer_backend": identity.backend,
            "channel_id": channel_id, "invite_id": secrets.token_hex(16),
            "inviter_node_id": identity.node_id,
            "inviter_public_key": identity.public_key,
            "invitee_node_id": identity.node_id,
            "invitee_public_key": identity.public_key,
            "previous_invite_id": None, "nonce": nonce or secrets.token_hex(16),
            "expires_at": expires_at,
        }
        return cls(identity, channel_id, _signed(invite, identity))

    def genesis_invite(self):
        with self._lock:
            return json.loads(json.dumps(self._invites[self.genesis_node_id]))

    def _identity_for(self, node_id, public_key, backend):
        identity_class = Ed25519Identity if backend == "ed25519" else HmacIdentity
        if identity_class is Ed25519Identity:
            if Ed25519PrivateKey is None:
                raise IdentityError("Ed25519 backend unavailable")
            identity = object.__new__(Ed25519Identity)
            identity._private_key = None
            identity.public_key = public_key
        else:
            # An HMAC verifier must already share the authority's secret.  The
            # public fingerprint prevents accidental admission of other keys.
            if self._identity.backend != "hmac-sha256":
                raise IdentityError("HMAC channel requires an HMAC authority")
            # The public key selects a stable node label, not a distinct secret.
            identity = object.__new__(HmacIdentity)
            identity._secret = self._identity._secret
            identity._label = None
            identity._public_material = _unb64(public_key)
            identity.public_key = public_key
        if identity.node_id != node_id or identity.public_key != public_key:
            raise IdentityError("identity mismatch")
        return identity

    def join(self, invite, _bootstrap=False):
        with self._lock:
            if _bootstrap:
                if invite.get("inviter_node_id") != invite.get("invitee_node_id") or \
                        invite.get("previous_invite_id") is not None:
                    raise IdentityError("invalid genesis invite")
            payload, signature, cls, node_id, public_key = _verify_envelope(
                invite, "genesis_invite")
            if invite.get("signer_backend") != self._identity.backend:
                raise IdentityError("mixed signing backends are not allowed")
            if payload.get("channel_id") != self.channel_id:
                raise IdentityError("invite channel mismatch")
            invitee_id = payload.get("invitee_node_id")
            invitee_public_key = payload.get("invitee_public_key")
            if not isinstance(invitee_id, str) or not invitee_id or \
                    not isinstance(invitee_public_key, str):
                raise IdentityError("missing invitee identity")
            if _node_id(self._identity.backend,
                        _unb64(invitee_public_key)) != invitee_id:
                raise IdentityError("invitee identity fingerprint mismatch")
            if not _bootstrap:
                if node_id not in self._members or node_id in self._revoked:
                    raise RevokedMemberError("inviter is not an active member")
                if invitee_id in self._members:
                    raise IdentityError("already a member")
                if payload.get("previous_invite_id") != self._latest_invite_id(node_id):
                    raise IdentityError("invite is not based on inviter's latest invitation")
                if invitee_id == node_id:
                    raise IdentityError("invitee must differ from inviter")
            elif self._members or node_id != invitee_id or public_key != invitee_public_key:
                raise IdentityError("invalid genesis bootstrap")
            if payload.get("invite_id") in self._invites:
                raise ReplayError("invite already used")
            if not isinstance(payload.get("expires_at"), int) or \
                    payload["expires_at"] < int(time.time()):
                raise IdentityError("invitation expired or invalid")
            if not isinstance(payload.get("nonce"), str) or not payload["nonce"]:
                raise IdentityError("invalid invitation nonce")
            identity = self._identity_for(node_id, public_key, invite["signer_backend"])
            if not identity.verify(canonical_json(payload), signature):
                raise IdentityError("invalid invitation signature")
            self._members[invitee_id] = invitee_public_key
            self._invites[invitee_id] = invite
            return invitee_id

    def _latest_invite_id(self, node_id):
        # For non-genesis members their admission invite remains their latest.
        return self._invites[node_id]["invite_id"]

    def revoke(self, node_id, actor=None):
        with self._lock:
            actor = self.genesis_node_id if actor is None else actor
            if actor != self.genesis_node_id:
                raise IdentityError("only genesis authority may revoke")
            if node_id == self.genesis_node_id:
                raise IdentityError("genesis cannot be revoked")
            if node_id not in self._members:
                raise IdentityError("unknown member")
            self._revoked.add(node_id)

    def is_member(self, node_id):
        with self._lock:
            return node_id in self._members and node_id not in self._revoked

    def members(self):
        with self._lock:
            return {node: key for node, key in self._members.items()
                    if node not in self._revoked}

    def sign_message(self, body, message_id, nonce, sent_at=None, identity=None):
        """Sign as an active member; defaults to this channel's authority key."""
        identity = self._identity if identity is None else identity
        if identity.node_id not in self.members() or \
                identity.public_key != self._members[identity.node_id]:
            raise RevokedMemberError("sender is not active")
        payload = {
            "type": "member_message", "signer_backend": identity.backend,
            "channel_id": self.channel_id, "message_id": message_id, "nonce": nonce,
            "sender_node_id": identity.node_id,
            "sender_public_key": identity.public_key, "body": body,
            "sent_at": int(time.time()) if sent_at is None else int(sent_at),
        }
        return _signed(payload, identity)

    def verify_message(self, envelope):
        """Verify and atomically consume nonce/message_id. Returns sender ID."""
        with self._lock:
            if not isinstance(envelope, dict) or envelope.get("type") != "member_message":
                raise IdentityError("unexpected message type")
            signature = _unb64(envelope.get("signature"))
            payload = {k: v for k, v in envelope.items() if k != "signature"}
            sender = payload.get("sender_node_id")
            if sender not in self._members or sender in self._revoked:
                raise RevokedMemberError("sender is not an active member")
            if payload.get("channel_id") != self.channel_id:
                raise IdentityError("message channel mismatch")
            if payload.get("sender_public_key") != self._members[sender]:
                raise IdentityError("sender public key mismatch")
            if envelope.get("signer_backend") != self._identity.backend:
                raise IdentityError("mixed signing backends are not allowed")
            message_id, nonce = payload.get("message_id"), payload.get("nonce")
            if not isinstance(message_id, str) or not message_id or \
                    not isinstance(nonce, str) or not nonce:
                raise IdentityError("message_id and nonce must be non-empty strings")
            if not isinstance(payload.get("sent_at"), int):
                raise IdentityError("invalid sent_at")
            identity = self._identity_for(sender, payload["sender_public_key"],
                                          envelope["signer_backend"])
            if not identity.verify(canonical_json(payload), signature):
                raise IdentityError("invalid message signature")
            if message_id in self._message_ids or nonce in self._nonces:
                raise ReplayError("replayed message_id or nonce")
            # Mark only after every validation succeeds; application work should
            # be idempotent if it fails after this method returns.
            self._message_ids.add(message_id)
            self._nonces.add(nonce)
            return sender

    def export_state(self):
        with self._lock:
            payload = {
                "type": "channel_state", "version": 1,
                "signer_backend": self._identity.backend,
                "channel_id": self.channel_id,
                "genesis_node_id": self.genesis_node_id,
                "genesis_public_key": self._identity.public_key,
                "inviter_node_id": self.genesis_node_id,
                "inviter_public_key": self._identity.public_key,
                "members": self._members, "invites": self._invites,
                "revoked": sorted(self._revoked),
                "nonces": sorted(self._nonces), "message_ids": sorted(self._message_ids),
            }
            return _signed(payload, self._identity)

    @classmethod
    def restore(cls, snapshot, genesis_node_id, identity=None):
        """Restore only state signed by the explicitly pinned genesis identity.

        ``identity`` is the genesis HMAC secret, required only for HMAC state.
        """
        payload, signature, cls_type, node_id, public_key = _verify_envelope(
            snapshot, "channel_state")
        if payload.get("version") != 1 or node_id != genesis_node_id or \
                public_key != payload.get("genesis_public_key"):
            raise IdentityError("untrusted channel state")
        backend = snapshot.get("signer_backend")
        verifier = identity if backend == "hmac-sha256" else None
        if backend == "hmac-sha256" and (verifier is None or
                                        verifier.backend != backend):
            raise IdentityError("restoring HMAC state requires the genesis secret")
        if backend == "ed25519":
            if Ed25519PrivateKey is None:
                raise IdentityError("Ed25519 backend unavailable")
            verifier = object.__new__(Ed25519Identity)
            verifier._private_key = None
            verifier.public_key = public_key
        if verifier is None or verifier.node_id != node_id or \
                not verifier.verify(canonical_json(payload), signature):
            raise IdentityError("invalid channel state signature")
        members, invites = payload.get("members"), payload.get("invites")
        revoked = payload.get("revoked")
        if not isinstance(members, dict) or not isinstance(invites, dict) or \
                not isinstance(revoked, list) or set(members) != set(invites) or \
                genesis_node_id not in members or genesis_node_id in revoked:
            raise IdentityError("invalid channel state membership")
        for values in (revoked, payload.get("nonces"), payload.get("message_ids")):
            if not isinstance(values, list) or any(not isinstance(v, str) for v in values):
                raise IdentityError("invalid replay state")
        result = object.__new__(cls)
        result._identity = verifier
        result.channel_id = payload.get("channel_id")
        result.genesis_node_id = genesis_node_id
        result._members = dict(members)
        result._invites = dict(invites)
        result._revoked = set(revoked)
        result._nonces = set(payload["nonces"])
        result._message_ids = set(payload["message_ids"])
        result._lock = threading.RLock()
        # Revalidate every persisted invitation before exposing restored state.
        for member_id, invite in invites.items():
            if not isinstance(invite, dict) or invite.get("invitee_node_id") != member_id or \
                    invite.get("invitee_public_key") != members[member_id]:
                raise IdentityError("invalid persisted invitation")
        genesis_invite = invites[genesis_node_id]
        if genesis_invite.get("previous_invite_id") is not None or \
                genesis_invite.get("inviter_node_id") != genesis_node_id:
            raise IdentityError("invalid persisted genesis invite")
        for member_id, invite in invites.items():
            result._verify_stored_invite(invite, member_id)
        return result

    def _verify_stored_invite(self, invite, member_id):
        payload, signature, cls, node_id, public_key = _verify_envelope(
            invite, "genesis_invite")
        identity = self._identity_for(node_id, public_key, invite.get("signer_backend"))
        if not identity.verify(canonical_json(payload), signature) or \
                payload.get("channel_id") != self.channel_id or \
                payload.get("invitee_node_id") != member_id or \
                payload.get("invitee_public_key") != self._members.get(member_id):
            raise IdentityError("invalid persisted invitation signature")
        if node_id not in self._members or self._members[node_id] != public_key:
            raise IdentityError("persisted invite is not a member")
