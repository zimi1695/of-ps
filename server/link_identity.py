"""Persistent Ed25519 identities and signed link negotiation messages."""

import base64
import hashlib
import secrets
import time

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)

from server.channel_identity import IdentityError, canonical_json
from utils.secure_store import SecureStore


class LinkIdentityError(IdentityError):
    pass


def generate_private_key():
    return Ed25519PrivateKey.generate()


def serialize_private_key(private_key):
    return private_key.private_bytes(
        Encoding.Raw,
        PrivateFormat.Raw,
        NoEncryption(),
    )


def load_private_key(raw):
    if not isinstance(raw, bytes) or len(raw) != 32:
        raise LinkIdentityError("invalid Ed25519 private key")
    return Ed25519PrivateKey.from_private_bytes(raw)


def public_key_bytes(private_key):
    return private_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)


def node_id_for_public_key(public_key):
    if not isinstance(public_key, bytes) or len(public_key) != 32:
        raise LinkIdentityError("invalid Ed25519 public key")
    return hashlib.sha256(b"ofps-link-node-v1\0" + public_key).hexdigest()


def load_or_create_identity(path):
    store = SecureStore(path, entropy=b"ofps-link-identity-v1")
    try:
        raw = store.read_bytes()
    except FileNotFoundError:
        raw = serialize_private_key(generate_private_key())
        store.write_bytes(raw)
    return load_private_key(raw)


def negotiation_payload(message):
    return {
        "type": int(message.type),
        "conv": int(message.conv),
        "server_name": message.server_name,
        "is_public": bool(message.is_public),
        "node_id": message.node_id,
        "public_key": base64.b64encode(message.public_key).decode("ascii"),
        "channel_id": message.channel_id,
        "nonce": int(message.nonce),
        "issued_at_us": int(message.issued_at_us),
        "ephemeral_public": base64.b64encode(message.ephemeral_public).decode("ascii"),
    }


def sign_negotiation(message, private_key):
    public_key = public_key_bytes(private_key)
    message.node_id = node_id_for_public_key(public_key)
    message.public_key = public_key
    message.signature = private_key.sign(canonical_json(negotiation_payload(message)))
    return message


def link_message_payload(message):
    return {
        "id": message.id,
        "origin": message.origin,
        "type": int(message.type),
        "addrs": [{"host": item.host, "port": int(item.port)} for item in message.addrs],
        "player_info": base64.b64encode(message.player_info.SerializeToString()).decode("ascii"),
        "datas": list(message.datas),
        "msg_id": int(message.msg_id),
        "payload": base64.b64encode(message.payload).decode("ascii"),
        "peer_names": list(message.peer_names),
        "subs": list(message.subs),
        "has_state": bool(message.has_state),
        "config_generation": int(message.config_generation),
        "authority_epoch": int(message.authority_epoch),
        "event_seq": int(message.event_seq),
        "session_generation": int(message.session_generation),
        "trace_id": int(message.trace_id),
        "roster_seq": int(message.roster_seq),
        "origin_node_id": message.origin_node_id,
        "origin_public_key": base64.b64encode(message.origin_public_key).decode("ascii"),
        "nonce": int(message.nonce),
        "issued_at_us": int(message.issued_at_us),
    }


def sign_link_message(message, private_key):
    message.origin_node_id = node_id_for_public_key(public_key_bytes(private_key))
    message.origin_public_key = public_key_bytes(private_key)
    if not message.nonce:
        message.nonce = secrets.randbits(63) or 1
    if not message.issued_at_us:
        message.issued_at_us = time.time_ns() // 1000
    message.signature = private_key.sign(canonical_json(link_message_payload(message)))
    return message


def verify_link_message(message, trusted_node_ids, now_us=None, max_skew_us=120_000_000):
    if message.origin_node_id not in set(trusted_node_ids):
        raise LinkIdentityError("untrusted link message origin")
    if len(message.origin_public_key) != 32:
        raise LinkIdentityError("invalid link message public key")
    if node_id_for_public_key(message.origin_public_key) != message.origin_node_id:
        raise LinkIdentityError("link message fingerprint mismatch")
    now_us = time.time_ns() // 1000 if now_us is None else int(now_us)
    if abs(now_us - int(message.issued_at_us)) > int(max_skew_us):
        raise LinkIdentityError("link message timestamp is stale")
    if not message.id or not message.nonce or not message.signature:
        raise LinkIdentityError("link message is missing replay metadata or signature")
    try:
        Ed25519PublicKey.from_public_bytes(message.origin_public_key).verify(
            message.signature,
            canonical_json(link_message_payload(message)),
        )
    except (InvalidSignature, ValueError) as error:
        raise LinkIdentityError("invalid link message signature") from error
    return message.origin_node_id


def verify_negotiation(message, trusted_node_ids, channel_id, now_us=None, max_skew_us=30_000_000):
    if message.node_id not in set(trusted_node_ids):
        raise LinkIdentityError("untrusted link node")
    if message.channel_id != channel_id:
        raise LinkIdentityError("link channel mismatch")
    if len(message.public_key) != 32 or len(message.ephemeral_public) != 32:
        raise LinkIdentityError("invalid link public key")
    if node_id_for_public_key(message.public_key) != message.node_id:
        raise LinkIdentityError("link node fingerprint mismatch")
    now_us = time.time_ns() // 1000 if now_us is None else int(now_us)
    if abs(now_us - int(message.issued_at_us)) > int(max_skew_us):
        raise LinkIdentityError("link negotiation timestamp is stale")
    if not message.nonce or not message.signature:
        raise LinkIdentityError("missing link negotiation nonce or signature")
    try:
        Ed25519PublicKey.from_public_bytes(message.public_key).verify(
            message.signature,
            canonical_json(negotiation_payload(message)),
        )
    except (InvalidSignature, ValueError) as error:
        raise LinkIdentityError("invalid link negotiation signature") from error
    return message.node_id
