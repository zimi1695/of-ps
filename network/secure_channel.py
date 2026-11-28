"""Authenticated, replay-protected channel encryption over a reliable byte stream."""

import hashlib
import os
import struct
from threading import Lock

from cryptography.exceptions import InvalidSignature, InvalidTag
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey,
    X25519PublicKey,
)
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes

_MAGIC = b"OFPS1"
_HELLO = b"H"
_CHALLENGE = b"C"
_CONFIRM = b"K"
_FINAL = b"F"
_MAX_FRAME = 65536


class HandshakeError(ValueError):
    pass


def _pub(private):
    return private.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)


def _sign(private, *parts):
    return private.sign(_MAGIC + b"".join(parts))


def _verify(public, signature, *parts, label="handshake"):
    try:
        if isinstance(public, bytes):
            public = public_key_from_bytes(public)
        public.verify(signature, _MAGIC + b"".join(parts))
    except InvalidSignature as error:
        raise HandshakeError(f"invalid {label} signature") from error


def _frame(payload):
    return struct.pack(">H", len(payload)) + payload


def _recv_exact(stream, size):
    data = bytearray()
    while len(data) < size:
        chunk = stream.recv(size - len(data))
        if not chunk:
            raise HandshakeError("handshake stream closed")
        data.extend(chunk)
    return bytes(data)


def _recv_frame(stream):
    size = struct.unpack(">H", _recv_exact(stream, 2))[0]
    if size > _MAX_FRAME:
        raise HandshakeError("handshake frame is too large")
    return _recv_exact(stream, size)


def _derive(secret, transcript):
    return HKDF(
        algorithm=hashes.SHA256(),
        length=64,
        salt=None,
        info=b"ofps-channel-v1" + transcript,
    ).derive(secret)


def handshake(stream, identity_private, peer_identity_public, initiator):
    ephemeral = X25519PrivateKey.generate()
    ephemeral_public = _pub(ephemeral)
    identity_public = _pub(identity_private)
    if initiator:
        hello = (
            _HELLO
            + ephemeral_public
            + identity_public
            + peer_identity_public
        )
        stream.sendall(_frame(hello + _sign(identity_private, hello)))
        response = _recv_frame(stream)
        if len(response) != 1 + 32 + 32 + 32 + 64:
            raise HandshakeError("invalid challenge frame")
        if response[:1] != _CHALLENGE:
            raise HandshakeError("expected challenge")
        peer_ephemeral = response[1:33]
        peer_identity = response[33:65]
        nonce = response[65:97]
        _verify(
            peer_identity_public,
            response[97:],
            _CHALLENGE,
            peer_ephemeral,
            peer_identity,
            hello,
            nonce,
            label="challenge",
        )
        confirm = _CONFIRM + nonce
        stream.sendall(_frame(confirm + _sign(identity_private, hello, response, confirm)))
        final = _recv_frame(stream)
        if len(final) != 1 + 64 or final[:1] != _FINAL:
            raise HandshakeError("invalid final frame")
        _verify(
            peer_identity_public,
            final[1:],
            hello,
            response,
            confirm,
            final[:1],
            label="final",
        )
        transcript = hashlib.sha256(hello + response + confirm + final[:1]).digest()
    else:
        hello = _recv_frame(stream)
        if len(hello) != 1 + 32 + 32 + 32 + 64 or hello[:1] != _HELLO:
            raise HandshakeError("invalid hello frame")
        initiator_ephemeral = hello[1:33]
        initiator_identity = hello[33:65]
        target_identity = hello[65:97]
        _verify(
            peer_identity_public,
            hello[97:],
            hello[:97],
            label="hello",
        )
        if target_identity != identity_public:
            raise HandshakeError("handshake targets a different identity")
        hello_payload = hello[:97]
        nonce = os.urandom(32)
        challenge = _CHALLENGE + ephemeral_public + identity_public + nonce
        response = challenge + _sign(
            identity_private,
            _CHALLENGE,
            ephemeral_public,
            identity_public,
            hello_payload,
            nonce,
        )
        stream.sendall(_frame(response))
        confirmation = _recv_frame(stream)
        if len(confirmation) != 1 + 32 + 64 or confirmation[:1] != _CONFIRM:
            raise HandshakeError("invalid confirmation frame")
        if confirmation[1:33] != nonce:
            raise HandshakeError("challenge nonce mismatch")
        _verify(
            peer_identity_public,
            confirmation[33:],
            hello_payload,
            response,
            confirmation[:33],
            label="confirmation",
        )
        confirmation_payload = confirmation[:33]
        final = _FINAL + _sign(
            identity_private, hello_payload, response, confirmation_payload, _FINAL
        )
        stream.sendall(_frame(final))
        peer_ephemeral = initiator_ephemeral
        transcript = hashlib.sha256(
            hello_payload + response + confirmation_payload + _FINAL
        ).digest()
    shared = ephemeral.exchange(X25519PublicKey.from_public_bytes(peer_ephemeral))
    material = _derive(shared, transcript)
    if initiator:
        return SecureChannel(material[:32], material[32:])
    return SecureChannel(material[32:], material[:32])


class SecureChannel:
    def __init__(self, send_key, receive_key):
        self._send = AESGCM(send_key)
        self._receive = AESGCM(receive_key)
        self._send_counter = 0
        self._receive_counter = 0
        self._lock = Lock()

    def encrypt(self, plaintext):
        with self._lock:
            if self._send_counter >= 2**64:
                raise HandshakeError("send counter exhausted")
            nonce = b"\x00" + self._send_counter.to_bytes(8, "big") + os.urandom(3)
            self._send_counter += 1
            return nonce + self._send.encrypt(nonce, plaintext, None)

    def decrypt(self, framed):
        if len(framed) < 12 or len(framed) > 65536 + 12:
            raise HandshakeError("invalid secure frame length")
        with self._lock:
            counter = int.from_bytes(framed[1:9], "big")
            if counter < self._receive_counter:
                raise HandshakeError("secure frame replay detected")
            try:
                plaintext = self._receive.decrypt(framed[:12], framed[12:], None)
            except InvalidTag as error:
                raise HandshakeError("secure frame authentication failed") from error
            self._receive_counter = counter + 1
            return plaintext


def generate_identity():
    return Ed25519PrivateKey.generate()


def identity_public_bytes(private_key):
    return _pub(private_key)


def public_key_from_bytes(raw):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    return Ed25519PublicKey.from_public_bytes(raw)
