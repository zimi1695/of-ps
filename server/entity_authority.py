"""Explicit, signed entity ownership transfers with monotonic versions."""

from dataclasses import dataclass
import base64
import json
import re
from threading import RLock

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from proto.net_pb2 import EntityOwnershipTransfer
from server import shared_projection
from server.etcd_authority import AuthorityError

_DOMAIN = re.compile(r"^[a-z][a-z0-9_]*(?::[a-z0-9_-]+)+$")


class OwnershipError(ValueError):
    pass


class StaleOwnershipError(OwnershipError):
    pass


def transfer_payload(transfer):
    return {
        "domain": transfer.domain,
        "authority_epoch": int(transfer.authority_epoch),
        "entity_id": int(transfer.entity_id),
        "prior_owner_generation": int(transfer.prior_owner_generation),
        "prior_owner_id": transfer.prior_owner_id,
        "new_owner_generation": int(transfer.new_owner_generation),
        "new_owner_id": transfer.new_owner_id,
        "transfer_seq": int(transfer.transfer_seq),
        "issued_at_us": int(transfer.issued_at_us),
        "new_owner_public_key": base64.b64encode(
            transfer.new_owner_public_key
        ).decode("ascii"),
        "signer_public_key": base64.b64encode(
            transfer.signer_public_key
        ).decode("ascii"),
    }


def sign_transfer(transfer, private_key):
    payload = json.dumps(
        transfer_payload(transfer), sort_keys=True, separators=(",", ":")
    ).encode()
    transfer.signature = private_key.sign(payload)
    return transfer


def verify_transfer(transfer, public_key):
    if isinstance(public_key, bytes):
        public_key = Ed25519PublicKey.from_public_bytes(public_key)
    if transfer.signer_public_key:
        expected = public_key.public_bytes(Encoding.Raw, PublicFormat.Raw)
        if transfer.signer_public_key != expected:
            raise OwnershipError("ownership transfer signer does not match public key")
    payload = json.dumps(
        transfer_payload(transfer), sort_keys=True, separators=(",", ":")
    ).encode()
    try:
        public_key.verify(transfer.signature, payload)
    except (InvalidSignature, ValueError, TypeError) as error:
        raise OwnershipError("invalid ownership transfer signature") from error


@dataclass(frozen=True)
class EntityVersion:
    domain: str
    authority_epoch: int
    entity_id: int
    owner_generation: int
    owner_id: str
    sequence: int

    def order_key(self):
        return (
            self.authority_epoch,
            self.owner_generation,
            self.sequence,
        )


class OwnershipRegistry:
    def __init__(self):
        self._items = {}
        self._lock = RLock()

    def get(self, domain, entity_id):
        key = _entity_key(domain, entity_id)
        with self._lock:
            return self._items.get(key)

    def snapshot(self):
        with self._lock:
            return [
                {
                    "domain": item.domain,
                    "authority_epoch": item.authority_epoch,
                    "entity_id": item.entity_id,
                    "owner_generation": item.owner_generation,
                    "owner_id": item.owner_id,
                    "sequence": item.sequence,
                }
                for item in self._items.values()
            ]

    def restore(self, items, authority_epoch=None):
        if not isinstance(items, list):
            raise OwnershipError("ownership snapshot must be a list")
        restored = {}
        for item in items:
            if not isinstance(item, dict):
                raise OwnershipError("ownership snapshot item is invalid")
            domain = str(item.get("domain", ""))
            entity_id = int(item.get("entity_id", 0))
            _entity_key(domain, entity_id)
            candidate = EntityVersion(
                domain,
                int(item.get("authority_epoch", 0)),
                entity_id,
                int(item.get("owner_generation", 0)),
                str(item.get("owner_id", "")),
                int(item.get("sequence", 0)),
            )
            if (
                candidate.authority_epoch < 1
                or candidate.owner_generation < 1
                or candidate.sequence < 1
                or not candidate.owner_id
            ):
                raise OwnershipError("ownership snapshot item is invalid")
            if authority_epoch is not None and candidate.authority_epoch > int(
                authority_epoch
            ):
                raise StaleOwnershipError("ownership snapshot is from a newer authority")
            restored[(domain, entity_id)] = candidate
        with self._lock:
            self._items = restored

    def clear(self):
        with self._lock:
            self._items.clear()

    def record(self, item):
        key = _entity_key(item.domain, item.entity_id)
        with self._lock:
            self._items[key] = item
        return item

    def apply(self, transfer: EntityOwnershipTransfer, public_key):
        key = _entity_key(transfer.domain, transfer.entity_id)
        if not transfer.prior_owner_id or not transfer.new_owner_id:
            raise OwnershipError("ownership domain and owner IDs are required")
        if int(transfer.authority_epoch) < 1:
            raise OwnershipError("authority epoch must be positive")
        if min(
            int(transfer.prior_owner_generation),
            int(transfer.new_owner_generation),
            int(transfer.transfer_seq),
        ) < 0:
            raise OwnershipError("ownership versions cannot be negative")
        if int(transfer.new_owner_generation) <= int(transfer.prior_owner_generation):
            raise OwnershipError("new owner generation must increase")
        verify_transfer(transfer, public_key)
        candidate = EntityVersion(
            transfer.domain,
            int(transfer.authority_epoch),
            int(transfer.entity_id),
            int(transfer.new_owner_generation),
            transfer.new_owner_id,
            int(transfer.transfer_seq),
        )
        with self._lock:
            current = self._items.get(key)
            if current is not None:
                if int(transfer.authority_epoch) < current.authority_epoch:
                    raise StaleOwnershipError("ownership authority epoch is stale")
                if candidate.order_key() <= current.order_key():
                    raise StaleOwnershipError("ownership transfer is not newer")
                if current.owner_id != transfer.prior_owner_id:
                    raise OwnershipError("prior owner does not match current owner")
                if int(transfer.prior_owner_generation) != current.owner_generation:
                    raise OwnershipError("prior owner generation does not match")
            self._items[key] = candidate
        return candidate


def _entity_key(domain, entity_id):
    if not isinstance(domain, str) or not _DOMAIN.fullmatch(domain):
        raise OwnershipError("ownership domain is invalid")
    if isinstance(entity_id, bool) or int(entity_id) <= 0:
        raise OwnershipError("ownership entity_id must be positive")
    return str(domain), int(entity_id)


def entity_domain(kind, scene_id, channel_id, player_id=None):
    if kind not in {"gather", "monster", "drop", "campfire"}:
        raise OwnershipError("unsupported entity domain")
    base = f"scene:{int(scene_id)}:channel:{int(channel_id)}:{kind}"
    if player_id is None:
        return base
    if isinstance(player_id, bool) or int(player_id) <= 0:
        raise OwnershipError("entity player scope is invalid")
    return f"{base}:player:{int(player_id)}"


_active = None
_active_lock = RLock()


def activate(owner_id, private_key):
    global _active
    if not isinstance(owner_id, str) or not owner_id:
        raise OwnershipError("local owner ID is required")
    if not isinstance(private_key, Ed25519PrivateKey):
        raise OwnershipError("local ownership key is invalid")
    authority = EntityAuthority(owner_id, private_key)
    with _active_lock:
        _active = authority
    return authority


def deactivate(authority):
    global _active
    with _active_lock:
        if _active is authority:
            _active = None


def active():
    with _active_lock:
        return _active


class EntityAuthority:
    def __init__(self, owner_id, private_key):
        self.owner_id = str(owner_id)
        self.private_key = private_key
        self.public_key = private_key.public_key()
        self.public_key_bytes = private_key.public_key().public_bytes(
            Encoding.Raw, PublicFormat.Raw
        )
        self.registry = OwnershipRegistry()
        self.store = None
        self.credential_provider = None

    def configure_store(self, store, credential_provider):
        self.store = store
        self.credential_provider = credential_provider

    def _credential(self):
        if self.credential_provider is None:
            raise OwnershipError("entity authority store is not configured")
        credential = self.credential_provider()
        if credential is None:
            raise OwnershipError("channel authority credential is unavailable")
        return credential

    @staticmethod
    def _store_call(action, *args):
        try:
            return action(*args)
        except AuthorityError as error:
            raise OwnershipError("entity authority storage is unavailable") from error

    @staticmethod
    def _from_transfer(transfer):
        return EntityVersion(
            transfer.domain,
            int(transfer.authority_epoch),
            int(transfer.entity_id),
            int(transfer.new_owner_generation),
            transfer.new_owner_id,
            int(transfer.transfer_seq),
        )

    def _epoch(self):
        return int(shared_projection.store.roster_metadata()["authority_epoch"])

    def claim(self, domain, entity_id):
        key = _entity_key(domain, entity_id)
        if self.store is not None:
            current = self.store.load(domain, entity_id)
            if current is not None:
                item = self._from_transfer(current["transfer"])
                self.registry.record(item)
                return item
            transfer = sign_transfer(
                EntityOwnershipTransfer(
                    domain=domain,
                    authority_epoch=self._credential().app_term,
                    entity_id=int(entity_id),
                    prior_owner_generation=0,
                    prior_owner_id="unowned",
                    new_owner_generation=1,
                    new_owner_id=self.owner_id,
                    transfer_seq=1,
                    new_owner_public_key=self.public_key_bytes,
                    signer_public_key=self.public_key_bytes,
                ),
                self.private_key,
            )
            current = self._store_call(self.store.claim, self._credential(), transfer)
            return self.registry.record(self._from_transfer(current["transfer"]))
        with self.registry._lock:
            current = self.registry._items.get(key)
            if current is not None:
                if current.owner_id != self.owner_id:
                    raise OwnershipError("entity is owned by another host")
                return current
            transfer = sign_transfer(
                EntityOwnershipTransfer(
                    domain=domain,
                    authority_epoch=self._epoch(),
                    entity_id=int(entity_id),
                    prior_owner_generation=0,
                    prior_owner_id="unowned",
                    new_owner_generation=1,
                    new_owner_id=self.owner_id,
                    transfer_seq=1,
                    new_owner_public_key=self.public_key_bytes,
                    signer_public_key=self.public_key_bytes,
                ),
                self.private_key,
            )
            return self.registry.apply(transfer, self.public_key)

    def assert_owner(self, domain, entity_id, require_shared=False):
        if require_shared and self.store is None:
            raise OwnershipError("shared entity authority is unavailable")
        current = self.claim(domain, entity_id)
        if self.store is not None:
            channel = self._store_call(self.store.channel_state)
            if current.authority_epoch > channel["app_term"]:
                raise StaleOwnershipError("entity belongs to a newer authority")
            if current.authority_epoch < channel["app_term"]:
                credential = self._credential()
                if credential.host_id != self.owner_id:
                    raise StaleOwnershipError("entity authority epoch is stale")
                current = self.takeover(domain, entity_id)
        if current.owner_id != self.owner_id:
            raise OwnershipError("entity is owned by another host")
        return current

    def transfer(self, domain, entity_id, new_owner_id, new_owner_public_key=None):
        if not isinstance(new_owner_id, str) or not new_owner_id:
            raise OwnershipError("new owner ID is required")
        if self.store is not None and (
            not isinstance(new_owner_public_key, bytes)
            or len(new_owner_public_key) != 32
        ):
            raise OwnershipError("new entity owner public key is required")
        current = self.assert_owner(domain, entity_id)
        transfer = sign_transfer(
            EntityOwnershipTransfer(
                domain=current.domain,
                authority_epoch=current.authority_epoch,
                entity_id=current.entity_id,
                prior_owner_generation=current.owner_generation,
                prior_owner_id=current.owner_id,
                new_owner_generation=current.owner_generation + 1,
                new_owner_id=new_owner_id,
                transfer_seq=current.sequence + 1,
                new_owner_public_key=new_owner_public_key or b"",
                signer_public_key=self.public_key_bytes,
            ),
            self.private_key,
        )
        if self.store is not None:
            stored = self.store.load(domain, entity_id)
            if stored is None:
                raise OwnershipError("entity ownership record is missing")
            current = self._store_call(
                self.store.transfer, transfer, stored["version"]
            )
            return self.registry.record(self._from_transfer(current["transfer"]))
        self.registry.apply(transfer, self.public_key)
        return transfer

    def takeover(self, domain, entity_id):
        if self.store is None:
            raise OwnershipError("entity authority store is not configured")
        credential = self._credential()
        self._store_call(self.store.verify_credential, credential)
        stored = self.store.load(domain, entity_id)
        if stored is None:
            raise OwnershipError("entity ownership record is missing")
        owner = stored["transfer"]
        if (
            owner.new_owner_id == self.owner_id
            and owner.authority_epoch == credential.app_term
        ):
            return self.registry.record(self._from_transfer(owner))
        transfer = sign_transfer(
            EntityOwnershipTransfer(
                domain=owner.domain,
                authority_epoch=credential.app_term,
                entity_id=owner.entity_id,
                prior_owner_generation=owner.new_owner_generation,
                prior_owner_id=owner.new_owner_id,
                new_owner_generation=owner.new_owner_generation + 1,
                new_owner_id=self.owner_id,
                transfer_seq=owner.transfer_seq + 1,
                new_owner_public_key=self.public_key_bytes,
                signer_public_key=self.public_key_bytes,
            ),
            self.private_key,
        )
        current = self._store_call(self.store.takeover, credential, transfer)
        return self.registry.record(self._from_transfer(current["transfer"]))

    def release(self, domain, entity_id):
        key = _entity_key(domain, entity_id)
        if self.store is not None:
            current = self.store.load(domain, entity_id)
            if current is None:
                return False
            if current["transfer"].new_owner_id != self.owner_id:
                raise OwnershipError("entity is owned by another host")
            self._store_call(
                self.store.release, domain, entity_id, current["version"]
            )
        with self.registry._lock:
            return self.registry._items.pop(key, None) is not None

    def receive(self, transfer, public_key):
        return self.registry.apply(transfer, public_key)

    def snapshot(self):
        return self.registry.snapshot()

    def restore(self, items):
        self.registry.restore(items, authority_epoch=self._epoch())
