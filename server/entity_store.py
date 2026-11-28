"""Persistent entity ownership records with signed, compare-and-swap updates."""

import base64
import hashlib
import json

from proto.net_pb2 import EntityOwnershipTransfer
from server.entity_authority import (
    OwnershipError,
    StaleOwnershipError,
    transfer_payload,
    verify_transfer,
)
from server.etcd_authority import NotLeaderError


def _b64(value):
    return base64.b64encode(value).decode("ascii")


def _unb64(value):
    return base64.b64decode(value, validate=True)


class EntityOwnershipStore:
    def __init__(self, client, channel_id, prefix="channels"):
        self.client = client
        self.channel_id = str(channel_id)
        self.prefix = prefix.strip("/")

    def _key(self, domain, entity_id):
        digest = hashlib.sha256(f"{domain}\0{int(entity_id)}".encode()).hexdigest()
        return f"{self.prefix}/{self.channel_id}/entity/{digest}"

    @staticmethod
    def _encode(transfer):
        return json.dumps(
            {
                **transfer_payload(transfer),
                "signature": _b64(transfer.signature),
                "new_owner_public_key": _b64(transfer.new_owner_public_key),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()

    @staticmethod
    def _decode(record):
        try:
            value = json.loads(record["value"])
            transfer = EntityOwnershipTransfer(
                domain=value["domain"],
                authority_epoch=int(value["authority_epoch"]),
                entity_id=int(value["entity_id"]),
                prior_owner_generation=int(value["prior_owner_generation"]),
                prior_owner_id=value["prior_owner_id"],
                new_owner_generation=int(value["new_owner_generation"]),
                new_owner_id=value["new_owner_id"],
                transfer_seq=int(value["transfer_seq"]),
                signature=_unb64(value["signature"]),
                issued_at_us=int(value["issued_at_us"]),
                new_owner_public_key=_unb64(value["new_owner_public_key"]),
                signer_public_key=_unb64(value["signer_public_key"]),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise OwnershipError("stored entity ownership is invalid") from error
        if len(transfer.new_owner_public_key) != 32 or len(transfer.signer_public_key) != 32:
            raise OwnershipError("stored entity owner public key is invalid")
        verify_transfer(transfer, transfer.signer_public_key)
        return {
            "transfer": transfer,
            "version": int(record["version"]),
            "mod_revision": int(record.get("mod_revision", 0)),
        }

    def load(self, domain, entity_id):
        current = self.client.range(self._key(domain, entity_id))
        if current is None:
            return None
        decoded = self._decode(current)
        transfer = decoded["transfer"]
        if transfer.domain != str(domain) or int(transfer.entity_id) != int(entity_id):
            raise OwnershipError("stored entity ownership key does not match payload")
        return decoded

    def channel_state(self, credential=None):
        key = (
            credential.key
            if credential is not None
            else f"{self.prefix}/{self.channel_id}/host"
        )
        current = self.client.range(key)
        if current is None:
            raise NotLeaderError("channel authority key is gone")
        try:
            value = json.loads(current["value"])
            host_id = value["host_id"]
            app_term = int(value["app_term"])
            config_generation = int(value["config_generation"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise OwnershipError("channel authority record is invalid") from error
        lease_id = int(current.get("lease", 0))
        if not lease_id or self.client.time_to_live(lease_id) <= 0:
            raise NotLeaderError("channel authority lease is not live")
        return {
            "host_id": host_id,
            "app_term": app_term,
            "config_generation": config_generation,
            "version": int(current["version"]),
            "mod_revision": int(current.get("mod_revision", 0)),
            "lease_id": lease_id,
        }

    def verify_credential(self, credential):
        current = self.channel_state(credential)
        if (
            current["host_id"] != credential.host_id
            or current["app_term"] != credential.app_term
            or current["config_generation"] != credential.config_generation
            or current["version"] != credential.key_version
            or current["mod_revision"] != credential.key_revision
            or current["lease_id"] != credential.lease_id
        ):
            raise NotLeaderError("channel authority credential is stale")

    def claim(self, credential, transfer):
        self.verify_credential(credential)
        if (
            transfer.prior_owner_id != "unowned"
            or transfer.prior_owner_generation != 0
            or transfer.new_owner_generation != 1
            or transfer.transfer_seq != 1
            or transfer.new_owner_id != credential.host_id
            or transfer.authority_epoch != credential.app_term
        ):
            raise OwnershipError("initial entity claim is invalid")
        if len(transfer.new_owner_public_key) != 32:
            raise OwnershipError("entity owner public key is required")
        if transfer.signer_public_key != transfer.new_owner_public_key:
            raise OwnershipError("initial entity claim signer is invalid")
        verify_transfer(transfer, transfer.new_owner_public_key)
        key = self._key(transfer.domain, transfer.entity_id)
        value = self._encode(transfer)
        try:
            self.client.compare_and_put(
                key,
                0,
                value,
                0,
                authority_compare=(credential.key, credential.key_revision),
            )
        except NotLeaderError:
            current = self.load(transfer.domain, transfer.entity_id)
            if current is None:
                raise
            return current
        return {"transfer": transfer, "version": 1, "mod_revision": 0}

    def transfer(self, transfer, expected_version):
        current = self.load(transfer.domain, transfer.entity_id)
        if current is None or current["version"] != int(expected_version):
            raise StaleOwnershipError("entity ownership changed")
        owner = current["transfer"]
        if (
            owner.new_owner_id != transfer.prior_owner_id
            or owner.new_owner_generation != transfer.prior_owner_generation
        ):
            raise OwnershipError("transfer prior owner does not match")
        if len(transfer.new_owner_public_key) != 32:
            raise OwnershipError("new entity owner public key is required")
        if transfer.signer_public_key != owner.new_owner_public_key:
            raise OwnershipError("entity transfer signer is not the current owner")
        verify_transfer(transfer, owner.new_owner_public_key)
        self.client.compare_and_put(
            self._key(transfer.domain, transfer.entity_id),
            current["version"],
            self._encode(transfer),
            0,
        )
        return {"transfer": transfer, "version": current["version"] + 1, "mod_revision": 0}

    def release(self, domain, entity_id, expected_version):
        self.client.compare_and_delete(
            self._key(domain, entity_id), int(expected_version)
        )

    def takeover(self, credential, transfer):
        self.verify_credential(credential)
        current = self.load(transfer.domain, transfer.entity_id)
        if current is None:
            raise OwnershipError("cannot take over an unclaimed entity")
        owner = current["transfer"]
        if (
            transfer.prior_owner_id != owner.new_owner_id
            or transfer.prior_owner_generation != owner.new_owner_generation
            or transfer.new_owner_generation <= owner.new_owner_generation
            or transfer.transfer_seq <= owner.transfer_seq
            or transfer.authority_epoch != credential.app_term
            or transfer.new_owner_id != credential.host_id
        ):
            raise OwnershipError("authority entity takeover is invalid")
        if len(transfer.new_owner_public_key) != 32:
            raise OwnershipError("new entity owner public key is required")
        if transfer.signer_public_key != transfer.new_owner_public_key:
            raise OwnershipError("authority takeover signer is invalid")
        verify_transfer(transfer, transfer.new_owner_public_key)
        self.client.compare_and_put(
            self._key(transfer.domain, transfer.entity_id),
            current["version"],
            self._encode(transfer),
            0,
            authority_compare=(credential.key, credential.key_revision),
        )
        return {"transfer": transfer, "version": current["version"] + 1, "mod_revision": 0}
