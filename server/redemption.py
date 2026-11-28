"""Atomic redemption-code, reward-claim and invite-relation operations.

Every mutation and its item grant shares one player-locked SQLite transaction.
Successful packet operations also store a fingerprint and their original
response in that transaction, so retries replay without repeating the grant.
"""

import base64
import hashlib
import json
import re
import time

import utils.db as db
from utils import item_mgr
from server.payment_security import audit, audit_best_effort

_CODE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{3,127}$")
_INVITE_NAMESPACE = "invite"
_OPERATION_NS = "activity_operations"
_MAX_REWARDS = 128
_MAX_REWARD_COUNT = 1_000_000


def normalize_code(code):
    if not isinstance(code, str):
        return ""
    code = code.strip()
    return code if _CODE_RE.fullmatch(code) else ""


def code_fingerprint(code):
    return hashlib.sha256(code.encode("utf-8")).hexdigest()


def parse_rewards(rewards_str):
    if not isinstance(rewards_str, str) or not rewards_str or len(rewards_str) > 8192:
        raise ValueError("Invalid redemption reward configuration")
    merged = {}
    pairs = rewards_str.split(",")
    if len(pairs) > _MAX_REWARDS:
        raise ValueError("Too many redemption rewards")
    for pair in pairs:
        if pair.count(":") != 1:
            raise ValueError("Invalid redemption reward pair")
        item_raw, count_raw = pair.split(":", 1)
        item_raw, count_raw = item_raw.strip(), count_raw.strip()
        if not item_raw.isdigit() or not count_raw.isdigit():
            raise ValueError("Invalid redemption reward number")
        item_id, count = int(item_raw), int(count_raw)
        if not 0 < item_id <= 2**31 - 1 or not 0 < count <= _MAX_REWARD_COUNT:
            raise ValueError("Invalid redemption reward value")
        merged[item_id] = merged.get(item_id, 0) + count
        if merged[item_id] > _MAX_REWARD_COUNT:
            raise ValueError("Redemption reward count is too large")
    if not merged:
        raise ValueError("Redemption code has no rewards")
    return tuple(merged.items())


def validate_rewards(rewards):
    if not isinstance(rewards, (list, tuple)) or not rewards or len(rewards) > _MAX_REWARDS:
        raise ValueError("Invalid reward list")
    result = []
    for reward in rewards:
        if not isinstance(reward, (list, tuple)) or len(reward) != 2:
            raise ValueError("Invalid reward entry")
        item_id, count = reward
        if type(item_id) is not int or type(count) is not int:
            raise ValueError("Invalid reward number")
        if not 0 < item_id <= 2**31 - 1 or not 0 < count <= _MAX_REWARD_COUNT:
            raise ValueError("Invalid reward value")
        result.append((item_id, count))
    return tuple(result)


def _operation_key(system, activity_id, packet_id):
    return f"{system}:{activity_id}:{packet_id}"


def _load_operation(player_id, system, activity_id, packet_id, fingerprint):
    raw = db.get_kv(
        player_id, _OPERATION_NS, _operation_key(system, activity_id, packet_id)
    )
    if raw is None:
        return None
    try:
        stored = json.loads(bytes(raw).decode("utf-8"))
        if not isinstance(stored, dict) or stored.get("fingerprint") != fingerprint:
            return "conflict"
        response = base64.b64decode(stored["response"], validate=True)
        pack = base64.b64decode(stored["pack"], validate=True)
        return stored["status"], stored.get("payload"), response, pack
    except (KeyError, TypeError, ValueError, UnicodeDecodeError):
        return "conflict"


def _save_operation(
    player_id, system, activity_id, packet_id, fingerprint, status, payload, response, pack
):
    value = json.dumps(
        {
            "fingerprint": fingerprint,
            "status": status,
            "payload": payload,
            "response": base64.b64encode(
                response.SerializeToString() if response is not None else b""
            ).decode("ascii"),
            "pack": base64.b64encode(
                pack.SerializeToString() if pack is not None else b""
            ).decode("ascii"),
        },
        separators=(",", ":"),
    ).encode("utf-8")
    db.set_kv(
        player_id, _OPERATION_NS, _operation_key(system, activity_id, packet_id), value
    )


def run_operation(
    player_id, system, activity_id, packet_id, fingerprint, response, pack, callback
):
    """Run ``callback`` under one lock/transaction and persist successful replays.

    The callback returns ``(status, payload)`` and must perform all reads and
    writes.  Status 0 or ``"ok"`` is considered committed success.  A corrupt
    record or a different fingerprint is rejected as ``"conflict"``.
    """
    with item_mgr.player_lock(player_id), db.transaction():
        if type(packet_id) is int and packet_id > 0:
            prior = _load_operation(
                player_id, system, activity_id, packet_id, fingerprint
            )
            if prior == "conflict":
                return "conflict", None
            if prior is not None:
                status, payload, response_blob, pack_blob = prior
                if response is not None:
                    response.Clear()
                    response.ParseFromString(response_blob)
                if pack is not None:
                    pack.Clear()
                    pack.ParseFromString(pack_blob)
                return status, payload
        status, payload = callback()
        if (
            type(packet_id) is int
            and packet_id > 0
            and (status == "ok" or (type(status) is int and status == 0))
        ):
            _save_operation(
                player_id, system, activity_id, packet_id, fingerprint,
                status, payload, response, pack,
            )
        return status, payload


def claim_reward(
    player_id, system, key, rewards, rsp_rewards, pack_notice,
    packet_id=0, fingerprint=None, response=None, on_success=None,
):
    """Atomically validate a server reward, claim it and grant it once."""
    if fingerprint is None:
        fingerprint = json.dumps([str(system), str(key)], separators=(",", ":"))

    def apply():
        try:
            validated = validate_rewards(rewards)
        except ValueError:
            return "invalid_rewards", None
        if not db.try_claim(player_id, system, key):
            return "already_claimed", None
        db.grant_rewards(player_id, validated, rsp_rewards, pack_notice)
        if on_success is not None:
            on_success()
        return "ok", None

    return run_operation(
        player_id, system, 0, packet_id, fingerprint, response, pack_notice, apply
    )


def _redeem_code_locked(player_id, normalized, code_type, rewards_out, pack_notice, now):
    current_time = int(time.time() if now is None else now)
    fingerprint = code_fingerprint(normalized)
    row = db.conn.execute(
        "SELECT code_type, rewards, max_uses, used, expire_time FROM redeem_codes WHERE code=?",
        (normalized,),
    ).fetchone()
    if row is None or row[0] != code_type:
        audit(player_id, "redeem_code", "not_found", code_hash=fingerprint)
        return 1, ()
    _, rewards_str, max_uses, used, expire_time = row
    if type(max_uses) is not int or max_uses < 0 or type(used) is not int or used < 0:
        audit(player_id, "redeem_code", "invalid_config", code_hash=fingerprint)
        return 6, ()
    if type(expire_time) is not int or expire_time < 0:
        audit(player_id, "redeem_code", "invalid_config", code_hash=fingerprint)
        return 6, ()
    if expire_time and current_time >= expire_time:
        audit(player_id, "redeem_code", "expired", code_hash=fingerprint)
        return 2, ()
    if max_uses and used >= max_uses:
        audit(player_id, "redeem_code", "exhausted", code_hash=fingerprint)
        return 3, ()
    try:
        rewards = parse_rewards(rewards_str)
    except ValueError:
        audit(player_id, "redeem_code", "invalid_rewards", code_hash=fingerprint)
        return 6, ()

    prior = db.conn.execute(
        "SELECT 1 FROM redeem_code_uses WHERE code=? AND player_id=?",
        (normalized, player_id),
    ).fetchone()
    if prior is not None:
        audit(player_id, "redeem_code", "already_used", code_hash=fingerprint)
        return 4, ()
    if db.conn.execute(
        "SELECT 1 FROM players WHERE player_id=?", (player_id,)
    ).fetchone() is None:
        audit(player_id, "redeem_code", "player_missing", code_hash=fingerprint)
        return 1, ()

    cursor = db.conn.execute(
        """UPDATE redeem_codes SET used=used+1
           WHERE code=? AND code_type=?
             AND (max_uses=0 OR used<max_uses)
             AND (expire_time=0 OR expire_time>?)""",
        (normalized, code_type, current_time),
    )
    if cursor.rowcount != 1:
        cursor.close()
        audit(player_id, "redeem_code", "exhausted", code_hash=fingerprint)
        return 3, ()
    cursor.close()
    cursor = db.conn.execute(
        "INSERT INTO redeem_code_uses (code, player_id, use_time) VALUES (?, ?, ?)",
        (normalized, player_id, current_time),
    )
    cursor.close()
    # Any exception from the item layer rolls back the use marker, counter and
    # operation record, allowing a safe retry.
    db.grant_rewards(player_id, rewards, rewards_out, pack_notice)
    audit(player_id, "redeem_code", "fulfilled", code_hash=fingerprint)
    return 0, rewards


def redeem_code(
    player_id, code, rsp_rewards, pack_notice, now=None, *,
    code_type="redeem", packet_id=0, response=None,
):
    """Return ``(status, rewards)``; status 0 means the grant committed.

    Statuses: 1 missing/type mismatch, 2 expired, 3 exhausted, 4 player already
    used it, 5 malformed code, 6 malformed server reward configuration.
    ``packet_id`` adds fingerprint-checked response replay for legacy handlers.
    """
    normalized = normalize_code(code)
    fingerprint = json.dumps([normalized], separators=(",", ":"))

    def apply():
        if not normalized:
            audit_best_effort(player_id, "redeem_code", "invalid_code")
            return 5, ()
        return _redeem_code_locked(
            player_id, normalized, code_type, rsp_rewards, pack_notice, now
        )

    status, payload = run_operation(
        player_id, f"redeem:{code_type}", 0, packet_id,
        fingerprint, response, pack_notice, apply,
    )
    if status == 0 and isinstance(payload, (list, tuple)):
        payload = tuple(tuple(reward) for reward in payload)
    return status, payload


def register_invite_code(code, owner_id):
    """Register an owner's reusable invite code (GM/admin integration helper)."""
    normalized = normalize_code(code)
    if not normalized or type(owner_id) is not int or owner_id <= 0:
        raise ValueError("Invalid invite code or owner")
    key = f"code:{normalized}"
    with item_mgr.player_lock(owner_id), db.transaction():
        if db.conn.execute("SELECT 1 FROM players WHERE player_id=?", (owner_id,)).fetchone() is None:
            raise ValueError("Invite owner does not exist")
        existing = db.get_kv(0, _INVITE_NAMESPACE, key)
        if existing is not None and int(existing) != owner_id:
            raise ValueError("Invite code already belongs to another player")
        db.set_kv(0, _INVITE_NAMESPACE, key, str(owner_id))
        audit(owner_id, "invite_code", "registered", code_hash=code_fingerprint(normalized))
    return normalized


def _load_invitees(owner_id):
    raw = db.get_kv(owner_id, _INVITE_NAMESPACE, "invitees")
    if raw is None:
        return []
    try:
        values = json.loads(bytes(raw).decode("utf-8"))
    except (TypeError, ValueError, UnicodeDecodeError) as error:
        raise ValueError("Corrupt invite relation state") from error
    if not isinstance(values, list) or any(type(value) is not int or value <= 0 for value in values):
        raise ValueError("Corrupt invite relation state")
    if len(values) != len(set(values)):
        raise ValueError("Corrupt invite relation state")
    return values


def invite_count(owner_id):
    return len(_load_invitees(owner_id))


def _would_create_cycle(invitee_id, inviter_id):
    """An inviter may not already be reachable below the invitee."""
    pending = [invitee_id]
    visited = set()
    while pending:
        current = pending.pop()
        if current == inviter_id:
            return True
        if current in visited:
            continue
        visited.add(current)
        if len(visited) > 10_000:
            raise ValueError("Invite relation graph is too large")
        pending.extend(_load_invitees(current))
    return False


def use_invite_code(
    player_id, code, rewards, rsp_rewards, pack_notice, packet_id=0, response=None,
):
    """Bind one invitee to one inviter and grant the invitee reward atomically."""
    normalized = normalize_code(code)
    fingerprint = json.dumps([normalized], separators=(",", ":"))

    def apply():
        if not normalized:
            audit_best_effort(player_id, "invite_code_use", "invalid_code")
            return "invalid_code", 0
        try:
            validated = validate_rewards(rewards)
        except ValueError:
            audit_best_effort(player_id, "invite_code_use", "invalid_rewards")
            return "invalid_rewards", 0
        code_hash = code_fingerprint(normalized)
        if db.get_kv(player_id, _INVITE_NAMESPACE, "inviter") is not None or db.get_kv(
            player_id, _INVITE_NAMESPACE, "used"
        ) is not None:
            audit(player_id, "invite_code_use", "already_filled", code_hash=code_hash)
            return "already_filled", 0
        owner_raw = db.get_kv(0, _INVITE_NAMESPACE, f"code:{normalized}")
        if owner_raw is None:
            audit(player_id, "invite_code_use", "not_found", code_hash=code_hash)
            return "not_found", 0
        try:
            owner_id = int(owner_raw)
        except (TypeError, ValueError) as error:
            raise ValueError("Corrupt invite owner mapping") from error
        if owner_id == player_id:
            audit(player_id, "invite_code_use", "self_invite", code_hash=code_hash)
            return "self_invite", owner_id
        if db.conn.execute("SELECT 1 FROM players WHERE player_id=?", (owner_id,)).fetchone() is None:
            audit(player_id, "invite_code_use", "owner_missing", code_hash=code_hash)
            return "not_found", 0
        if db.conn.execute("SELECT 1 FROM players WHERE player_id=?", (player_id,)).fetchone() is None:
            audit(player_id, "invite_code_use", "invitee_missing", code_hash=code_hash)
            return "not_found", 0

        invitees = _load_invitees(owner_id)
        if player_id in invitees:
            audit(player_id, "invite_code_use", "already_filled", code_hash=code_hash)
            return "already_filled", owner_id
        if _would_create_cycle(player_id, owner_id):
            audit(player_id, "invite_code_use", "cycle_rejected", code_hash=code_hash)
            return "cycle", owner_id
        invitees.append(player_id)
        db.set_kv(owner_id, _INVITE_NAMESPACE, "invitees", json.dumps(invitees).encode("ascii"))
        db.set_kv(player_id, _INVITE_NAMESPACE, "inviter", str(owner_id))
        db.set_kv(player_id, _INVITE_NAMESPACE, "used", b"1")
        db.set_kv(0, _INVITE_NAMESPACE, f"relation:{player_id}", str(owner_id))
        db.grant_rewards(player_id, validated, rsp_rewards, pack_notice)
        audit(
            owner_id, "invite_code_use", "fulfilled", invitee_id=player_id,
            code_hash=code_hash, invitee_reward=True,
        )
        return "ok", owner_id

    return run_operation(
        player_id, "invite-code-use", 0, packet_id,
        fingerprint, response, pack_notice, apply,
    )


def claim_invite_reward(
    player_id, activity_id, condition_id, rewards, required_count,
    predecessor_id, rsp_rewards, pack_notice, packet_id=0, response=None,
):
    """Claim an inviter reward using server-derived valid invitee count."""
    if (
        type(required_count) is not int
        or not 1 <= required_count <= 1000
        or type(activity_id) is not int
        or activity_id <= 0
        or type(condition_id) is not int
        or condition_id <= 0
    ):
        return "invalid_request"
    fingerprint = json.dumps([activity_id, condition_id], separators=(",", ":"))

    def apply():
        try:
            validated = validate_rewards(rewards)
        except ValueError:
            return "invalid_rewards", None
        count = invite_count(player_id)
        if count < required_count:
            audit(
                player_id, "invite_reward", "not_completed", activity_id=activity_id,
                condition_id=condition_id, invitee_count=count, required_count=required_count,
            )
            return "not_completed", None
        claim_key = f"{activity_id}:{condition_id}"
        if predecessor_id and not db.has_claimed(
            player_id, "activity_invite", f"{activity_id}:{predecessor_id}"
        ):
            audit(
                player_id, "invite_reward", "predecessor_missing", activity_id=activity_id,
                condition_id=condition_id,
            )
            return "not_completed", None
        if not db.try_claim(player_id, "activity_invite", claim_key):
            audit(player_id, "invite_reward", "already_claimed", activity_id=activity_id, condition_id=condition_id)
            return "already_claimed", None
        db.grant_rewards(player_id, validated, rsp_rewards, pack_notice)
        audit(
            player_id, "invite_reward", "fulfilled", activity_id=activity_id,
            condition_id=condition_id, invitee_count=count,
        )
        return "ok", None

    status, _payload = run_operation(
        player_id, "invite-reward", activity_id, packet_id,
        fingerprint, response, pack_notice, apply,
    )
    return status
