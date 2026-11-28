"""邮件附件领取：配置预校验、原样发放实例、一次性账本与邮件状态同事务提交。"""

import base64
import binascii
from dataclasses import dataclass, field
import hashlib
import json

from google.protobuf.message import DecodeError

from proto.net_pb2 import (
    ItemDetail,
    MailBriefData,
    MailState,
    OperateMailsRsp,
    PackNotice,
    StatusCode,
)
import utils.db as db
from utils import item_mgr
from server.character_growth import item_config


CLAIM_SYSTEM = "mail_attachment"
_OPERATION_SYSTEM = "mail-attachment"
_OPERATION_NS = "activity_operations"
_INSTANCE_KINDS = ("weapon", "armor", "poster", "pet", "treasure")
_INSTANCE_TAGS = {"weapon": 2, "armor": 3, "poster": 5, "pet": 56, "treasure": 53}
_STACKABLE_EXCLUDED_TAGS = (2, 3, 5, 7, 34, 56)


class MailRewardError(Exception):
    def __init__(self, status):
        super().__init__(StatusCode.Name(status))
        self.status = status


@dataclass
class ClaimResult:
    mails: list = field(default_factory=list)
    gains: list = field(default_factory=list)
    already_claimed: bool = False


def _fingerprint(mail_id, claim_all):
    value = json.dumps([int(mail_id), bool(claim_all)], separators=(",", ":"))
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _operation_key(packet_id):
    return f"{_OPERATION_SYSTEM}:0:{packet_id}"


def _load_operation(player_id, packet_id, fingerprint):
    raw = db.get_kv(player_id, _OPERATION_NS, _operation_key(packet_id))
    if raw is None:
        return None
    try:
        stored = json.loads(bytes(raw).decode("utf-8"))
        if not isinstance(stored, dict) or stored.get("fingerprint") != fingerprint:
            return "conflict"
        if type(stored.get("already_claimed")) is not bool:
            return "conflict"
        mails = [
            MailBriefData.FromString(base64.b64decode(blob, validate=True))
            for blob in stored["mails"]
        ]
        gains = [(int(item[0]), int(item[1])) for item in stored["gains"]]
        items = [
            ItemDetail.FromString(base64.b64decode(blob, validate=True))
            for blob in stored["items"]
        ]
        pack = PackNotice.FromString(base64.b64decode(stored["pack"], validate=True))
        if any(not mail.mail_id for mail in mails):
            return "conflict"
        return ClaimResult(mails, gains, stored["already_claimed"]), items, pack
    except (
        KeyError,
        IndexError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        DecodeError,
        binascii.Error,
    ):
        return "conflict"


def _save_operation(player_id, packet_id, fingerprint, result, items, pack):
    if not packet_id:
        return
    value = json.dumps(
        {
            "fingerprint": fingerprint,
            "already_claimed": result.already_claimed,
            "mails": [
                base64.b64encode(mail.SerializeToString()).decode("ascii")
                for mail in result.mails
            ],
            "gains": [[item_id, count] for item_id, count in result.gains],
            "items": [
                base64.b64encode(item.SerializeToString()).decode("ascii")
                for item in items
            ],
            "pack": base64.b64encode(pack.SerializeToString()).decode("ascii"),
        },
        separators=(",", ":"),
    ).encode("utf-8")
    db.set_kv(player_id, _OPERATION_NS, _operation_key(packet_id), value)


def _prepare_rewards(player_id, mail, seen_instances):
    stackables = []
    instances = []
    for base in mail.items:
        item_id = int(base.item_id or 0)
        count = int(base.num or 0)
        cfg = item_config(item_id)
        if (
            count <= 0
            or count > 0x7FFFFFFFFFFFFFFF
            or cfg is None
            or cfg.get("new_bag_item_tag") in _STACKABLE_EXCLUDED_TAGS
        ):
            raise MailRewardError(StatusCode.StatusCode_CFG_NOT_FOUND)
        try:
            item_mgr.validate_stackable_item_id(item_id)
        except item_mgr.EconomyConfigError as error:
            raise MailRewardError(StatusCode.StatusCode_CFG_NOT_FOUND) from error
        stackables.append((item_id, count))

    for source in mail.reward:
        detail = ItemDetail()
        detail.CopyFrom(source)
        kind = detail.main_item.WhichOneof("item")
        item_id = int(detail.main_item.item_id or 0)
        cfg = item_config(item_id)
        if not kind or item_id <= 0 or cfg is None:
            raise MailRewardError(StatusCode.StatusCode_CFG_NOT_FOUND)
        if kind not in _INSTANCE_KINDS:
            if kind == "base_item":
                count = int(detail.main_item.base_item.num or 0)
                if count <= 0 or count > 0x7FFFFFFFFFFFFFFF:
                    raise MailRewardError(StatusCode.StatusCode_CFG_NOT_FOUND)
                if detail.main_item.base_item.item_id != item_id:
                    raise MailRewardError(StatusCode.StatusCode_BAD_REQ)
            elif kind == "character":
                count = 1
                if detail.main_item.character.character_id != item_id:
                    raise MailRewardError(StatusCode.StatusCode_BAD_REQ)
                try:
                    item_mgr.character_config(item_id)
                except item_mgr.EconomyConfigError as error:
                    raise MailRewardError(
                        StatusCode.StatusCode_CFG_NOT_FOUND
                    ) from error
            else:
                raise MailRewardError(StatusCode.StatusCode_CFG_NOT_FOUND)
            if kind != "character":
                if cfg.get("new_bag_item_tag") in _STACKABLE_EXCLUDED_TAGS:
                    raise MailRewardError(StatusCode.StatusCode_CFG_NOT_FOUND)
                try:
                    item_mgr.validate_stackable_item_id(item_id)
                except item_mgr.EconomyConfigError as error:
                    raise MailRewardError(
                        StatusCode.StatusCode_CFG_NOT_FOUND
                    ) from error
            stackables.append((item_id, count))
            continue

        instance = getattr(detail.main_item, kind)
        instance_id = int(getattr(instance, "instance_id", 0) or 0)
        if (
            cfg.get("new_bag_item_tag") != _INSTANCE_TAGS[kind]
            or detail.main_item.item_tag not in (0, _INSTANCE_TAGS[kind])
            or getattr(instance, f"{kind}_id", getattr(instance, "item_id", 0))
            != item_id
        ):
            raise MailRewardError(StatusCode.StatusCode_BAD_REQ)
        try:
            item_mgr.validate_counter(
                instance_id, allow_zero=False, label="mail instance id"
            )
        except item_mgr.EconomyConfigError as error:
            raise MailRewardError(StatusCode.StatusCode_BAD_REQ) from error
        if instance_id in seen_instances or db.get_item_detail(
            player_id, None, instance_id
        ):
            raise MailRewardError(StatusCode.StatusCode_BAD_REQ)
        seen_instances.add(instance_id)
        detail.pack_type = ItemDetail.PackType.PackType_Inventory
        detail.main_item.is_new = False
        detail.main_item.temp_pack_index = 0
        if hasattr(instance, "wearer_id"):
            instance.wearer_id = 0
        instances.append(detail)
    return stackables, instances


def _grant_prepared(player_id, prepared, claim_result, pack_notice):
    for stackables, instances in prepared:
        if stackables:
            item_mgr.grant_items(
                player_id,
                stackables,
                rsp_items=claim_result,
                pack_notice=pack_notice,
                session=None,
            )
        for detail in instances:
            item_mgr.mark_new_item(player_id, detail)
            kind = detail.main_item.WhichOneof("item")
            instance_id = getattr(detail.main_item, kind).instance_id
            db.set_item_detail(player_id, detail.SerializeToString(), None, instance_id)
            claim_result.add().CopyFrom(detail)
            pack_notice.items.add().CopyFrom(detail)


def _claimed_mail(player_id, mail):
    # 老版本可能已经发放但仍保留附件；只清理展示状态，绝不再次发放。
    db.try_claim(player_id, CLAIM_SYSTEM, str(mail.mail_id))
    mail.mail_state = MailState.MailState_CLAIMED
    mail.ClearField("items")
    mail.ClearField("reward")
    db.set_mail(player_id, mail.mail_id, mail.SerializeToString())


def claim_attachments(
    player_id,
    mail_id=0,
    claim_all=False,
    claim_result=None,
    pack_notice=None,
    packet_id=0,
):
    """领取单封或全部附件。返回已领取邮件和本次到账明细。

    单封重复领取通过 already_claimed 返回；CLAIM_ALL 重复调用是成功的空操作。
    packet_id > 0 时按请求指纹重放首次结果；0 保持兼容并由邮件账本去重。
    整个请求要么全部到账并清除附件，要么零变更。
    """
    if claim_result is None or pack_notice is None:
        raise ValueError("claim_result and pack_notice are required")
    if not claim_all and not mail_id:
        raise MailRewardError(StatusCode.StatusCode_BAD_REQ)
    fingerprint = _fingerprint(mail_id, claim_all)
    result = ClaimResult()
    staged_claim = OperateMailsRsp()
    staged_pack = PackNotice(status=pack_notice.status)
    staged_items = []
    with (
        item_mgr.player_lock(player_id),
        item_mgr.deferred_notifications(),
        db.transaction(),
    ):
        saved = (
            _load_operation(player_id, packet_id, fingerprint)
            if packet_id > 0
            else None
        )
        if saved == "conflict":
            raise MailRewardError(StatusCode.StatusCode_BAD_REQ)
        if saved is not None:
            result, staged_items, staged_pack = saved
        else:
            blobs = db.get_mail(player_id)
            selected = []
            for blob in blobs:
                mail = MailBriefData()
                try:
                    mail.ParseFromString(blob)
                except Exception as error:
                    raise MailRewardError(StatusCode.StatusCode_BAD_REQ) from error
                if not mail.mail_id:
                    raise MailRewardError(StatusCode.StatusCode_BAD_REQ)
                if claim_all or mail.mail_id == mail_id:
                    selected.append(mail)
            if not claim_all and not selected:
                raise MailRewardError(StatusCode.StatusCode_BAD_REQ)

            prepared = []
            seen_instances = set()
            for mail in selected:
                already_claimed = mail.mail_state == MailState.MailState_CLAIMED or db.has_claimed(
                    player_id, CLAIM_SYSTEM, str(mail.mail_id)
                )
                if already_claimed:
                    _claimed_mail(player_id, mail)
                    result.mails.append(mail)
                    result.already_claimed = True
                    continue
                if not (len(mail.items) or len(mail.reward)):
                    continue
                prepared.append((mail, _prepare_rewards(player_id, mail, seen_instances)))
                if not prepared[-1][1][0] and not prepared[-1][1][1]:
                    raise MailRewardError(StatusCode.StatusCode_CFG_NOT_FOUND)

            # 所有邮件均通过校验后才写领取账本和物品；后续异常由事务整体回滚。
            for mail, rewards in prepared:
                if not db.try_claim(player_id, CLAIM_SYSTEM, str(mail.mail_id)):
                    _claimed_mail(player_id, mail)
                    result.mails.append(mail)
                    result.already_claimed = True
                    continue
                before = len(staged_claim.claim_result)
                _grant_prepared(
                    player_id, [rewards], staged_claim.claim_result, staged_pack
                )
                for gained in staged_claim.claim_result[before:]:
                    item_id = int(gained.main_item.item_id or 0)
                    kind = gained.main_item.WhichOneof("item")
                    count = (
                        int(gained.main_item.base_item.num or 0)
                        if kind == "base_item"
                        else 1
                    )
                    if count > 0:
                        result.gains.append((item_id, count))
                mail.mail_state = MailState.MailState_CLAIMED
                mail.ClearField("items")
                mail.ClearField("reward")
                db.set_mail(player_id, mail.mail_id, mail.SerializeToString())
                result.mails.append(mail)

            if not claim_all and not result.mails:
                raise MailRewardError(StatusCode.StatusCode_MAIL_NO_REWARD)
            staged_items = list(staged_claim.claim_result)
            _save_operation(
                player_id, packet_id, fingerprint, result, staged_items, staged_pack
            )

    # 只在数据库成功提交后合并回包；失败或指纹冲突不会污染调用方缓冲区。
    for item in staged_items:
        claim_result.add().CopyFrom(item)
    pack_notice.items.extend(staged_pack.items)
    return result


def sanitize_claimed_mails(player_id):
    """列表/重启读取时清理历史已领取邮件残留附件，并补一次性账本。"""
    changed = False
    with item_mgr.player_lock(player_id), db.transaction():
        for blob in db.get_mail(player_id):
            mail = MailBriefData()
            try:
                mail.ParseFromString(blob)
            except Exception:
                continue
            if mail.mail_state != MailState.MailState_CLAIMED:
                continue
            before = mail.SerializeToString()
            _claimed_mail(player_id, mail)
            changed = changed or mail.SerializeToString() != before
    return changed
