import hashlib

from config import Config
from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId
from proto.net_pb2 import (
    TreasureIllustrationRewardReq,
    TreasureIllustrationRewardRsp,
    StatusCode,
    PackNotice,
)

import utils.db as db
from utils.res_loader import res
from utils.reward_resolver import resolve_reward
from utils import achieve_track, item_mgr
from server import activities


_OPERATION_SYSTEM = "treasure-illustration-reward"


def _collection_entry(archaeology_type, entry_id):
    table = Config.TREASURE_ILLUSTRATION_REWARD_TYPE_TABLES.get(archaeology_type)
    if not table:
        raise item_mgr.EconomyConfigError("unknown archaeology type")
    section = res.get(table[0], {}).get(table[1], {})
    rows = section.get("datas", ()) if isinstance(section, dict) else ()
    if not isinstance(rows, (list, tuple)):
        raise item_mgr.EconomyConfigError("invalid illustration table")
    matches = [
        row
        for row in rows
        if isinstance(row, dict)
        and (row.get("i_d") == entry_id or row.get("item_i_d") == entry_id)
    ]
    if len(matches) != 1:
        raise item_mgr.EconomyConfigError("missing or duplicate illustration entry")
    entry = matches[0]
    reward_id = entry.get("collection_i_d")
    if type(reward_id) is not int or reward_id <= 0:
        raise item_mgr.EconomyConfigError("invalid illustration reward")
    return entry, reward_id


def get_collection_reward_id(archaeology_type, entry_id):
    """按权威图鉴条目返回 collection_i_d；配置不唯一时拒绝。"""
    try:
        return _collection_entry(archaeology_type, entry_id)[1]
    except item_mgr.EconomyConfigError:
        return 0


def _claim_key(archaeology_type, entry_id):
    return f"{archaeology_type}:{entry_id}"


def get_rewarded_ids(player_id, archaeology_type):
    prefix = f"{archaeology_type}:"
    return [
        int(key[len(prefix):])
        for key in db.get_claimed(player_id, "treasure_illustration")
        if key.startswith(prefix)
    ]


def _has_collection_proof(player_id, entry):
    """最终物品曾获得、对应物品的精确挖掘记录或足量合成碎片均可证明。"""
    item_id = entry.get("item_i_d") or entry.get("i_d")
    item_mgr.validate_item_id(item_id)
    dug = db.get_kv(player_id, "archaeology", str(item_id), 0)
    try:
        dug = int(dug or 0)
    except (TypeError, ValueError) as error:
        raise item_mgr.EconomyConfigError("invalid archaeology progress") from error
    if dug > 0 or db.has_claimed(player_id, "got_ids", str(item_id)):
        return True

    requirements = entry.get("items", ()) or ()
    if not isinstance(requirements, (list, tuple)):
        raise item_mgr.EconomyConfigError("invalid illustration requirements")
    for requirement in requirements:
        if not isinstance(requirement, dict):
            raise item_mgr.EconomyConfigError("invalid illustration requirement")
        fragment_id = requirement.get("furniture_fragment_i_d")
        need = requirement.get("need_item_count")
        item_mgr.validate_stackable_item_id(fragment_id)
        item_mgr.validate_count(need, label="illustration fragment requirement")
        if item_mgr.get_num(player_id, fragment_id) < need:
            return False
    return bool(requirements)


@packet_handler(MsgId.TreasureIllustrationRewardReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = TreasureIllustrationRewardReq()
        req.ParseFromString(data)
        rsp = TreasureIllustrationRewardRsp(
            archaeology_type=req.archaeology_type, id=req.id
        )
        pack = PackNotice(status=StatusCode.StatusCode_OK)
        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()

        try:
            with achieve_track.batch(session), item_mgr.deferred_notifications(), item_mgr.player_lock(session.player_id), db.transaction():
                operation = activities._load_operation(
                    session.player_id, _OPERATION_SYSTEM, 0, packet_id, fingerprint
                )
                if operation == "conflict":
                    rsp.status = StatusCode.StatusCode_BAD_REQ
                elif operation is not None:
                    saved_response, saved_pack = operation
                    rsp.ParseFromString(saved_response)
                    pack.ParseFromString(saved_pack)
                else:
                    entry, reward_id = _collection_entry(req.archaeology_type, req.id)
                    if not _has_collection_proof(session.player_id, entry):
                        rsp.status = StatusCode.StatusCode_FAIL
                    else:
                        level_row = db.get_players_info(session.player_id, "level")
                        if not level_row or type(level_row[0]) is not int or level_row[0] <= 0:
                            rsp.status = StatusCode.StatusCode_FAIL
                        else:
                            rewards = resolve_reward(reward_id, level_row[0])
                            if not rewards:
                                rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                            else:
                                item_mgr.validate_rewards(rewards)
                                if not db.try_claim(
                                    session.player_id,
                                    "treasure_illustration",
                                    _claim_key(req.archaeology_type, req.id),
                                ):
                                    rsp.status = StatusCode.StatusCode_REPEAT_GET_REWARD
                                else:
                                    item_mgr.grant_items(
                                        session.player_id,
                                        rewards,
                                        rsp_items=rsp.rewards,
                                        pack_notice=pack,
                                        session=session,
                                    )
                                    achieve_track.report(
                                        session,
                                        "archaeology_collect",
                                        archaeology_type=req.archaeology_type,
                                    )
                                    rsp.status = StatusCode.StatusCode_OK
                                    activities._save_operation(
                                        session.player_id,
                                        _OPERATION_SYSTEM,
                                        0,
                                        packet_id,
                                        fingerprint,
                                        rsp,
                                        pack,
                                    )
        except item_mgr.EconomyConfigError:
            rsp.Clear()
            rsp.archaeology_type = req.archaeology_type
            rsp.id = req.id
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            pack.Clear()

        session.send(MsgId.TreasureIllustrationRewardRsp, rsp, packet_id)
        if rsp.status == StatusCode.StatusCode_OK and pack.items:
            session.send(MsgId.PackNotice, pack, 0)
