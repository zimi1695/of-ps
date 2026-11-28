import hashlib

from google.protobuf.message import DecodeError

from config import Config

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId

from proto.net_pb2 import ItemSweepReq, ItemSweepRsp, StatusCode, PackNotice

import utils.db as db
from utils.res_loader import res
from utils import item_mgr
from server import activities

_SYSTEM = "item-sweep"


def _quick_cfg(item_id, player_level):
    """QuickObtaining.json quick_obtaining: 按物品 id 查速刷条目,
    quick_obtaining_item 按 player_level 分档, 取 <= 玩家等级的最高档。
    返回 {"num": 单次获得数量, "need_vitality": 单次体力消耗} 或 None。"""
    section = res.get("QuickObtaining", {})
    section = section.get("quick_obtaining", {}) if isinstance(section, dict) else {}
    datas = section.get("datas", ()) if isinstance(section, dict) else ()
    if not isinstance(datas, (list, tuple)):
        return None
    for entry in datas:
        if not isinstance(entry, dict) or entry.get("i_d") != item_id:
            continue
        groups = entry.get("quick_obtaining_item", ())
        if not isinstance(groups, (list, tuple)):
            raise item_mgr.EconomyConfigError("invalid quick obtaining tiers")
        picked = None
        for group in groups:
            if not isinstance(group, dict):
                raise item_mgr.EconomyConfigError("invalid quick obtaining tier")
            level = item_mgr.validate_counter(
                group.get("player_level", 1),
                allow_zero=True,
                label="quick obtaining player level",
            )
            if level <= player_level and (
                picked is None or level > picked.get("player_level", 1)
            ):
                picked = group
        if picked is None and groups:
            picked = groups[0]  # 低于最低档时按最低档
        return picked
    return None


@packet_handler(MsgId.ItemSweepReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = ItemSweepReq()
        try:
            req.ParseFromString(data)
        except DecodeError:
            rsp = ItemSweepRsp(status=StatusCode.StatusCode_BAD_REQ)
            session.send(MsgId.ItemSweepRsp, rsp, packet_id)
            return

        rsp = ItemSweepRsp()
        notice = PackNotice(status=StatusCode.StatusCode_OK)
        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()
        try:
            with (
                item_mgr.player_lock(session.player_id),
                item_mgr.deferred_notifications(),
                db.transaction(),
            ):
                saved = (
                    activities._load_operation(
                        session.player_id, _SYSTEM, 0, packet_id, fingerprint
                    )
                    if packet_id > 0
                    else None
                )
                if saved == "conflict":
                    rsp.status = StatusCode.StatusCode_BAD_REQ
                elif saved is not None:
                    response, saved_notice = saved
                    rsp.ParseFromString(response)
                    notice.ParseFromString(saved_notice)
                elif not req.item_id or req.count <= 0:
                    rsp.status = StatusCode.StatusCode_BAD_REQ
                else:
                    count = item_mgr.validate_count(req.count, label="sweep count")
                    player_info = db.get_players_info(session.player_id, "level")
                    if not player_info:
                        raise item_mgr.EconomyConfigError("missing player")
                    player_level = item_mgr.validate_counter(
                        player_info[0], allow_zero=False, label="player level"
                    )
                    cfg = _quick_cfg(req.item_id, player_level)
                    if not cfg:
                        raise item_mgr.EconomyConfigError(
                            "missing quick obtaining config"
                        )
                    per_round = item_mgr.validate_count(
                        cfg.get("num", 0), label="sweep yield"
                    )
                    if count % per_round:
                        rsp.status = StatusCode.StatusCode_BAD_REQ
                    else:
                        rounds = count // per_round
                        gain = item_mgr.checked_mul(per_round, rounds)
                        vitality = item_mgr.validate_count(
                            cfg.get("need_vitality", 0),
                            allow_zero=True,
                            label="sweep vitality",
                        )
                        cost = (
                            item_mgr.checked_mul(vitality, rounds) if vitality else 0
                        )
                        gain_exp = (
                            item_mgr.checked_mul(cost, Config.EXP_PER_VITALITY)
                            if cost
                            else 0
                        )
                        item_mgr.validate_stackable_item_id(req.item_id)
                        item_mgr.validate_stackable_item_id(Config.VITALITY_ITEM_ID)
                        reward_list = [(req.item_id, gain)]
                        if gain_exp > 0:
                            reward_list.append((1, gain_exp))
                        item_mgr.validate_rewards(reward_list)

                        if not req.is_sweep:
                            tmp = rsp.rewards.add()
                            tmp.main_item.item_id = req.item_id
                            tmp.main_item.base_item.item_id = req.item_id
                            tmp.main_item.base_item.num = gain
                            rsp.status = StatusCode.StatusCode_OK
                        elif cost and not item_mgr.consume_items(
                            session.player_id,
                            [(Config.VITALITY_ITEM_ID, cost)],
                            pack_notice=notice,
                            session=session,
                        ):
                            rsp.status = StatusCode.StatusCode_ITEM_NOT_ENOUGH
                        else:
                            item_mgr.grant_items(
                                session.player_id,
                                reward_list,
                                rsp_items=rsp.rewards,
                                pack_notice=notice,
                                session=session,
                            )
                            rsp.status = StatusCode.StatusCode_OK
                        if rsp.status == StatusCode.StatusCode_OK:
                            activities._save_operation(
                                session.player_id,
                                _SYSTEM,
                                0,
                                packet_id,
                                fingerprint,
                                rsp,
                                notice,
                            )
        except item_mgr.EconomyConfigError:
            rsp.Clear()
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            notice.Clear()

        session.send(MsgId.ItemSweepRsp, rsp, packet_id)
        if rsp.status == StatusCode.StatusCode_OK and notice.items:
            session.send(MsgId.PackNotice, notice, 0)
