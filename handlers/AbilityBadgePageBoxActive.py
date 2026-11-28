import hashlib

from utils import achieve_track

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId

from proto.net_pb2 import (
    AbilityBadgePageBoxActiveReq,
    AbilityBadgePageBoxActiveRsp,
    PackNotice,
    StatusCode,
)

import utils.db as db
from utils.res_loader import res
from utils import badge_tree, item_mgr
from server import activities

"""
# 能力徽章页格子激活 1633 1634(官方抓包 pack/pack6.0 实证)
包序: Rsp(status+page+box_id+items) → 1400(消耗与获得合并一份)。
page对应stone_area_data.scene_i_d，经unlock_i_d选择stone_unlock_data，
再按box_i_d查节点，消耗其item_i_d x item_count；行列号仅是布局。
节点奖励由badge_tree统一提供，使用通用物品发放同步功能与能力解锁。
"""

_OPERATION_SYSTEM = "ability-badge-node"


def _rows(sheet, table):
    section = res.get(sheet, {}).get(table, {})
    rows = section.get("datas", ()) if isinstance(section, dict) else ()
    if not isinstance(rows, (list, tuple)):
        return ()
    return tuple(row for row in rows if isinstance(row, dict))


def _validated_box(page, box_id):
    areas = [row for row in _rows("MoonStone", "stone_area_data") if row.get("scene_i_d") == page]
    if len(areas) != 1:
        raise item_mgr.EconomyConfigError("missing or duplicate badge page config")
    unlocks = [row for row in _rows("MoonStone", "stone_unlock_data") if row.get("i_d") == areas[0].get("unlock_i_d")]
    if len(unlocks) != 1:
        raise item_mgr.EconomyConfigError("missing or duplicate badge unlock config")
    raw_groups = unlocks[0].get("stone_unlock_data_group_info", ())
    if not isinstance(raw_groups, (list, tuple)):
        raise item_mgr.EconomyConfigError("invalid badge nodes")
    groups = tuple(row for row in raw_groups if isinstance(row, dict))
    if len(groups) != len(raw_groups):
        raise item_mgr.EconomyConfigError("invalid badge node")
    matches = [row for row in groups if row.get("box_i_d") == box_id]
    if len(matches) != 1:
        raise item_mgr.EconomyConfigError("missing or duplicate badge node config")
    box = matches[0]

    cost_item = box.get("item_i_d", 0)
    cost_num = box.get("item_count", 0)
    if type(cost_item) is not int or type(cost_num) is not int:
        raise item_mgr.EconomyConfigError("invalid badge cost")
    if cost_item < 0 or cost_num < 0 or bool(cost_item) != bool(cost_num):
        raise item_mgr.EconomyConfigError("invalid badge cost")
    costs = []
    if cost_item:
        item_mgr.validate_stackable_item_id(cost_item)
        item_mgr.validate_count(cost_num, label="badge cost")
        costs.append((cost_item, cost_num))
    rewards = tuple(badge_tree.rewards(page, box_id) or ())
    item_mgr.validate_rewards(rewards)
    return costs, rewards


@packet_handler(MsgId.AbilityBadgePageBoxActiveReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = AbilityBadgePageBoxActiveReq()
        req.ParseFromString(data)

        rsp = AbilityBadgePageBoxActiveRsp(page=req.page, box_id=req.box_id)

        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()
        notice = PackNotice(status=StatusCode.StatusCode_OK)
        changed = False
        with achieve_track.batch(session):
            with item_mgr.deferred_notifications(), item_mgr.player_lock(session.player_id), db.transaction():
                operation = activities._load_operation(
                    session.player_id, _OPERATION_SYSTEM, 0, packet_id, fingerprint
                )
                if operation == "conflict":
                    rsp.status = StatusCode.StatusCode_BAD_REQ
                elif operation is not None:
                    saved_response, saved_pack = operation
                    rsp.ParseFromString(saved_response)
                    notice.ParseFromString(saved_pack)
                else:
                    try:
                        costs, rewards = _validated_box(req.page, req.box_id)
                    except (KeyError, TypeError, ValueError):
                        rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                    else:
                        stored_ids = db.get_ability_tree(session.player_id, req.page)
                        if not isinstance(stored_ids, (list, tuple)):
                            rsp.status = StatusCode.StatusCode_FAIL
                        else:
                            ids = list(stored_ids)
                            if req.box_id in ids:
                                # 重复激活保持既有幂等回显, 不再消耗。
                                rsp.status = StatusCode.StatusCode_OK
                            elif not item_mgr.consume_items(
                                session.player_id, costs, pack_notice=notice, session=session
                            ):
                                rsp.status = StatusCode.StatusCode_ITEM_NOT_ENOUGH
                            else:
                                ids.append(req.box_id)
                                db.set_ability_tree(session.player_id, req.page, ids)
                                item_mgr.grant_items(
                                    session.player_id,
                                    rewards,
                                    rsp_items=rsp.items,
                                    pack_notice=notice,
                                    session=session,
                                )
                                rsp.status = StatusCode.StatusCode_OK
                                changed = True
                                activities._save_operation(
                                    session.player_id,
                                    _OPERATION_SYSTEM,
                                    0,
                                    packet_id,
                                    fingerprint,
                                    rsp,
                                    notice,
                                )
            if changed and req.page == 1 and req.box_id == 31:
                # 成就44(通过星云树解锁二段跳)推进任务链。
                achieve_track.report(session, "double_jump", match=101000000)

        session.send(MsgId.AbilityBadgePageBoxActiveRsp, rsp, packet_id)
        if rsp.status == StatusCode.StatusCode_OK and notice.items:
            session.send(MsgId.PackNotice, notice, 0)
