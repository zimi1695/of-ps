import hashlib

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId

from proto.net_pb2 import (
    PlayerAbilityLevelUpReq,
    PlayerAbilityLevelUpRsp,
    StatusCode,
    PackNotice,
)

import utils.db as db
from utils import item_mgr
from utils.res_loader import res
from utils import achieve_track
from server import activities

"""
# 玩家能力升级 1613 1614
消耗配置(已对表验证): Ability.ability{i_d, need_item_i_d, level_info}
level_info: [{level, cost_item_i_d, cost_count, reward_item_i_d}, ...]
升到 level N 扣 cost_item_i_d x cost_count(如 ability 1010 升1级 -> 水晶103 x10),
超出 level_info 最高级返回 ABILITY_LEVEL_LIMIT。
等级持久化: kv ns=ability, k=ability_id, v=level(PlayerAbilityList 读同一处)。
"""

_OPERATION_SYSTEM = "player-ability-level-up"


def _rows(sheet, table):
    section = res.get(sheet, {}).get(table, {})
    rows = section.get("datas", ()) if isinstance(section, dict) else ()
    if not isinstance(rows, (list, tuple)):
        return ()
    return tuple(row for row in rows if isinstance(row, dict))


def _validated_ability(ability_id):
    matches = [row for row in _rows("Ability", "ability") if row.get("i_d") == ability_id]
    if len(matches) != 1:
        raise item_mgr.EconomyConfigError("missing or duplicate ability config")
    ability = matches[0]
    item_mgr.validate_stackable_item_id(ability.get("need_item_i_d", 0))
    raw_level_info = ability.get("level_info", ())
    if not isinstance(raw_level_info, (list, tuple)) or not raw_level_info:
        raise item_mgr.EconomyConfigError("invalid ability level config")
    level_info = tuple(row for row in raw_level_info if isinstance(row, dict))
    if len(level_info) != len(raw_level_info):
        raise item_mgr.EconomyConfigError("invalid ability level entry")

    levels = {}
    for info in level_info:
        level = info.get("level")
        if type(level) is not int or level <= 0 or level in levels:
            raise item_mgr.EconomyConfigError("invalid ability level")
        cost_item = info.get("cost_item_i_d", 0)
        cost_count = info.get("cost_count", 0)
        if type(cost_item) is not int or type(cost_count) is not int:
            raise item_mgr.EconomyConfigError("invalid ability cost")
        if cost_item < 0 or cost_count < 0 or bool(cost_item) != bool(cost_count):
            raise item_mgr.EconomyConfigError("invalid ability cost")
        if cost_item:
            item_mgr.validate_stackable_item_id(cost_item)
            item_mgr.validate_count(cost_count, label="ability cost")
        reward_item = info.get("reward_item_i_d", 0)
        if reward_item:
            item_mgr.validate_stackable_item_id(reward_item)
            item_mgr.validate_rewards(((reward_item, 1),))
        levels[level] = info
    return levels


def _upgrade_plan(levels, current_level):
    target = levels.get(current_level + 1)
    if target is None:
        return None
    costs = []
    cost_item = target.get("cost_item_i_d", 0)
    if cost_item:
        costs.append((cost_item, target["cost_count"]))
    previous = levels.get(current_level)
    if previous and previous.get("reward_item_i_d"):
        # 等级效果互相替换，不能把不同等级的能力道具同时留在背包。
        costs.append((previous["reward_item_i_d"], 1))
    rewards = []
    if target.get("reward_item_i_d"):
        rewards.append((target["reward_item_i_d"], 1))
    item_mgr.validate_rewards(rewards)
    return costs, rewards


@packet_handler(MsgId.PlayerAbilityLevelUpReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = PlayerAbilityLevelUpReq()
        req.ParseFromString(data)

        rsp = PlayerAbilityLevelUpRsp()
        rsp.ability.ability_id = req.ability_id

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
                        levels = _validated_ability(req.ability_id)
                    except (KeyError, TypeError, ValueError):
                        rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                    else:
                        raw_level = db.get_kv(session.player_id, "ability", req.ability_id)
                        if raw_level is None:
                            rsp.status = StatusCode.StatusCode_FAIL
                            rsp.ability.level = 0
                        else:
                            try:
                                current_level = int(raw_level)
                            except (TypeError, ValueError):
                                rsp.status = StatusCode.StatusCode_FAIL
                            else:
                                if current_level < 0:
                                    rsp.status = StatusCode.StatusCode_FAIL
                                else:
                                    rsp.ability.level = current_level
                                    plan = _upgrade_plan(levels, current_level)
                                    if plan is None:
                                        rsp.status = StatusCode.StatusCode_ABILITY_LEVEL_LIMIT
                                    else:
                                        costs, rewards = plan
                                        if not item_mgr.consume_items(
                                            session.player_id,
                                            costs,
                                            pack_notice=notice,
                                            session=session,
                                        ):
                                            rsp.status = StatusCode.StatusCode_ITEM_NOT_ENOUGH
                                        else:
                                            new_level = current_level + 1
                                            db.set_kv(session.player_id, "ability", req.ability_id, new_level)
                                            item_mgr.grant_items(
                                                session.player_id,
                                                rewards,
                                                rsp_items=rsp.items,
                                                pack_notice=notice,
                                                session=session,
                                            )
                                            rsp.status = StatusCode.StatusCode_OK
                                            rsp.ability.level = new_level
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
            if changed:
                # 成就138: 使用水晶提升能力。
                achieve_track.report(session, "ability_level_up")

        session.send(MsgId.PlayerAbilityLevelUpRsp, rsp, packet_id)
        if rsp.status == StatusCode.StatusCode_OK and notice.items:
            session.send(MsgId.PackNotice, notice, 0)
