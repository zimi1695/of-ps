import hashlib

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId
from proto.net_pb2 import GetAchieveGroupAwardReq, GetAchieveGroupAwardRsp, StatusCode, PackNotice

import utils.db as db
from utils import achieve_track, item_mgr
from utils.res_loader import res
from utils.reward_resolver import resolve_reward
from server import activities


_OPERATION_SYSTEM = "achieve-group-award"


def _rows(sheet, table):
    section = res.get(sheet, {}).get(table, {})
    rows = section.get("datas", ()) if isinstance(section, dict) else ()
    return rows if isinstance(rows, (list, tuple)) else ()


def _group_config(group_id):
    rewards = [row for row in _rows("AchieveQuest", "achieve_quest") if row.get("achieve_group_i_d") == group_id]
    groups = [row for row in _rows("AchieveQuest", "achieve_quest_group") if row.get("i_d") == group_id]
    if len(rewards) != 1 or len(groups) != 1:
        raise item_mgr.EconomyConfigError("missing or duplicate achievement group config")
    reward_id = rewards[0].get("reward_i_d")
    if type(reward_id) is not int or reward_id <= 0:
        raise item_mgr.EconomyConfigError("invalid achievement group reward")
    achieve_ids = [
        entry.get("achieve_condition_i_d")
        for entry in groups[0].get("achieve_quest_group_info", ()) or ()
        if isinstance(entry, dict) and entry.get("achieve_condition_i_d")
    ]
    if not achieve_ids or len(set(achieve_ids)) != len(achieve_ids):
        raise item_mgr.EconomyConfigError("invalid achievement group conditions")
    targets = []
    for achieve_id in achieve_ids:
        target = achieve_track.target_of(achieve_id)
        if type(target) is not int or target <= 0:
            raise item_mgr.EconomyConfigError("missing achievement target")
        targets.append((achieve_id, target))
    return reward_id, tuple(targets)


@packet_handler(MsgId.GetAchieveGroupAwardReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = GetAchieveGroupAwardReq()
        req.ParseFromString(data)
        rsp = GetAchieveGroupAwardRsp()
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
                    reward_id, conditions = _group_config(req.group_id)
                    achieve_track.sync_scoped_progress(
                        session, [achieve_id for achieve_id, _ in conditions]
                    )
                    if any(
                        achieve_track.progress(session.player_id, achieve_id) < target
                        for achieve_id, target in conditions
                    ):
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
                                if not db.try_claim(session.player_id, "achieve_group", req.group_id):
                                    rsp.status = StatusCode.StatusCode_REPEAT_GET_REWARD
                                else:
                                    item_mgr.grant_items(
                                        session.player_id,
                                        rewards,
                                        rsp_items=rsp.rewards,
                                        pack_notice=pack,
                                        session=session,
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
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            pack.Clear()

        session.send(MsgId.GetAchieveGroupAwardRsp, rsp, packet_id)
        if rsp.status == StatusCode.StatusCode_OK and pack.items:
            session.send(MsgId.PackNotice, pack, 0)
