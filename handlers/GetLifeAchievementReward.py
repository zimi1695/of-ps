import hashlib

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId
from proto.net_pb2 import (
    GetLifeAchievementRewardReq,
    GetLifeAchievementRewardRsp,
    StatusCode,
    PackNotice,
)

import utils.db as db
from utils import achieve_track, item_mgr
from utils.reward_resolver import resolve_reward
from server import activities


_OPERATION_SYSTEM = "life-achievement-reward"


@packet_handler(MsgId.GetLifeAchievementRewardReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = GetLifeAchievementRewardReq()
        req.ParseFromString(data)
        rsp = GetLifeAchievementRewardRsp()
        pack = PackNotice(status=StatusCode.StatusCode_OK)
        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()

        try:
            with item_mgr.deferred_notifications(), item_mgr.player_lock(session.player_id), db.transaction():
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
                    entry = achieve_track.life_reward_index().get(req.achieve_id)
                    target = achieve_track.target_of(req.achieve_id)
                    if not entry or type(target) is not int or target <= 0:
                        rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                    elif achieve_track.progress(session.player_id, req.achieve_id) < target:
                        rsp.status = StatusCode.StatusCode_FAIL
                    else:
                        level_row = db.get_players_info(session.player_id, "level")
                        if not level_row or type(level_row[0]) is not int or level_row[0] <= 0:
                            rsp.status = StatusCode.StatusCode_FAIL
                        else:
                            rewards = resolve_reward(entry[0], level_row[0])
                            if not rewards:
                                rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                            else:
                                item_mgr.validate_rewards(rewards)
                                if not db.try_claim(session.player_id, "life_achieve", str(req.achieve_id)):
                                    rsp.status = StatusCode.StatusCode_REPEAT_GET_REWARD
                                else:
                                    item_mgr.grant_items(
                                        session.player_id,
                                        rewards,
                                        session=session,
                                        rsp_items=rsp.items,
                                        pack_notice=pack,
                                    )
                                    rsp.status = StatusCode.StatusCode_OK
                                    rsp.rewarded_achieve_ids.append(req.achieve_id)
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

        session.send(MsgId.GetLifeAchievementRewardRsp, rsp, packet_id)
        if rsp.status == StatusCode.StatusCode_OK and pack.items:
            session.send(MsgId.PackNotice, pack, 0)
