import hashlib

from config import Config
from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId
from proto.net_pb2 import (
    ManualFlagRewardReq,
    ManualFlagRewardRsp,
    StatusCode,
    RewardStatus,
    PackNotice,
)

import utils.db as db
from utils import achieve_track, flag_state, item_mgr
from utils.manual_res import iter_flags, flag_reward_list
from server import activities


_OPERATION_SYSTEM = "manual-flag-reward"


def _flag_config(flag_id):
    matches = [(sheet, cfg) for current_id, sheet, cfg in iter_flags() if current_id == flag_id]
    if len(matches) != 1:
        raise item_mgr.EconomyConfigError("missing or duplicate manual flag config")
    return matches[0]


@packet_handler(MsgId.ManualFlagRewardReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = ManualFlagRewardReq()
        req.ParseFromString(data)
        rsp = ManualFlagRewardRsp()
        rsp.flag.flag_id = req.flag_id
        rsp.flag.status = RewardStatus.NotReward
        rsp.flag.dungeon_task_finish_num = 0
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
                    sheet, flag_cfg = _flag_config(req.flag_id)
                    if req.flag_id not in flag_state.get_ids(session.player_id, Config.MANUAL_FLAGS_NS):
                        rsp.status = StatusCode.StatusCode_FAIL
                    else:
                        level_row = db.get_players_info(session.player_id, "level")
                        if not level_row or type(level_row[0]) is not int or level_row[0] <= 0:
                            rsp.status = StatusCode.StatusCode_FAIL
                        else:
                            rewards = flag_reward_list(sheet, flag_cfg, level_row[0])
                            if not rewards:
                                rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                            else:
                                item_mgr.validate_rewards(rewards)
                                if not db.try_claim(session.player_id, "manual_flag", req.flag_id):
                                    rsp.status = StatusCode.StatusCode_MANUAL_REWARDED
                                    rsp.flag.status = RewardStatus.Reward
                                else:
                                    item_mgr.grant_items(
                                        session.player_id,
                                        rewards,
                                        rsp_items=rsp.items,
                                        pack_notice=pack,
                                        session=session,
                                    )
                                    rsp.status = StatusCode.StatusCode_OK
                                    rsp.flag.status = RewardStatus.Reward
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
            rsp.flag.flag_id = req.flag_id
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            pack.Clear()

        session.send(MsgId.ManualFlagRewardRsp, rsp, packet_id)
        if rsp.status == StatusCode.StatusCode_OK and pack.items:
            session.send(MsgId.PackNotice, pack, 0)
