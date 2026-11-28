import hashlib

from config import Config
from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId
from proto.net_pb2 import PlayerLevelRewardReq, PlayerLevelRewardRsp, StatusCode, PackNotice

import utils.db as db
from utils import item_mgr
from utils.res_loader import res
from utils.reward_resolver import resolve_reward
from server import activities


_OPERATION_SYSTEM = "player-level-reward"


def _rows(sheet, table):
    section = res.get(sheet, {}).get(table, {})
    rows = section.get("datas", ()) if isinstance(section, dict) else ()
    return rows if isinstance(rows, (list, tuple)) else ()


def _level_config(level):
    rows = [row for row in _rows("Level", "player_level") if row.get("i_d") == level]
    if len(rows) != 1:
        raise item_mgr.EconomyConfigError("missing or duplicate player level config")
    reward_id = rows[0].get("reward_i_d", 0)
    if type(reward_id) is not int or reward_id < 0:
        raise item_mgr.EconomyConfigError("invalid player level reward")
    return reward_id


@packet_handler(MsgId.PlayerLevelRewardReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = PlayerLevelRewardReq()
        req.ParseFromString(data)
        rsp = PlayerLevelRewardRsp(level=req.level)
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
                    reward_id = _level_config(req.level)
                    level_row = db.get_players_info(session.player_id, "level")
                    if (
                        req.level <= 0
                        or not level_row
                        or type(level_row[0]) is not int
                        or req.level > level_row[0]
                    ):
                        rsp.status = StatusCode.StatusCode_FAIL
                    else:
                        rewards = (
                            []
                            if reward_id == Config.NONE_REWARD
                            else resolve_reward(reward_id, level_row[0])
                        )
                        if reward_id != Config.NONE_REWARD and not rewards:
                            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                        elif reward_id != Config.NONE_REWARD and not db.try_claim(
                            session.player_id, "player_level", req.level
                        ):
                            rsp.status = StatusCode.StatusCode_REPEAT_GET_REWARD
                        else:
                            if rewards:
                                item_mgr.validate_rewards(rewards)
                                item_mgr.grant_items(
                                    session.player_id,
                                    rewards,
                                    rsp_items=rsp.items,
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
            rsp.level = req.level
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            pack.Clear()

        session.send(MsgId.PlayerLevelRewardRsp, rsp, packet_id)
        if rsp.status == StatusCode.StatusCode_OK and pack.items:
            session.send(MsgId.PackNotice, pack, 0)
