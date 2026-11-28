import hashlib

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId
from proto.net_pb2 import (
    RestaurantGameQuestRewardReq,
    RestaurantGameQuestRewardRsp,
    QuestStatus,
    StatusCode,
    PackNotice,
)

import utils.db as db
from utils import item_mgr
from utils import refresh
from utils import restaurant_game_store as store
from server import activities

_SYSTEM = "restaurant-quest-reward"


class _Reject(Exception):
    def __init__(self, status):
        self.status = status


def _rewards(cfg):
    rewards = []
    try:
        for item_key, count_key in (
            ("game_coin_i_d", "game_coin"),
            ("game_integral_item", "game_integral"),
        ):
            item_id = cfg.get(item_key, 0)
            count = cfg.get(count_key, 0)
            if bool(item_id) != bool(count):
                raise item_mgr.EconomyConfigError("incomplete restaurant reward")
            if item_id:
                rewards.append((item_id, count))
        validated = item_mgr.validate_rewards(rewards)
        if not validated:
            raise item_mgr.EconomyConfigError("empty restaurant reward")
        is_refresh = cfg.get("is_refresh", False)
        if not isinstance(is_refresh, bool):
            raise item_mgr.EconomyConfigError("invalid restaurant refresh flag")
        quest_type = cfg.get("new_restaurant_game_quest_type", 0)
        if (
            isinstance(quest_type, bool)
            or not isinstance(quest_type, int)
            or quest_type not in (0, 1)
        ):
            raise item_mgr.EconomyConfigError("invalid restaurant quest type")
        if quest_type == 0:
            param = cfg.get("param")
            item_mgr.validate_counter(param, allow_zero=False, label="quest param")
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        raise item_mgr.EconomyConfigError("invalid restaurant quest config") from error
    return validated, is_refresh, quest_type


def _set_quest_status(rsp):
    if rsp.status in (
        StatusCode.StatusCode_OK,
        StatusCode.StatusCode_REPEAT_GET_REWARD,
    ):
        rsp.quest.status = QuestStatus.QuestStatus_Finish
    else:
        rsp.quest.status = QuestStatus.QuestStatus_InProgress


@packet_handler(MsgId.RestaurantGameQuestRewardReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = RestaurantGameQuestRewardReq()
        req.ParseFromString(data)

        rsp = RestaurantGameQuestRewardRsp()
        rsp.status = StatusCode.StatusCode_OK
        rsp.activity_id = req.activity_id
        pack = PackNotice(status=StatusCode.StatusCode_OK)
        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()

        try:
            with (
                item_mgr.player_lock(session.player_id),
                item_mgr.deferred_notifications(),
                db.transaction(),
            ):
                saved = activities._load_operation(
                    session.player_id, _SYSTEM, 0, packet_id, fingerprint
                )
                if saved == "conflict":
                    rsp.status = StatusCode.StatusCode_BAD_REQ
                    rsp.quest.quest_id = req.quest_id
                    _set_quest_status(rsp)
                elif saved is not None:
                    response, saved_pack = saved
                    rsp.ParseFromString(response)
                    pack.ParseFromString(saved_pack)
                else:
                    cfg = store.get_quests(req.activity_id).get(req.quest_id)
                    if not cfg or not store.get_desk_bags(req.activity_id):
                        rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                        rsp.quest.quest_id = req.quest_id
                        _set_quest_status(rsp)
                    else:
                        try:
                            rewards, is_refresh, quest_type = _rewards(cfg)
                            state = store.get_state(session.player_id, req.activity_id)
                            day = str(refresh.day_index())
                            claim_key = f"{req.quest_id}:{day}"
                            refresh_claimed = state.get("refresh_claimed", {})
                            if not isinstance(refresh_claimed, dict):
                                raise item_mgr.EconomyConfigError(
                                    "invalid restaurant claim state"
                                )
                            if is_refresh:
                                state["rewarded_quests"].discard(req.quest_id)
                            if req.quest_id in state["rewarded_quests"]:
                                raise _Reject(StatusCode.StatusCode_REPEAT_GET_REWARD)
                            if quest_type == 0 and cfg.get("param") not in state["completed"]:
                                raise _Reject(StatusCode.StatusCode_FAIL)
                            if is_refresh and claim_key in refresh_claimed:
                                raise _Reject(StatusCode.StatusCode_REPEAT_GET_REWARD)

                            if is_refresh:
                                state["refresh_claimed"] = refresh_claimed
                                refresh_claimed[claim_key] = day
                                for key in [
                                    key
                                    for key, value in refresh_claimed.items()
                                    if value != day
                                ]:
                                    del refresh_claimed[key]
                            else:
                                state["rewarded_quests"].add(req.quest_id)
                            item_mgr.grant_items(
                                session.player_id,
                                rewards,
                                rsp_items=rsp.item_details,
                                pack_notice=pack,
                            )
                            store.save_state(session.player_id, req.activity_id, state)
                            rsp.quest.quest_id = req.quest_id
                            _set_quest_status(rsp)
                            activities._save_operation(
                                session.player_id,
                                _SYSTEM,
                                0,
                                packet_id,
                                fingerprint,
                                rsp,
                                pack,
                            )
                        except (KeyError, TypeError, ValueError, OverflowError) as error:
                            raise item_mgr.EconomyConfigError(
                                "invalid restaurant state or config"
                            ) from error
        except _Reject as error:
            rsp.Clear()
            rsp.status = error.status
            rsp.activity_id = req.activity_id
            rsp.quest.quest_id = req.quest_id
            _set_quest_status(rsp)
            pack.Clear()
        except item_mgr.EconomyConfigError:
            rsp.Clear()
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            rsp.activity_id = req.activity_id
            rsp.quest.quest_id = req.quest_id
            _set_quest_status(rsp)
            pack.Clear()

        session.send(MsgId.RestaurantGameQuestRewardRsp, rsp, packet_id)
        if pack.items:
            session.send(MsgId.PackNotice, pack, 0)
