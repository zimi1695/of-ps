import hashlib

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId
from proto.net_pb2 import PetDecomposeReq, PetDecomposeRsp, PackNotice, StatusCode

from server import activities, character_growth as growth
from utils import item_mgr
import utils.db as db


_SYSTEM = "pet-decompose"


def _positive_config_int(value):
    if type(value) is not int or value <= 0:
        raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND)
    return value


def _pet_rewards(item_id):
    pet_rows = [
        row for row in growth.rows("Pet", "pet") if row.get("i_d") == item_id
    ]
    if len(pet_rows) != 1:
        raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND)
    reward_ids = pet_rows[0].get("release_reward", ())
    if type(reward_ids) is int:
        reward_ids = (reward_ids,)
    if not isinstance(reward_ids, (tuple, list)) or not reward_ids:
        raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND)

    costs = []
    for reward_id in reward_ids:
        _positive_config_int(reward_id)
        groups = [
            row
            for row in growth.rows("Pet", "release_reward")
            if row.get("i_d") == reward_id
        ]
        if len(groups) != 1:
            raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND)
        entries = groups[0].get("release_reward_item", ())
        if not isinstance(entries, (tuple, list)) or not entries:
            raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND)
        for entry in entries:
            if not isinstance(entry, dict):
                raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND)
            costs.append(
                (
                    _positive_config_int(entry.get("release_reward_i_d", 0)),
                    _positive_config_int(entry.get("release_reward_count", 0)),
                )
            )
    rewards = growth.normalize_costs(costs)
    try:
        for item_id, _count in rewards:
            item_mgr.validate_stackable_item_id(item_id)
        item_mgr.validate_rewards(rewards)
    except item_mgr.EconomyConfigError as error:
        raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND) from error
    return rewards


@packet_handler(MsgId.PetDecomposeReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = PetDecomposeReq()
        req.ParseFromString(data)
        rsp = PetDecomposeRsp(status=StatusCode.StatusCode_OK)
        notice = PackNotice(status=StatusCode.StatusCode_OK)
        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()
        try:
            with growth.transaction(session.player_id), item_mgr.deferred_notifications():
                saved = (
                    activities._load_operation(
                        session.player_id, _SYSTEM, 0, packet_id, fingerprint
                    )
                    if packet_id
                    else None
                )
                if saved == "conflict":
                    rsp.status = StatusCode.StatusCode_BAD_REQ
                elif saved is not None:
                    saved_response, saved_pack = saved
                    rsp.ParseFromString(saved_response)
                    notice.ParseFromString(saved_pack)
                else:
                    if not req.pet_instance_ids or any(
                        not instance_id for instance_id in req.pet_instance_ids
                    ):
                        raise growth.GrowthError(StatusCode.StatusCode_BAD_REQ)
                    ids = list(dict.fromkeys(req.pet_instance_ids))
                    active_rows = db.get_players_info(
                        session.player_id, "pet_instance_id"
                    )
                    active_id = int(active_rows[0] or 0) if active_rows else 0
                    if active_id in ids:
                        raise growth.GrowthError(StatusCode.StatusCode_BAD_REQ)

                    pets = []
                    rewards = []
                    for instance_id in ids:
                        detail = growth.load_owned_instance(
                            session.player_id, instance_id, "pet"
                        )
                        pets.append(detail)
                        rewards.extend(_pet_rewards(detail.main_item.item_id))
                    rewards = growth.normalize_costs(rewards)
                    rsp.pet_instance_ids.extend(ids)
                    for detail in pets:
                        db.del_item_detail(
                            session.player_id,
                            detail.main_item.pet.instance_id,
                        )
                    if rewards:
                        item_mgr.grant_items(
                            session.player_id,
                            rewards,
                            pack_notice=notice,
                            session=None,
                        )
                    activities._save_operation(
                        session.player_id,
                        _SYSTEM,
                        0,
                        packet_id,
                        fingerprint,
                        rsp,
                        notice,
                    )
        except growth.GrowthError as error:
            rsp.Clear()
            rsp.status = error.status
            notice.Clear()
        except item_mgr.EconomyConfigError:
            rsp.Clear()
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            notice.Clear()

        if notice.items:
            session.send(MsgId.PackNotice, notice, 0)
        session.send(MsgId.PetDecomposeRsp, rsp, packet_id)
