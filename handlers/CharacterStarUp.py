import hashlib

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId
from proto.net_pb2 import CharacterStarUpReq, CharacterStarUpRsp, StatusCode, PackNotice

from server import activities, character_growth as growth
from utils import achieve_track, item_mgr
import utils.db as db


_SYSTEM = "character-star-up"
# 满星碎片兑换规则由服务端固定；客户端只提供角色 ID。
_EXCHANGE_ITEM_ID = 101
_EXCHANGE_RATE = 500


def _star_stages(character_id):
    rows = [
        row
        for row in growth.rows("Character", "character_star")
        if row.get("i_d") == character_id
    ]
    if len(rows) != 1:
        raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND)
    infos = rows[0].get("star_info", ())
    if not isinstance(infos, (tuple, list)) or not infos:
        raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND)

    stages = {}
    for info in infos:
        if not isinstance(info, dict):
            raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND)
        star = info.get("star", 0)
        item_id = info.get("item_i_d", 0)
        count = info.get("item_num", 0)
        if (
            type(star) is not int
            or star <= 0
            or star in stages
            or type(count) is not int
        ):
            raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND)
        try:
            item_mgr.validate_stackable_item_id(item_id)
            item_mgr.validate_count(count, label="star-up cost")
        except item_mgr.EconomyConfigError as error:
            raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND) from error
        stages[star] = (item_id, count)
    if set(stages) != set(range(1, len(stages) + 1)):
        raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND)
    return tuple(stages[star] for star in range(1, len(stages) + 1))


# 升星或满星碎片兑换均在同一事务中完成，材料写入失败时不会留下半个升星结果。
@packet_handler(MsgId.CharacterStarUpReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = CharacterStarUpReq()
        req.ParseFromString(data)
        rsp = CharacterStarUpRsp(status=StatusCode.StatusCode_OK)
        notice = PackNotice(status=StatusCode.StatusCode_OK)
        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()
        converted = False
        replay = False
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
                    replay = True
                else:
                    character = growth.load_character(session.player_id, req.char_id)
                    stages = _star_stages(character.character_id)
                    if not 0 <= character.star <= len(stages):
                        raise growth.GrowthError(StatusCode.StatusCode_BAD_REQ)

                    if character.star == len(stages):
                        item_mgr.validate_stackable_item_id(_EXCHANGE_ITEM_ID)
                        item_mgr.validate_count(_EXCHANGE_RATE, label="shard exchange rate")
                        shards = item_mgr.get_num(
                            session.player_id, stages[-1][0]
                        )
                        if shards <= 0:
                            raise growth.GrowthError(StatusCode.StatusCode_FAIL)
                        reward_count = item_mgr.checked_mul(
                            shards, _EXCHANGE_RATE
                        )
                        rewards = [(_EXCHANGE_ITEM_ID, reward_count)]
                        item_mgr.validate_rewards(rewards)
                        growth.consume(
                            session.player_id, [(stages[-1][0], shards)], notice
                        )
                        item_mgr.grant_items(
                            session.player_id, rewards, rsp_items=rsp.items
                        )
                        rsp.char_id = req.char_id
                        rsp.star = len(stages)
                        converted = True
                    else:
                        item_id, count = stages[character.star]
                        growth.consume(session.player_id, [(item_id, count)], notice)
                        character.star += 1
                        growth.save_character(session.player_id, character)
                        rsp.char_id = req.char_id
                        rsp.star = character.star

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
        session.send(MsgId.CharacterStarUpRsp, rsp, packet_id)
        if not converted and not replay and rsp.status == StatusCode.StatusCode_OK:
            achieve_track.report(session, "star_up", match=rsp.star)
