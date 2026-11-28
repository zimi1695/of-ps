import hashlib

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId
from proto.net_pb2 import (
    GetCharacterAchievementAwardReq,
    GetCharacterAchievementAwardRsp,
    StatusCode,
    Character,
    PackNotice,
)

import utils.db as db
from utils import item_mgr
from utils.res_loader import res
from server import activities


_OPERATION_SYSTEM = "character-achievement-award"


def _rows(sheet, table):
    section = res.get(sheet, {}).get(table, {})
    rows = section.get("datas", ()) if isinstance(section, dict) else ()
    return rows if isinstance(rows, (list, tuple)) else ()


def _reward_config(character_id, reward_index):
    characters = [row for row in _rows("Character", "character_achieve_reward") if row.get("i_d") == character_id]
    if len(characters) != 1:
        raise item_mgr.EconomyConfigError("missing or duplicate character achievement config")
    tiers = [
        entry
        for entry in characters[0].get("achieve_reward_info", ()) or ()
        if isinstance(entry, dict) and entry.get("num") == reward_index
    ]
    if len(tiers) != 1 or reward_index <= 0:
        raise item_mgr.EconomyConfigError("missing or duplicate character reward tier")
    cfg = tiers[0]
    unlock_item = cfg.get("unlock_item_i_d")
    unlock_count = cfg.get("unlock_item_count")
    item_mgr.validate_stackable_item_id(unlock_item)
    item_mgr.validate_count(unlock_count, label="character achievement badge requirement")
    price_type = cfg.get("new_reward_price_type", 0)
    if type(price_type) is not int or price_type not in (0, 1):
        raise item_mgr.EconomyConfigError("invalid character reward payment type")
    rewards = ((cfg.get("reward_item_i_d", 0), cfg.get("reward_item_count", 1)),)
    item_mgr.validate_rewards(rewards)
    return cfg, rewards


@packet_handler(MsgId.GetCharacterAchievementAwardReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = GetCharacterAchievementAwardReq()
        req.ParseFromString(data)
        rsp = GetCharacterAchievementAwardRsp(character_id=req.character_id)
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
                    characters = db.get_characters(session.player_id, req.character_id)
                    if not characters:
                        rsp.status = StatusCode.StatusCode_BAD_REQ
                    else:
                        chr = Character()
                        chr.ParseFromString(characters[0])
                        if chr.character_id != req.character_id:
                            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                        else:
                            rsp.rewarded_index_lst.extend(chr.reward_index_lst)
                            cfg, rewards = _reward_config(req.character_id, req.reward_index)
                            if cfg.get("new_reward_price_type", 0) and not chr.is_unlock_payment:
                                rsp.status = StatusCode.StatusCode_FAIL
                            elif item_mgr.get_num(
                                session.player_id, cfg["unlock_item_i_d"]
                            ) < cfg["unlock_item_count"]:
                                rsp.status = StatusCode.StatusCode_FAIL
                            elif req.reward_index in chr.reward_index_lst:
                                rsp.status = StatusCode.StatusCode_REPEAT_GET_REWARD
                            elif not db.try_claim(
                                session.player_id,
                                "chr_achieve",
                                f"{req.character_id}:{req.reward_index}",
                            ):
                                rsp.status = StatusCode.StatusCode_REPEAT_GET_REWARD
                            else:
                                item_mgr.grant_items(
                                    session.player_id,
                                    rewards,
                                    rsp_items=rsp.items,
                                    pack_notice=pack,
                                    session=session,
                                )
                                chr.reward_index_lst.append(req.reward_index)
                                db.set_character(
                                    session.player_id,
                                    req.character_id,
                                    chr.SerializeToString(),
                                )
                                del rsp.rewarded_index_lst[:]
                                rsp.rewarded_index_lst.extend(chr.reward_index_lst)
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
            rsp.character_id = req.character_id
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            pack.Clear()

        session.send(MsgId.GetCharacterAchievementAwardRsp, rsp, packet_id)
        if rsp.status == StatusCode.StatusCode_OK and pack.items:
            session.send(MsgId.PackNotice, pack, 0)
