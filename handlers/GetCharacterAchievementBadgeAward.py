import hashlib

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId
from proto.net_pb2 import (
    GetCharacterAchievementBadgeAwardReq,
    GetCharacterAchievementBadgeAwardRsp,
    StatusCode,
    Achieve,
    Character,
    PackNotice,
)

import utils.db as db
from utils import achieve_track, item_mgr
from server import activities


_OPERATION_SYSTEM = "character-achievement-badge-award"


def _badge_config(player_id, character_id, achievement_id):
    entries = [
        entry
        for entry in db.get_character_achievement_lst(player_id, character_id) or ()
        if isinstance(entry, dict) and entry.get("achieve_i_d") == achievement_id
    ]
    if len(entries) != 1:
        raise item_mgr.EconomyConfigError("missing or duplicate character badge config")
    cfg = entries[0]
    rewards = ((cfg.get("item_i_d", 0), cfg.get("item_count", 1)),)
    item_mgr.validate_rewards(rewards)
    return cfg, rewards


@packet_handler(MsgId.GetCharacterAchievementBadgeAwardReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = GetCharacterAchievementBadgeAwardReq()
        req.ParseFromString(data)
        rsp = GetCharacterAchievementBadgeAwardRsp()
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
                            rsp.rewarded_achievement_id_lst.extend(chr.rewarded_achievement_id_lst)
                            _, rewards = _badge_config(
                                session.player_id, req.character_id, req.achievement_id
                            )
                            target = achieve_track.target_of(req.achievement_id)
                            achieve_b = db.get_achieve(session.player_id, req.achievement_id)
                            if type(target) is not int or target <= 0 or not achieve_b:
                                rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                            else:
                                achieve = Achieve()
                                achieve.ParseFromString(achieve_b)
                                if achieve.achieve_id != req.achievement_id or achieve.count < target:
                                    rsp.status = StatusCode.StatusCode_FAIL
                                elif req.achievement_id in chr.rewarded_achievement_id_lst:
                                    rsp.status = StatusCode.StatusCode_REPEAT_GET_REWARD
                                elif not db.try_claim(
                                    session.player_id,
                                    "chr_badge",
                                    f"{req.character_id}:{req.achievement_id}",
                                ):
                                    rsp.status = StatusCode.StatusCode_REPEAT_GET_REWARD
                                else:
                                    item_mgr.grant_items(
                                        session.player_id,
                                        rewards,
                                        rsp_items=rsp.update_items,
                                        pack_notice=pack,
                                        session=session,
                                    )
                                    chr.rewarded_achievement_id_lst.append(req.achievement_id)
                                    db.set_character(
                                        session.player_id,
                                        req.character_id,
                                        chr.SerializeToString(),
                                    )
                                    del rsp.rewarded_achievement_id_lst[:]
                                    rsp.rewarded_achievement_id_lst.extend(chr.rewarded_achievement_id_lst)
                                    rsp.achieves.add().ParseFromString(achieve_b)
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

        session.send(MsgId.GetCharacterAchievementBadgeAwardRsp, rsp, packet_id)
        if rsp.status == StatusCode.StatusCode_OK and pack.items:
            session.send(MsgId.PackNotice, pack, 0)
