import hashlib
import logging
import math
import random

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId
from proto.net_pb2 import (
    SewingReq,
    SewingRsp,
    PackNotice,
    LifeProficiencyNotice,
    LifeBaseInfo,
    LifeType,
    StatusCode,
)

import utils.db as db
import utils.life_achieve as life_achieve
from utils import achieve_track, item_mgr
from utils.res_loader import res
from server import activities

logger = logging.getLogger(__name__)
_SYSTEM = "sewing"


def _life_level(player_id):
    life_blob = db.get_life(player_id, LifeType.LIFE_TYPE_Sewing)
    if life_blob and isinstance(life_blob, (bytes, bytearray)):
        info = LifeBaseInfo()
        info.ParseFromString(life_blob)
        return max(info.level, 1)
    return 1


def _costs(recipe, level_key):
    needs = {}

    def add(item_id, count):
        item_mgr.validate_stackable_item_id(item_id)
        item_mgr.validate_count(count, allow_zero=True)
        if count:
            needs[item_id] = item_mgr.checked_add(needs.get(item_id, 0), count)

    try:
        item_mgr.validate_counter(recipe.get(level_key, 0), label="craft level")
        need_id = recipe.get("need_item_i_d")
        if need_id:
            add(need_id, recipe.get("need_item_count", 0))
        for material in recipe.get("spin_items", ()):
            add(material.get("item_i_d"), material.get("item_count", 0))
        probability = recipe.get("successful_probability", 1)
        if (
            isinstance(probability, bool)
            or not isinstance(probability, (int, float))
            or not math.isfinite(probability)
            or not 0 <= probability <= 1
        ):
            raise item_mgr.EconomyConfigError("invalid sewing probability")
        product_id = recipe.get("get_item_i_d", 0)
        if not product_id:
            raise item_mgr.EconomyConfigError("missing sewing product")
        item_mgr.validate_item_id(product_id)
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        raise item_mgr.EconomyConfigError("invalid sewing recipe") from error
    return needs, probability, product_id


@packet_handler(MsgId.SewingReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = SewingReq()
        req.ParseFromString(data)

        rsp = SewingRsp()
        rsp.status = StatusCode.StatusCode_OK
        result = rsp.life_common_result
        result.life_type = LifeType.LIFE_TYPE_Sewing
        result.is_success = False
        pack = PackNotice(status=StatusCode.StatusCode_OK)
        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()

        replay = False
        life_notice = None
        try:
            with (
                item_mgr.player_lock(session.player_id),
                item_mgr.deferred_notifications(),
                achieve_track.batch(session),
                db.transaction(),
            ):
                saved = activities._load_operation(
                    session.player_id, _SYSTEM, 0, packet_id, fingerprint
                )
                if saved == "conflict":
                    rsp.status = StatusCode.StatusCode_BAD_REQ
                elif saved is not None:
                    response, saved_pack = saved
                    rsp.ParseFromString(response)
                    pack.ParseFromString(saved_pack)
                    replay = True
                else:
                    recipe = next(
                        (
                            row
                            for row in res.get("Spin", {})
                            .get("spin_item", {})
                            .get("datas", ())
                            if row.get("i_d") == req.sewing_id
                        ),
                        None,
                    )
                    if not recipe:
                        rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                    else:
                        needs, probability, product_id = _costs(recipe, "spin_level")
                        if recipe.get("spin_level", 0) > _life_level(session.player_id):
                            rsp.status = StatusCode.StatusCode_LifeLvNotEnough
                        elif not item_mgr.consume_items(
                            session.player_id, list(needs.items()), pack_notice=pack
                        ):
                            rsp.status = (
                                StatusCode.StatusCode_EnergyNotEnough
                                if 11 in needs
                                else StatusCode.StatusCode_ITEM_NOT_ENOUGH
                            )
                        else:
                            with achieve_track.batch(session):
                                if random.random() < probability:
                                    item_mgr.grant_items(
                                        session.player_id,
                                        [(product_id, 1)],
                                        rsp_items=result.result_items,
                                        pack_notice=pack,
                                    )
                                    result.is_success = True
                                    db.incr_kv(
                                        session.player_id,
                                        "life_book",
                                        "5:" + str(product_id),
                                        1,
                                    )
                                    achieve_track.report(
                                        session, "craft_sewing", match=product_id
                                    )
                                life_blob = db.get_life(
                                    session.player_id, LifeType.LIFE_TYPE_Sewing
                                )
                                if life_blob:
                                    info = LifeBaseInfo()
                                    info.ParseFromString(life_blob)
                                    info.proficiency_value += 10
                                    db.set_life(
                                        session.player_id,
                                        LifeType.LIFE_TYPE_Sewing,
                                        info.SerializeToString(),
                                    )
                                    life_notice = LifeProficiencyNotice(
                                        status=StatusCode.StatusCode_OK
                                    )
                                    life_notice.life_proficiency.life_type = (
                                        LifeType.LIFE_TYPE_Sewing
                                    )
                                    life_notice.life_proficiency.proficiency_value = (
                                        info.proficiency_value
                                    )
                                activities._save_operation(
                                    session.player_id,
                                    _SYSTEM,
                                    0,
                                    packet_id,
                                    fingerprint,
                                    rsp,
                                    pack,
                                )
        except item_mgr.EconomyConfigError:
            rsp.Clear()
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            rsp.life_common_result.life_type = LifeType.LIFE_TYPE_Sewing
            pack.Clear()

        if pack.items:
            session.send(MsgId.PackNotice, pack, 0)
        if replay:
            session.send(MsgId.SewingRsp, rsp, packet_id)
            return
        if life_notice is not None:
            session.send(MsgId.LifeProficiencyNotice, life_notice, 0)
            life_achieve.notify(session, LifeType.LIFE_TYPE_Sewing)
        session.send(MsgId.SewingRsp, rsp, packet_id)
