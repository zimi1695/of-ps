import hashlib
import math
import random

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId
from proto.net_pb2 import (
    MakePropReq,
    MakePropRsp,
    PackNotice,
    LifeProficiencyNotice,
    StatusCode,
    ItemDetail,
    LifeBaseInfo,
    LifeType,
)

import utils.db as db
import utils.life_achieve as life_achieve
from utils.res_loader import res
from utils import achieve_track, item_mgr
from server import activities

_SYSTEM = "make-prop"


def _life_level(player_id):
    life_blob = db.get_life(player_id, LifeType.LIFE_TYPE_Making)
    if life_blob:
        info = LifeBaseInfo()
        info.ParseFromString(life_blob)
        return max(info.level, 1)
    return 1


def _has_formula(player_id, formula_id):
    item_mgr.validate_item_id(formula_id)
    raw = db.get_item_detail(player_id, formula_id)
    if not raw:
        return False
    detail = ItemDetail()
    detail.ParseFromString(raw)
    if detail.main_item.WhichOneof("item") == "base_item":
        item_mgr.validate_count(detail.main_item.base_item.num, allow_zero=True)
        return detail.main_item.base_item.num >= 1
    return bool(detail.main_item.item_id)


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
        for material in recipe.get("make_items", ()):
            add(material.get("item_i_d"), material.get("item_count", 0))
        probability = recipe.get("successful_probability", 1)
        if (
            isinstance(probability, bool)
            or not isinstance(probability, (int, float))
            or not math.isfinite(probability)
            or not 0 <= probability <= 1
        ):
            raise item_mgr.EconomyConfigError("invalid craft probability")
        product_id = recipe.get("get_item_i_d", 0)
        if not product_id:
            raise item_mgr.EconomyConfigError("missing craft product")
        item_mgr.validate_item_id(product_id)
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        raise item_mgr.EconomyConfigError("invalid craft recipe") from error
    return needs, probability, product_id


@packet_handler(MsgId.MakePropReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = MakePropReq()
        req.ParseFromString(data)

        rsp = MakePropRsp()
        rsp.status = StatusCode.StatusCode_OK
        result = rsp.life_common_result
        result.life_type = LifeType.LIFE_TYPE_Making
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
                            for row in res.get("Make", {})
                            .get("make_item", {})
                            .get("datas", ())
                            if row.get("i_d") == req.make_id
                        ),
                        None,
                    )
                    if not recipe:
                        rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                    else:
                        needs, probability, product_id = _costs(recipe, "make_level")
                        formula_id = recipe.get("need_formula_item_i_d")
                        if recipe.get("make_level", 0) > _life_level(session.player_id):
                            rsp.status = StatusCode.StatusCode_LifeLvNotEnough
                        elif formula_id and not _has_formula(session.player_id, formula_id):
                            rsp.status = StatusCode.StatusCode_ITEM_NOT_ENOUGH
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
                                        "3:" + str(product_id),
                                        1,
                                    )
                                    achieve_track.report(
                                        session, "craft_handmade", match=product_id
                                    )
                                life_blob = db.get_life(
                                    session.player_id, LifeType.LIFE_TYPE_Making
                                )
                                if life_blob:
                                    info = LifeBaseInfo()
                                    info.ParseFromString(life_blob)
                                    info.proficiency_value += 10
                                    db.set_life(
                                        session.player_id,
                                        LifeType.LIFE_TYPE_Making,
                                        info.SerializeToString(),
                                    )
                                    life_notice = LifeProficiencyNotice(
                                        status=StatusCode.StatusCode_OK
                                    )
                                    life_notice.life_proficiency.life_type = (
                                        LifeType.LIFE_TYPE_Making
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
            rsp.life_common_result.life_type = LifeType.LIFE_TYPE_Making
            pack.Clear()

        if pack.items:
            session.send(MsgId.PackNotice, pack, 0)
        if replay:
            session.send(MsgId.MakePropRsp, rsp, packet_id)
            return
        if life_notice is not None:
            session.send(MsgId.LifeProficiencyNotice, life_notice, 0)
            life_achieve.notify(session, LifeType.LIFE_TYPE_Making)
        session.send(MsgId.MakePropRsp, rsp, packet_id)
