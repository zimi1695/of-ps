import hashlib
import logging

from config import Config
from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId
from proto.net_pb2 import (
    EBagItemTag,
    GenericGameAReq,
    GenericGameARsp,
    ItemDetail,
    PackNotice,
    StatusCode,
)

import utils.db as db
from server import activities
from utils import item_mgr
from utils.dungeon_reward import sync_multiplier
from utils.res_loader import res

logger = logging.getLogger(__name__)

_OPERATION_SYSTEM = "generic-game-a-dye-unlock"


def _unlock_price():
    """染色方案价格只来自 GlobalConfig，缺失或非法时拒绝扣费。"""
    section = res.get("GlobalConfig", {})
    section = section.get("config", {}) if isinstance(section, dict) else {}
    rows = section.get("datas", ()) if isinstance(section, dict) else ()
    matches = [
        row
        for row in rows
        if isinstance(row, dict) and row.get("key") == "Fashion_ColoringPlan_Price"
    ] if isinstance(rows, (list, tuple)) else []
    if len(matches) != 1:
        raise item_mgr.EconomyConfigError("missing or duplicate dye unlock price")
    values = matches[0].get("values", ())
    if not isinstance(values, (list, tuple)) or len(values) != 1:
        raise item_mgr.EconomyConfigError("invalid dye unlock price")
    parts = str(values[0]).split(":")
    if len(parts) != 2 or not all(part.isdigit() for part in parts):
        raise item_mgr.EconomyConfigError("invalid dye unlock price")
    item_id, count = (int(part) for part in parts)
    item_mgr.validate_stackable_item_id(item_id)
    item_mgr.validate_count(count, label="dye unlock price")
    return item_id, count


def _owned_outfit(player_id, outfit_id):
    """客户端 ID 只能选择本人已拥有的配置时装，不能构造新时装。"""
    cfg = item_mgr.item_config(outfit_id)
    if cfg.get("new_bag_item_tag") != EBagItemTag.EBagItemTag_Fashion:
        raise item_mgr.EconomyConfigError("dye unlock target is not an outfit")
    blob = db.get_item_detail(player_id, outfit_id)
    if not isinstance(blob, (bytes, bytearray)):
        raise LookupError("outfit is not owned")
    outfit = ItemDetail()
    outfit.ParseFromString(blob)
    if (
        outfit.main_item.WhichOneof("item") != "outfit"
        or outfit.main_item.item_id != outfit_id
        or outfit.main_item.outfit.outfit_id != outfit_id
    ):
        raise ValueError("invalid stored outfit")
    return outfit


@packet_handler(MsgId.GenericGameAReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = GenericGameAReq()
        req.ParseFromString(data)

        rsp = GenericGameARsp()
        rsp.status = StatusCode.StatusCode_OK
        rsp.generic_msg_id = req.generic_msg_id
        rsp.params.extend(req.params)
        for _k, _v in req.param_map.items():
            rsp.param_map[_k] = _v

        match req.generic_msg_id:
            case 2:  # DUNGEON_MULTI，params=[副本ID, 倍率]
                if not sync_multiplier(session.player_id, req, rsp):
                    rsp.status = StatusCode.StatusCode_BAD_REQ
            case 5:  # 解锁染色方案：配置、扣费、解锁和幂等结果同事务提交
                notice = PackNotice(status=StatusCode.StatusCode_OK)
                fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()
                with item_mgr.deferred_notifications(), item_mgr.player_lock(
                    session.player_id
                ), db.transaction():
                    operation = None
                    if packet_id:
                        operation = activities._load_operation(
                            session.player_id,
                            _OPERATION_SYSTEM,
                            0,
                            packet_id,
                            fingerprint,
                        )
                    if operation == "conflict":
                        rsp.status = StatusCode.StatusCode_BAD_REQ
                    elif operation is not None:
                        saved_response, saved_pack = operation
                        rsp.ParseFromString(saved_response)
                        notice.ParseFromString(saved_pack)
                    elif not req.params:
                        rsp.status = StatusCode.StatusCode_BAD_REQ
                    else:
                        outfit_id = req.params[0].int_value
                        try:
                            price = _unlock_price()
                            if (
                                not isinstance(Config.MAX_DYE_SCHEME_NUM, int)
                                or Config.MAX_DYE_SCHEME_NUM <= 0
                            ):
                                raise item_mgr.EconomyConfigError(
                                    "invalid dye scheme limit"
                                )
                            outfit = _owned_outfit(session.player_id, outfit_id)
                        except item_mgr.EconomyConfigError:
                            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                        except LookupError:
                            rsp.status = StatusCode.StatusCode_BAD_REQ
                        except (TypeError, ValueError):
                            rsp.status = StatusCode.StatusCode_FAIL
                        else:
                            schemes = outfit.main_item.outfit.dye_schemes
                            if len(schemes) >= Config.MAX_DYE_SCHEME_NUM:
                                # 已全部解锁时保持幂等成功，不再扣费或扩表。
                                rsp.status = StatusCode.StatusCode_OK
                                activities._save_operation(
                                    session.player_id,
                                    _OPERATION_SYSTEM,
                                    0,
                                    packet_id,
                                    fingerprint,
                                    rsp,
                                    notice,
                                )
                            elif not item_mgr.consume_items(
                                session.player_id, [price], pack_notice=notice
                            ):
                                rsp.status = StatusCode.StatusCode_ITEM_NOT_ENOUGH
                            else:
                                scheme = schemes.add()
                                scheme.scheme_index = len(schemes) - 1
                                scheme.is_un_lock = True
                                db.set_item_detail(
                                    session.player_id,
                                    outfit.SerializeToString(),
                                    outfit_id,
                                )
                                notice.items.add().CopyFrom(outfit)
                                rsp.status = StatusCode.StatusCode_OK
                                activities._save_operation(
                                    session.player_id,
                                    _OPERATION_SYSTEM,
                                    0,
                                    packet_id,
                                    fingerprint,
                                    rsp,
                                    notice,
                                )
                if notice.items:
                    session.send(MsgId.PackNotice, notice, 0)
            case 7:  # 退出染色
                session.color_data[0] = None
            case 9:  # 登录时是否进入热门频道(0=进入,1=不进入), 存入玩家偏好
                if req.params:
                    db.set_kv(
                        session.player_id,
                        "preferences",
                        "hot_channel_skip",
                        int(req.params[0].int_value),
                    )
            case _:
                logger.info("GenericGameA 未处理分支: %s", req.generic_msg_id)
        session.send(MsgId.GenericGameARsp, rsp, packet_id)
