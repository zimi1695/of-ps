import hashlib

from google.protobuf.message import DecodeError
from utils import item_mgr

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId

from proto.net_pb2 import (
    TakeOutFurnitureReq,
    TakeOutFurnitureRsp,
    FurnitureDetailsInfo,
    PackNotice,
    SceneGardenFurnitureRemoveNotice,
    StatusCode,
)
import utils.db as db
from server import activities, scene_data
from utils import garden_state
from utils.res_loader import res

"""
# 收回家具 1697 1698
# is_put_in_package=True 时家具道具归还背包(否则仅移除摆放)
# 广播 SceneGardenFurnitureRemoveNotice 给同场景频道玩家
"""

_OPERATION_SYSTEM = "take-out-furniture"


def _rows(sheet, table):
    section = res.get(sheet, {}).get(table, {})
    rows = section.get("datas", ()) if isinstance(section, dict) else ()
    if not isinstance(rows, (list, tuple)):
        return ()
    return tuple(row for row in rows if isinstance(row, dict))


def _validate_furniture_item(item_id, to_inventory):
    item_mgr.validate_item_id(item_id)
    home_items = [
        row
        for row in _rows("HomeItem", "home_item")
        if row.get("i_d") == item_id or row.get("item_i_d") == item_id
    ]
    if not home_items:
        raise item_mgr.EconomyConfigError("missing furniture config")
    if to_inventory:
        item_mgr.validate_rewards(((item_id, 1),))


@packet_handler(MsgId.TakeOutFurnitureReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = TakeOutFurnitureReq()
        req.ParseFromString(data)

        rsp = TakeOutFurnitureRsp(furniture_id=req.furniture_id)
        if req.furniture_id <= 0:
            rsp.status = StatusCode.StatusCode_BAD_REQ
            session.send(MsgId.TakeOutFurnitureRsp, rsp, packet_id)
            return
        if session.scene_id != 9999 or session.channel_id != session.player_id:
            rsp.status = StatusCode.StatusCode_BAD_REQ
            session.send(MsgId.TakeOutFurnitureRsp, rsp, packet_id)
            return

        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()
        notice = PackNotice(status=StatusCode.StatusCode_OK)
        replayed = False
        details = None
        with item_mgr.deferred_notifications(), item_mgr.player_lock(session.player_id), db.transaction():
            operation = activities._load_operation(
                session.player_id, _OPERATION_SYSTEM, 0, packet_id, fingerprint
            )
            if operation == "conflict":
                rsp.status = StatusCode.StatusCode_BAD_REQ
            elif operation is not None:
                saved_response, saved_pack = operation
                rsp.ParseFromString(saved_response)
                notice.ParseFromString(saved_pack)
                replayed = True
            else:
                row = db.get_furniture(
                    session.scene_id, session.channel_id, session.player_id, req.furniture_id
                )
                if not row:
                    rsp.status = StatusCode.StatusCode_FURNITURE_NOT_EXIST
                else:
                    details = FurnitureDetailsInfo()
                    try:
                        details.ParseFromString(row[-1])
                    except (DecodeError, TypeError, ValueError):
                        rsp.status = StatusCode.StatusCode_FAIL
                    else:
                        if details.furniture_id != req.furniture_id or not details.furniture_item_id:
                            rsp.status = StatusCode.StatusCode_FAIL
                        else:
                            try:
                                _validate_furniture_item(details.furniture_item_id, req.is_put_in_package)
                            except (KeyError, TypeError, ValueError):
                                rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                            else:
                                db.del_furniture(
                                    session.scene_id,
                                    session.channel_id,
                                    session.player_id,
                                    req.furniture_id,
                                )
                                garden_state.adjust_furniture_count(session.player_id, -1)
                                if req.is_put_in_package:
                                    item_mgr.grant_items(
                                        session.player_id,
                                        [(details.furniture_item_id, 1)],
                                        pack_notice=notice,
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
                                    notice,
                                )

        if rsp.status == StatusCode.StatusCode_OK and notice.items:
            session.send(MsgId.PackNotice, notice, 0)
        session.send(MsgId.TakeOutFurnitureRsp, rsp, packet_id)

        # 场景广播只在首次提交后发送; 重放不得重复移除家具。
        if rsp.status == StatusCode.StatusCode_OK and not replayed:
            broadcast = SceneGardenFurnitureRemoveNotice(
                status=StatusCode.StatusCode_OK,
                furniture_id=req.furniture_id,
                item_id=details.furniture_item_id,
            )
            for other in scene_data.get_session():
                if other.scene_id == session.scene_id and other.channel_id == session.channel_id:
                    other.send(MsgId.SceneGardenFurnitureRemoveNotice, broadcast, 0)
