import hashlib

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId

from proto.net_pb2 import CollectMoonReq, CollectMoonRsp, StatusCode, PackNotice
import utils.db as db
from server import activities
from utils import item_mgr, achieve_track
from utils.res_loader import res

"""
# 收集祈愿月亮 2575 2576(官方抓包 pack0 1212 份 OK 样本实锤)
# 奖励由服务端固定为祈愿碎片(124) x5, 并校验权威 Item 配置。
# 同一 moon_id 用新 packet 重复收集回 BAD_REQ; 同 packet 同指纹重放原响应。
# moon_id 必须是 11000~11053 场景内 new_collection_type=10 的配置条目。
"""

_OPERATION_SYSTEM = "collect-moon"
_MOON_REWARDS = ((124, 5),)


def _moon_known(moon_id) -> bool:
    if not 11000000 <= moon_id < 12000000:
        return False
    section = res.get("CollectionItem", {})
    section = section.get("collection_item", {}) if isinstance(section, dict) else {}
    rows = section.get("datas", ()) if isinstance(section, dict) else ()
    if not isinstance(rows, (list, tuple)):
        return False
    return any(
        isinstance(row, dict)
        and row.get("i_d") == moon_id
        and row.get("new_collection_type") == 10
        for row in rows
    )


@packet_handler(MsgId.CollectMoonReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = CollectMoonReq()
        req.ParseFromString(data)

        rsp = CollectMoonRsp(moon_id=req.moon_id)
        notice = PackNotice(status=StatusCode.StatusCode_OK)
        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()

        with achieve_track.batch(session):
            with item_mgr.player_lock(session.player_id), db.transaction():
                operation = None
                if packet_id:
                    operation = activities._load_operation(
                        session.player_id, _OPERATION_SYSTEM, 0, packet_id, fingerprint
                    )
                if operation == "conflict":
                    rsp.status = StatusCode.StatusCode_BAD_REQ
                elif operation is not None:
                    saved_response, saved_pack = operation
                    rsp.ParseFromString(saved_response)
                    notice.ParseFromString(saved_pack)
                elif not _moon_known(req.moon_id):
                    rsp.status = StatusCode.StatusCode_BAD_REQ
                else:
                    try:
                        item_mgr.validate_rewards(_MOON_REWARDS)
                    except (KeyError, TypeError, ValueError):
                        rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                    else:
                        raw = db.get_kv(
                            session.player_id, "collected_moons", "ids", b""
                        )
                        try:
                            stored = raw.decode("utf-8")
                            ids = {value for value in stored.split(",") if value}
                            if any(not value.isdigit() for value in ids):
                                raise ValueError("invalid collected moon state")
                        except (AttributeError, UnicodeDecodeError, ValueError):
                            rsp.status = StatusCode.StatusCode_FAIL
                        else:
                            if str(req.moon_id) in ids:
                                rsp.status = StatusCode.StatusCode_BAD_REQ
                            else:
                                item_mgr.grant_items(
                                    session.player_id,
                                    _MOON_REWARDS,
                                    pack_notice=notice,
                                    rsp_items=rsp.rewards,
                                    session=session,
                                )
                                ids.add(str(req.moon_id))
                                db.set_kv(
                                    session.player_id,
                                    "collected_moons",
                                    "ids",
                                    ",".join(
                                        sorted(ids, key=lambda value: int(value))
                                    ).encode(),
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

            session.send(MsgId.CollectMoonRsp, rsp, packet_id)
            if notice.items:
                session.send(MsgId.PackNotice, notice, 0)
