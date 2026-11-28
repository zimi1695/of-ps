import pickle
import random

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId

from proto.net_pb2 import (
    ActivityKoiUseItemReq,
    ActivityKoiUseItemRsp,
    StatusCode,
    PackNotice,
)
import utils.db as db
from utils.res_loader import res
from utils import item_mgr


"""
# 锦鲤活动使用道具(获得抽奖码) 4519 4520
配置来源 ActivityJinli.json jinli(i_d=活动 id, 表内为 260326180):
- params2: 消耗道具 id(6044), 每个换一组抽奖码
- params3: 每个道具产出的抽奖码数量(1)
- params1: 单人抽奖码数量上限(20)
扣道具、累计次数检查与抽奖码写入在同一玩家事务内。
"""


def _koi_cfg(activity_id):
    """只接受精确活动 id；未知活动不回退到其他活动配置。"""
    sheet = res.get("ActivityJinli", {})
    section = sheet.get("jinli", {}) if isinstance(sheet, dict) else {}
    rows = section.get("datas", ()) if isinstance(section, dict) else ()
    if not isinstance(rows, (list, tuple)):
        return None
    return next(
        (row for row in rows if isinstance(row, dict) and row.get("i_d") == activity_id),
        None,
    )


def _parameters(cfg):
    if not isinstance(cfg, dict):
        return None
    try:
        code_limit = int(cfg.get("params1", 0) or 0)
        cost_item = int(cfg.get("params2", 0) or 0)
        codes_per_item = int(cfg.get("params3", 0) or 0)
    except (TypeError, ValueError):
        return None
    if (
        not 0 < code_limit <= 1_000_000
        or not 0 < cost_item <= 0xFFFFFFFF
        or not 0 < codes_per_item <= code_limit
    ):
        return None
    return code_limit, cost_item, codes_per_item


@packet_handler(MsgId.ActivityKoiUseItemReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = ActivityKoiUseItemReq()
        req.ParseFromString(data)

        rsp = ActivityKoiUseItemRsp()
        rsp.num = req.num
        rsp.activity_id = req.activity_id

        cfg = _koi_cfg(req.activity_id)
        parameters = _parameters(cfg) if cfg is not None else None
        if parameters is None:
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            session.send(MsgId.ActivityKoiUseItemRsp, rsp, packet_id)
            return
        code_limit, cost_item, codes_per_item = parameters
        if req.num == 0:
            rsp.status = StatusCode.StatusCode_BAD_REQ
            session.send(MsgId.ActivityKoiUseItemRsp, rsp, packet_id)
            return
        use_num = req.num
        code_count = use_num * codes_per_item
        if code_count > code_limit:
            rsp.status = StatusCode.StatusCode_FAIL
            session.send(MsgId.ActivityKoiUseItemRsp, rsp, packet_id)
            return

        pack = PackNotice(status=StatusCode.StatusCode_OK)
        with item_mgr.player_lock(session.player_id), db.transaction():
            raw = db.get_kv(session.player_id, "koi_codes", str(req.activity_id), b"")
            try:
                codes = pickle.loads(raw) if raw else []
            except (TypeError, ValueError, pickle.UnpicklingError, EOFError):
                codes = None
            if (
                not isinstance(codes, list)
                or len(codes) > code_limit
                or any(not isinstance(code, str) or len(code) != 8 for code in codes)
            ):
                rsp.status = StatusCode.StatusCode_FAIL
            elif len(codes) + code_count > code_limit:
                # 必须先检查上限再扣道具，且检查与扣费在同一个事务内。
                rsp.status = StatusCode.StatusCode_FAIL
            elif not db.consume_item(session.player_id, cost_item, use_num, pack):
                rsp.status = StatusCode.StatusCode_ITEM_NOT_ENOUGH
            else:
                used = set(codes)
                for _ in range(code_count):
                    code = "%08d" % random.randint(0, 99999999)
                    while code in used:
                        code = "%08d" % random.randint(0, 99999999)
                    used.add(code)
                    codes.append(code)
                    rsp.codes.append(code)
                db.set_kv(
                    session.player_id,
                    "koi_codes",
                    str(req.activity_id),
                    pickle.dumps(codes),
                )
                rsp.status = StatusCode.StatusCode_OK
                rsp.num = use_num

        session.send(MsgId.ActivityKoiUseItemRsp, rsp, packet_id)
        if rsp.status == StatusCode.StatusCode_OK and pack.items:
            session.send(MsgId.PackNotice, pack, 0)
