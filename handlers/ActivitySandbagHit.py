import base64
import hashlib
import json
import pickle
import random
import time
from threading import Lock

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId
from config import Config

from proto.net_pb2 import (
    ActivitySandbagHitReq,
    ActivitySandbagHitRsp,
    ActivitySandbagDropNotice,
    StatusCode,
)
import utils.db as db
from utils import achieve_track, item_mgr, refresh
from utils.res_loader import res

"""
# 打沙袋: 上报打击 4503 4504 -> 掉落宝箱通知 4508
沙包状态逐玩家保存在 player_kv(ns=sandbag), 每次命中在玩家锁和单事务内
保存掉落、幂等响应和成就领取标记; 不再依赖内存快照。
"""

lock_sandbag = Lock()
_OPERATION_NS = "activity_operations"


def sb_load():
    """兼容旧调用; 权威状态总是从数据库按玩家加载。"""


def sb_save(player_id, state=None):
    """保存指定玩家的沙包状态; 调用方须持 lock_sandbag 且处于事务中。"""
    if state is None:
        state = player_state(player_id)
    db.set_kv(player_id, "sandbag", "state", pickle.dumps(state))


def interaction_cfg(activity_id):
    """只接受精确匹配的活动配置, 未知 ID 失败关闭。"""
    rows = res.get("InteractionActivity", {}).get("activity_interaction", {}).get("datas", ())
    return next((row for row in rows if row.get("i_d") == activity_id), None)


def player_state(player_id):
    """加载(必要时创建)玩家沙包状态并处理每日重置; 须在 lock_sandbag 内调用。"""
    sb_load()
    raw = db.get_kv(player_id, "sandbag", "state")
    st = pickle.loads(raw) if raw else None
    if st is not None and (
        not isinstance(st, dict)
        or not isinstance(st.get("boxes"), list)
        or not isinstance(st.get("daily_drops"), dict)
        or not isinstance(st.get("life_drops"), dict)
        or not isinstance(st.get("next_seq"), int)
        or st["next_seq"] < 1
    ):
        raise ValueError("invalid sandbag state")
    if st is None:
        st = {
            "day": str(refresh.day_index()),
            "daily_drops": {},
            "life_drops": {},
            "boxes": [],
            "next_seq": 1,
            "server_claimed": set(),
        }
    elif st["day"] != str(refresh.day_index()):
        st["day"] = str(refresh.day_index())
        st["daily_drops"] = {}
    st["server_claimed"] = set(st.get("server_claimed", ()))
    return st


def _load_hit_operation(player_id, activity_id, packet_id, fingerprint):
    if not packet_id:
        return None
    raw = db.get_kv(player_id, _OPERATION_NS, f"sandbag-hit:{packet_id}")
    if raw is None:
        return None
    try:
        saved = json.loads(bytes(raw).decode("utf-8"))
        if saved.get("fingerprint") != fingerprint:
            return "conflict"
        return (
            base64.b64decode(saved["response"], validate=True),
            base64.b64decode(saved["drop"], validate=True) if saved.get("drop") else b"",
        )
    except (KeyError, TypeError, ValueError, UnicodeDecodeError):
        return "conflict"


def _save_hit_operation(player_id, activity_id, packet_id, fingerprint, rsp, drop):
    if not packet_id:
        return
    value = json.dumps(
        {
            "fingerprint": fingerprint,
            "response": base64.b64encode(rsp.SerializeToString()).decode("ascii"),
            "drop": base64.b64encode(drop.SerializeToString()).decode("ascii") if drop else "",
        },
        separators=(",", ":"),
    ).encode("utf-8")
    db.set_kv(player_id, _OPERATION_NS, f"sandbag-hit:{packet_id}", value)


@packet_handler(MsgId.ActivitySandbagHitReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = ActivitySandbagHitReq()
        req.ParseFromString(data)

        rsp = ActivitySandbagHitRsp()
        rsp.status = StatusCode.StatusCode_BAD_REQ
        rsp.activity_id = req.activity_id
        rsp.npc_id = req.npc_id
        rsp.hit_count = req.hit_count
        cfg = interaction_cfg(req.activity_id)
        if cfg is None:
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            session.send(MsgId.ActivitySandbagHitRsp, rsp, packet_id)
            return
        if req.npc_id != cfg.get("object_i_d", 0) or req.hit_count <= 0:
            session.send(MsgId.ActivitySandbagHitRsp, rsp, packet_id)
            return
        hits = min(req.hit_count, Config.SANDBAG_MAX_HIT_PER_REQUEST)
        items = cfg.get("activity_interaction_item", ())
        if not items:
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            session.send(MsgId.ActivitySandbagHitRsp, rsp, packet_id)
            return

        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()
        drop = ActivitySandbagDropNotice()
        first_today = False
        replayed = False
        with (
            item_mgr.player_lock(session.player_id),
            item_mgr.deferred_notifications(),
            db.transaction(),
            lock_sandbag,
        ):
            saved = _load_hit_operation(session.player_id, req.activity_id, packet_id, fingerprint)
            if isinstance(saved, tuple):
                rsp.ParseFromString(saved[0])
                if saved[1]:
                    drop.ParseFromString(saved[1])
                replayed = True
            elif saved == "conflict":
                rsp.status = StatusCode.StatusCode_BAD_REQ
            else:
                st = player_state(session.player_id)
                now = int(time.time())
                new_boxes = []
                for _ in range(hits):
                    avail = []
                    for it in items:
                        idx = it.get("index", 0)
                        ceiling = it.get("output_ceiling", 0)
                        if not isinstance(idx, int) or idx <= 0 or not isinstance(ceiling, int) or ceiling <= 0:
                            raise ValueError("invalid sandbag interaction item")
                        drops = st["daily_drops"] if it.get("reset_type", 0) == 1 else st["life_drops"]
                        if drops.get(idx, 0) < ceiling:
                            avail.append((it, drops))
                    if not avail:
                        break
                    it, drops = random.choice(avail)
                    idx = it["index"]
                    drops[idx] = drops.get(idx, 0) + 1
                    box = {
                        "seq": st["next_seq"],
                        "activity_id": req.activity_id,
                        "pool_id": it.get("reward_pool_group_i_d", 0),
                        "create_time": now,
                        "used": False,
                    }
                    st["next_seq"] += 1
                    st["boxes"].append(box)
                    new_boxes.append(box)
                # 先持久化状态; 任一后续发奖/幂等写入失败都会回滚本次掉落。
                sb_save(session.player_id, st)
                first_today = db.try_claim(
                    session.player_id,
                    "ach_sandbag_day",
                    f"{req.activity_id}:{refresh.day_index()}",
                )
                rsp.status = StatusCode.StatusCode_OK
                if new_boxes:
                    drop.status = StatusCode.StatusCode_OK
                    drop.activity_id = req.activity_id
                    for box in new_boxes:
                        b = drop.box_list.add()
                        b.seq = box["seq"]
                        b.pool_id = box["pool_id"]
                        b.create_time = box["create_time"]
                        b.use_status = False
                _save_hit_operation(session.player_id, req.activity_id, packet_id, fingerprint, rsp, drop)

        session.send(MsgId.ActivitySandbagHitRsp, rsp, packet_id)
        if drop.box_list:
            session.send(MsgId.ActivitySandbagDropNotice, drop, 0)
        if not replayed and rsp.status == StatusCode.StatusCode_OK:
            with achieve_track.batch(session):
                achieve_track.report(session, "sandbag_hit", match=req.npc_id, amount=hits)
                if first_today:
                    achieve_track.report(session, "sandbag_day", match=req.npc_id)
