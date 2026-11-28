import hashlib
import json
import uuid

from utils import achieve_track as _at

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId

from proto.net_pb2 import (
    RiddleStateUpdateReq,
    RiddleStateUpdateRsp,
    StatusCode,
    BattleState,
    PackNotice,
)

import utils.db as db
from utils.res_loader import res
from utils.reward_resolver import resolve_reward
from utils import item_mgr
from server import activities

"""
# 谜题状态更新 1323 1324
完成状态持久化于 kv(ns=riddle), 首次通关发首通池, 之后发重复池。
Finish 必须匹配服务端 Start 创建的 active run; 领取标记、奖励和状态同事务提交。
"""

_RUN_NS = "riddle_runs"


def _load_run(player_id, riddle_id):
    raw = db.get_kv(player_id, _RUN_NS, riddle_id)
    try:
        run = json.loads(bytes(raw).decode("utf-8")) if raw else None
    except (TypeError, ValueError, UnicodeDecodeError):
        return None
    if not isinstance(run, dict) or run.get("state") not in (
        "active",
        "settled",
        "failed",
    ):
        return None
    return run


def _save_run(player_id, riddle_id, run):
    db.set_kv(
        player_id,
        _RUN_NS,
        riddle_id,
        json.dumps(run, sort_keys=True, separators=(",", ":")).encode("utf-8"),
    )


@packet_handler(MsgId.RiddleStateUpdateReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = RiddleStateUpdateReq()
        req.ParseFromString(data)

        rsp = RiddleStateUpdateRsp()
        rsp.status = StatusCode.StatusCode_BAD_REQ
        rsp.riddle_data.riddle_id = req.riddle_id
        rows = res.get("TreasureBox", {}).get("riddle_treasure_box", {}).get("datas", ())
        box = next((b for b in rows if b.get("i_d") == req.riddle_id), None)
        if box:
            rsp.dynamic_treasure_box_base_info.box_id = box["i_d"]

        cfg = next(
            (
                e
                for e in res.get("Riddle", {}).get("riddle", {}).get("datas", ())
                if isinstance(e, dict) and e.get("i_d") == req.riddle_id
            ),
            None,
        )
        if cfg is None or req.battle_state not in (
            BattleState.Start,
            BattleState.Finish,
            BattleState.Fail,
            BattleState.Close,
            BattleState.Reset,
        ):
            session.send(MsgId.RiddleStateUpdateRsp, rsp, packet_id)
            return
        if (req.battle_state == BattleState.Finish) != req.is_win:
            session.send(MsgId.RiddleStateUpdateRsp, rsp, packet_id)
            return

        pack = PackNotice(status=StatusCode.StatusCode_OK)
        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()
        settled_now = False
        with (
            item_mgr.player_lock(session.player_id),
            item_mgr.deferred_notifications(),
            db.transaction(),
        ):
            saved = activities._load_operation(
                session.player_id,
                "riddle-state",
                0,
                packet_id,
                fingerprint,
            )
            if isinstance(saved, tuple):
                rsp.ParseFromString(saved[0])
                pack.ParseFromString(saved[1])
            elif saved == "conflict":
                rsp.status = StatusCode.StatusCode_BAD_REQ
            else:
                run = _load_run(session.player_id, req.riddle_id)
                if req.battle_state == BattleState.Start:
                    # 重放旧的 Start packet 也不会在已结算后创建新资格。
                    if run is None or run.get("state") != "active":
                        run = {
                            "version": 1,
                            "run_id": uuid.uuid4().hex,
                            "state": "active",
                            "scene_id": session.scene_id,
                        }
                        _save_run(session.player_id, req.riddle_id, run)
                    elif run.get("scene_id") != session.scene_id:
                        rsp.status = StatusCode.StatusCode_RiddleNotStart
                    if rsp.status != StatusCode.StatusCode_RiddleNotStart:
                        rsp.status = StatusCode.StatusCode_OK
                        rsp.riddle_data.state = BattleState.Start
                        rsp.riddle_data.is_win = False
                elif req.battle_state == BattleState.Finish:
                    if (
                        run is None
                        or run.get("state") != "active"
                        or run.get("scene_id") != session.scene_id
                    ):
                        # is_win 本身不是资格; 无服务端 Start 时失败关闭。
                        rsp.status = StatusCode.StatusCode_RiddleNotStart
                    elif not db.try_claim(
                        session.player_id, "riddle_finish", run["run_id"]
                    ):
                        rsp.status = StatusCode.StatusCode_REPEAT_GET_REWARD
                    else:
                        completed = bool(db.get_kv(session.player_id, "riddle", req.riddle_id))
                        first = not completed and db.try_claim(
                            session.player_id, "riddle_first", req.riddle_id
                        )
                        pool_id = (
                            cfg.get("reward_pool_i_d")
                            if first
                            else cfg.get("repeat_reward_pool_i_d")
                        )
                        db.set_kv(session.player_id, "riddle", req.riddle_id, 1)
                        run["state"] = "settled"
                        _save_run(session.player_id, req.riddle_id, run)
                        if pool_id:
                            row = db.get_players_info(session.player_id, "level")
                            rewards = resolve_reward(pool_id, row[0] if row else 0)
                            item_mgr.grant_items(
                                session.player_id,
                                rewards,
                                pack_notice=pack,
                                session=session,
                            )
                        rsp.status = StatusCode.StatusCode_OK
                        rsp.riddle_data.state = BattleState.Finish
                        rsp.riddle_data.is_win = True
                        settled_now = True
                else:
                    if run is None or run.get("state") != "active":
                        rsp.status = StatusCode.StatusCode_RiddleNotStart
                    else:
                        run["state"] = "failed"
                        _save_run(session.player_id, req.riddle_id, run)
                        rsp.status = StatusCode.StatusCode_OK
                        rsp.riddle_data.state = req.battle_state
                        rsp.riddle_data.is_win = False
                activities._save_operation(
                    session.player_id,
                    "riddle-state",
                    0,
                    packet_id,
                    fingerprint,
                    rsp,
                    pack,
                )

        if settled_now:
            _at.report(session, "riddle_done")
        session.send(MsgId.RiddleStateUpdateRsp, rsp, packet_id)
        if pack.items:
            session.send(MsgId.PackNotice, pack, 0)
