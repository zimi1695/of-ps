import hashlib
import json
import uuid

from utils import achieve_track, battle_rewards, item_mgr

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId

from proto.net_pb2 import (
    BattleState,
    BattleEncounterStateUpdateReq,
    BattleEncounterStateUpdateRsp,
    StatusCode,
)

import utils.db as db
from utils.res_loader import res
from server import activities

"""
# 战斗遭遇状态更新 1327 1328
状态持久化于 db player_kv ns=encounter k=states, 与 BattleEncounterInfo 共用。
Finish 必须匹配服务端 Start 创建的 active run; run、状态、经验和领取标记原子提交。
"""

_RUN_NS = "encounter_runs"


def _load_run(player_id, scene_id, encounter_id):
    raw = db.get_kv(player_id, _RUN_NS, f"{scene_id}:{encounter_id}")
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


def _save_run(player_id, scene_id, encounter_id, run):
    db.set_kv(
        player_id,
        _RUN_NS,
        f"{scene_id}:{encounter_id}",
        json.dumps(run, sort_keys=True, separators=(",", ":")).encode("utf-8"),
    )


class _DeferredSession:
    def __init__(self, session):
        self._session = session
        self.pending = []

    def __getattr__(self, name):
        return getattr(self._session, name)

    def send(self, message_id, message, packet_id=0):
        saved = type(message)()
        saved.CopyFrom(message)
        self.pending.append((message_id, saved, packet_id))

    def flush(self):
        for message_id, message, packet_id in self.pending:
            self._session.send(message_id, message, packet_id)
        self.pending.clear()


@packet_handler(MsgId.BattleEncounterStateUpdateReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = BattleEncounterStateUpdateReq()
        req.ParseFromString(data)

        rsp = BattleEncounterStateUpdateRsp()
        rsp.status = StatusCode.StatusCode_BAD_REQ
        rsp.encounter.battle_id = req.encounter_id
        rsp.encounter.box_id = 0
        cfg = next(
            (
                row
                for row in res.get("BattleEncounter", {}).get("battle_encounter", {}).get("datas", ())
                if row.get("i_d") == req.encounter_id
            ),
            None,
        )
        if cfg is None:
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            session.send(MsgId.BattleEncounterStateUpdateRsp, rsp, packet_id)
            return
        if req.battle_state not in (
            BattleState.Start,
            BattleState.Finish,
            BattleState.Fail,
            BattleState.Close,
            BattleState.Reset,
        ):
            session.send(MsgId.BattleEncounterStateUpdateRsp, rsp, packet_id)
            return

        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()
        deferred_session = _DeferredSession(session)
        settled_now = False
        with (
            item_mgr.player_lock(session.player_id),
            item_mgr.deferred_notifications(),
            db.transaction(),
        ):
            saved = activities._load_operation(
                session.player_id,
                "encounter-state",
                0,
                packet_id,
                fingerprint,
            )
            if isinstance(saved, tuple):
                rsp.ParseFromString(saved[0])
            elif saved == "conflict":
                rsp.status = StatusCode.StatusCode_BAD_REQ
            else:
                run = _load_run(session.player_id, session.scene_id, req.encounter_id)
                if req.battle_state == BattleState.Start:
                    if run is None or run.get("state") != "active":
                        run = {
                            "version": 1,
                            "run_id": uuid.uuid4().hex,
                            "state": "active",
                        }
                        _save_run(session.player_id, session.scene_id, req.encounter_id, run)
                    db.set_encounter_state(session.player_id, req.encounter_id, BattleState.Start)
                    rsp.status = StatusCode.StatusCode_OK
                    rsp.encounter.state = BattleState.Start
                elif req.battle_state == BattleState.Finish:
                    if run is None or run.get("state") != "active":
                        # 客户端 Finish 不能创建服务端资格。
                        rsp.status = StatusCode.StatusCode_BAD_REQ
                    elif not db.try_claim(session.player_id, "encounter_finish", run["run_id"]):
                        rsp.status = StatusCode.StatusCode_REPEAT_GET_REWARD
                    else:
                        run["state"] = "settled"
                        _save_run(session.player_id, session.scene_id, req.encounter_id, run)
                        db.set_encounter_state(session.player_id, req.encounter_id, BattleState.Finish)
                        battle_rewards.grant_character_exp(deferred_session, "encounter", req.encounter_id)
                        rsp.status = StatusCode.StatusCode_OK
                        rsp.encounter.state = BattleState.Finish
                        settled_now = True
                else:
                    if run is None or run.get("state") != "active":
                        rsp.status = StatusCode.StatusCode_BAD_REQ
                    else:
                        run["state"] = "failed"
                        _save_run(session.player_id, session.scene_id, req.encounter_id, run)
                        state = BattleState.Close if req.battle_state == BattleState.Fail else req.battle_state
                        db.set_encounter_state(session.player_id, req.encounter_id, state)
                        rsp.status = StatusCode.StatusCode_OK
                        rsp.encounter.state = state
                activities._save_operation(
                    session.player_id,
                    "encounter-state",
                    0,
                    packet_id,
                    fingerprint,
                    rsp,
                    rsp.__class__(),
                )

        deferred_session.flush()
        session.send(MsgId.BattleEncounterStateUpdateRsp, rsp, packet_id)
        if settled_now:
            achieve_track.report(
                session,
                "encounter_done",
                match=req.encounter_id,
                scene=session.scene_id,
            )
