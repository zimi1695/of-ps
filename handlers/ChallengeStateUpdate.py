import hashlib
import json
import uuid

from utils import achieve_track as _at

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId

from proto.net_pb2 import (
    ChallengeData,
    ChallengeStateUpdateReq,
    ChallengeStateUpdateRsp,
    PackNotice,
    StatusCode,
    BattleState,
)

import utils.db as db
from utils.res_loader import res
from utils import item_mgr
from server import activities


_STAR_REWARDS = {3: 1000, 2: 600, 1: 300}
_RUN_NS = "challenge_runs"


def _challenge_config(challenge_id):
    return next(
        (
            row
            for row in res.get("Challenge", {}).get("challenge", {}).get("datas", ())
            if row.get("i_d") == challenge_id
        ),
        None,
    )


def _run_key(scene_id, challenge_id):
    return f"{scene_id}:{challenge_id}"


def _load_run(player_id, scene_id, challenge_id):
    return db.get_kv(player_id, _RUN_NS, _run_key(scene_id, challenge_id))


def _save_run(player_id, scene_id, challenge_id, run):
    db.set_kv(
        player_id,
        _RUN_NS,
        _run_key(scene_id, challenge_id),
        json.dumps(run, sort_keys=True, separators=(",", ":")).encode("utf-8"),
    )


@packet_handler(MsgId.ChallengeStateUpdateReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = ChallengeStateUpdateReq()
        req.ParseFromString(data)

        rsp = ChallengeStateUpdateRsp(status=StatusCode.StatusCode_OK)
        cfg = _challenge_config(req.challenge_id)
        if cfg is None or int(cfg.get("scene_i_d", 0) or 0) != session.scene_id:
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            session.send(MsgId.ChallengeStateUpdateRsp, rsp, packet_id)
            return

        pack = PackNotice(status=StatusCode.StatusCode_OK)
        settled_now = False
        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()
        operation_id = _run_key(session.scene_id, req.challenge_id)
        with (
            item_mgr.player_lock(session.player_id),
            item_mgr.deferred_notifications(),
            db.transaction(),
        ):
            saved = None
            if req.battle_state == BattleState.Finish:
                saved = activities._load_operation(
                    session.player_id,
                    "challenge_finish",
                    operation_id,
                    packet_id,
                    fingerprint,
                )
            if isinstance(saved, tuple):
                response, _saved_pack = saved
                rsp.ParseFromString(response)
            elif saved == "conflict":
                rsp.status = StatusCode.StatusCode_BAD_REQ
            else:
                stored = db.get_challenge(
                    session.player_id, session.scene_id, req.challenge_id
                )
                challenge = ChallengeData(challenge_id=req.challenge_id)
                if stored:
                    challenge.ParseFromString(stored)
                raw_run = _load_run(session.player_id, session.scene_id, req.challenge_id)
                try:
                    run = json.loads(bytes(raw_run).decode("utf-8")) if raw_run else None
                except (TypeError, ValueError, UnicodeDecodeError):
                    run = None
                if not isinstance(run, dict) or run.get("state") not in (
                    "active", "settled", "failed"
                ):
                    run = None

                if challenge.challenge_id != req.challenge_id:
                    rsp.status = StatusCode.StatusCode_BAD_REQ
                elif req.battle_state == BattleState.Start:
                    # 只有 Start 开启新 run；重复 Finish 无法创建新资格。
                    if run is None or run.get("state") != "active" or challenge.state == BattleState.Finish:
                        run = {"version": 1, "run_id": uuid.uuid4().hex, "state": "active"}
                        _save_run(session.player_id, session.scene_id, req.challenge_id, run)
                    challenge = ChallengeData(
                        challenge_id=req.challenge_id,
                        state=BattleState.Start,
                    )
                    db.set_challenge(
                        session.player_id,
                        session.scene_id,
                        req.challenge_id,
                        challenge.SerializeToString(),
                    )
                    rsp.challenge_data.CopyFrom(challenge)
                elif challenge.state == BattleState.Finish:
                    # 已结算 run 的重放返回首次持久化成绩，不重新发币。
                    rsp.challenge_data.CopyFrom(challenge)
                    if run is None:
                        run = {"version": 1, "run_id": uuid.uuid4().hex, "state": "settled"}
                        _save_run(session.player_id, session.scene_id, req.challenge_id, run)
                elif req.battle_state == BattleState.Finish:
                    if run is not None and run.get("state") == "failed":
                        rsp.status = StatusCode.StatusCode_ChallengeNotStart
                    elif run is None or run.get("state") != "active":
                        # 新存档必须先由服务端 Start 创建 active run；Finish 不能自造资格。
                        rsp.status = StatusCode.StatusCode_ChallengeNotStart
                    else:
                        try:
                            use_time = item_mgr.validate_counter(
                                req.use_time, allow_zero=False, label="challenge use time"
                            )
                        except item_mgr.EconomyConfigError:
                            rsp.status = StatusCode.StatusCode_BAD_REQ
                        else:
                            if not db.try_claim(session.player_id, "challenge_finish", run["run_id"]):
                                rsp.status = StatusCode.StatusCode_REPEAT_GET_REWARD
                            else:
                                challenge.challenge_id = req.challenge_id
                                challenge.use_time = use_time
                                challenge.state = BattleState.Finish
                                challenge.star = 0
                                if use_time <= cfg["star_timer3"]:
                                    challenge.star = 3
                                elif use_time <= cfg["star_timer2"]:
                                    challenge.star = 2
                                elif use_time <= cfg["star_timer1"]:
                                    challenge.star = 1
                                # 领取标记、成绩、run 和金币在同一事务。
                                if challenge.star:
                                    item_mgr.grant_items(
                                        session.player_id,
                                        [(101, _STAR_REWARDS[challenge.star])],
                                        pack_notice=pack,
                                        session=session,
                                    )
                                db.set_challenge(
                                    session.player_id,
                                    session.scene_id,
                                    req.challenge_id,
                                    challenge.SerializeToString(),
                                )
                                run["state"] = "settled"
                                _save_run(session.player_id, session.scene_id, req.challenge_id, run)
                                rsp.challenge_data.CopyFrom(challenge)
                                activities._save_operation(
                                    session.player_id,
                                    "challenge_finish",
                                    operation_id,
                                    packet_id,
                                    fingerprint,
                                    rsp,
                                    pack,
                                )
                                settled_now = True
                else:
                    challenge.challenge_id = req.challenge_id
                    challenge.state = req.battle_state
                    challenge.use_time = req.use_time
                    db.set_challenge(
                        session.player_id,
                        session.scene_id,
                        req.challenge_id,
                        challenge.SerializeToString(),
                    )
                    rsp.challenge_data.CopyFrom(challenge)
                    if req.battle_state in (BattleState.Fail, BattleState.Close):
                        if run is None:
                            run = {"version": 1, "run_id": uuid.uuid4().hex, "state": "failed"}
                        else:
                            run["state"] = "failed"
                        _save_run(session.player_id, session.scene_id, req.challenge_id, run)

        if settled_now:
            asset = cfg.get("asset_full_name", "")
            racing = any(k in asset for k in ("TimeSpeed", "TimeStar", "TimeRun"))
            _at.report(
                session,
                "race_done" if racing else "world_challenge_done",
                match=cfg.get("scene_i_d"),
            )
        session.send(MsgId.ChallengeStateUpdateRsp, rsp, packet_id)
        if settled_now and pack.items:
            session.send(MsgId.PackNotice, pack, 0)
