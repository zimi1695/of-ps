import hashlib
import json
import uuid

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId
from config import Config

from proto.net_pb2 import (
    FlagBattleStateUpdateReq,
    FlagBattleStateUpdateRsp,
    FlagUnLockRemoveNotice,
    StatusCode,
    BattleState,
    EMissionType,
    PackNotice,
)

import utils.db as db
from utils import achieve_track, item_mgr, flag_state, battle_rewards
from utils.reward_resolver import resolve_drop
from utils.res_loader import res
from server import activities

"""
# 旗帜战斗状态更新 1325 1326
普通 Finish 必须匹配服务端 Start 创建的 active run。景点使用单个 ScenicSpot
请求, 但战斗 ID、场景和默认旗标必须由权威配置证明。首通状态、箱子和奖励
与 packet 指纹记录在同一玩家锁事务中提交。
"""

_RUN_NS = "flag_battle_runs"


def _flag_rows_of_battle(battle_id, scene_id=None):
    rows = []
    for row in res.get("Flag", {}).get("flag", {}).get("datas", ()):
        if scene_id is not None and row.get("scene_i_d") != scene_id:
            continue
        if any(g.get("relation_i_d") == battle_id for g in row.get("flag_group_info", ())):
            rows.append(row)
    return rows


def _flag_ids_of_battle(battle_id):
    return [row["i_d"] for row in _flag_rows_of_battle(battle_id) if row.get("i_d")]


def _eligible_flag_rows(battle_id, player_id, scene_id):
    rows = _flag_rows_of_battle(battle_id, scene_id)
    visible = flag_state.visible_ids(player_id)
    return [
        row
        for row in rows
        if row.get("icon_i_d") == 3
        or row.get("i_d") in visible
    ]


def _load_run(player_id, scene_id, battle_id):
    raw = db.get_kv(player_id, _RUN_NS, f"{scene_id}:{battle_id}")
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


def _save_run(player_id, scene_id, battle_id, run):
    db.set_kv(
        player_id,
        _RUN_NS,
        f"{scene_id}:{battle_id}",
        json.dumps(run, sort_keys=True, separators=(",", ":")).encode("utf-8"),
    )


class _DeferredSession:
    """抑制事务内通知; DB 回滚时不会发出已撤销奖励的提示。"""

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


@packet_handler(MsgId.FlagBattleStateUpdateReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = FlagBattleStateUpdateReq()
        req.ParseFromString(data)

        rsp = FlagBattleStateUpdateRsp()
        rsp.status = StatusCode.StatusCode_BAD_REQ
        rsp.battle_data.battle_id = req.battle_id
        rsp.battle_data.type = req.mission_type
        pack = PackNotice(status=StatusCode.StatusCode_OK)
        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()
        scenic = None
        eligible_rows = []
        if req.mission_type == EMissionType.EMissionType_ScenicSpot:
            if req.battle_state != BattleState.Start:
                session.send(MsgId.FlagBattleStateUpdateRsp, rsp, packet_id)
                return
            scenic = next(
                (
                    row
                    for row in res.get("Scenic", {}).get("scenic", {}).get("datas", ())
                    if row.get("i_d") == req.battle_id
                ),
                None,
            )
            eligible_rows = _eligible_flag_rows(req.battle_id, session.player_id, session.scene_id)
            if scenic is None or not eligible_rows:
                rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                session.send(MsgId.FlagBattleStateUpdateRsp, rsp, packet_id)
                return
            rsp.battle_data.state = BattleState.Finish
            rsp.battle_data.voice_id = Config.SCENIC_VOICE_IDS.get(
                req.battle_id, scenic.get("guide_voice", 0)
            )
        elif req.battle_state not in (
            BattleState.Start,
            BattleState.Finish,
            BattleState.Fail,
            BattleState.Close,
            BattleState.Reset,
        ):
            session.send(MsgId.FlagBattleStateUpdateRsp, rsp, packet_id)
            return

        deferred_session = _DeferredSession(session)
        notices = []
        settled_now = False
        scenic_first = False
        with (
            item_mgr.player_lock(session.player_id),
            item_mgr.deferred_notifications(),
            db.transaction(),
        ):
            saved = activities._load_operation(
                session.player_id,
                "flag-state",
                0,
                packet_id,
                fingerprint,
            )
            if isinstance(saved, tuple):
                rsp.ParseFromString(saved[0])
                pack.ParseFromString(saved[1])
            elif saved == "conflict":
                rsp.status = StatusCode.StatusCode_BAD_REQ
            elif scenic is not None:
                completed = bool(db.get_kv(session.player_id, "flag_battle", f"b:{req.battle_id}", 0))
                first = not completed and db.try_claim(
                    session.player_id, "flag_scenic", req.battle_id
                )
                db.set_kv(session.player_id, "flag_battle", f"b:{req.battle_id}", 1)
                if first:
                    level, world_level = db.get_players_info(
                        session.player_id, "level,world_level"
                    )
                    rewards = resolve_drop(scenic.get("first_reward_i_d"), world_level, level)
                    item_mgr.grant_items(
                        session.player_id, rewards, pack_notice=pack, session=session
                    )
                    scenic_first = True
                rsp.status = StatusCode.StatusCode_OK
                activities._save_operation(
                    session.player_id,
                    "flag-state",
                    0,
                    packet_id,
                    fingerprint,
                    rsp,
                    pack,
                )
            else:
                eligible_rows = _eligible_flag_rows(req.battle_id, session.player_id, session.scene_id)
                if not eligible_rows:
                    rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                else:
                    run = _load_run(session.player_id, session.scene_id, req.battle_id)
                    if req.battle_state == BattleState.Start:
                        if run is None or run.get("state") != "active":
                            run = {
                                "version": 1,
                                "run_id": uuid.uuid4().hex,
                                "state": "active",
                                "mission_type": req.mission_type,
                            }
                            _save_run(session.player_id, session.scene_id, req.battle_id, run)
                        elif run.get("mission_type") != req.mission_type:
                            rsp.status = StatusCode.StatusCode_BAD_REQ
                        if rsp.status != StatusCode.StatusCode_BAD_REQ:
                            rsp.status = StatusCode.StatusCode_OK
                            rsp.battle_data.state = BattleState.Start
                    elif req.battle_state == BattleState.Finish:
                        if (
                            run is None
                            or run.get("state") != "active"
                            or run.get("mission_type") != req.mission_type
                        ):
                            rsp.status = StatusCode.StatusCode_BAD_REQ
                        elif not db.try_claim(session.player_id, "flag_finish", run["run_id"]):
                            rsp.status = StatusCode.StatusCode_REPEAT_GET_REWARD
                        else:
                            completed = bool(db.get_kv(session.player_id, "flag_battle", f"b:{req.battle_id}", 0))
                            first = not completed and db.try_claim(
                                session.player_id, "flag_first", req.battle_id
                            )
                            db.set_kv(session.player_id, "flag_battle", f"b:{req.battle_id}", 1)
                            run["state"] = "settled"
                            _save_run(session.player_id, session.scene_id, req.battle_id, run)
                            rsp.battle_data.state = BattleState.Finish
                            if first:
                                battle_rewards.grant_character_exp(deferred_session, "flag", req.battle_id)
                                flag_ids = [row["i_d"] for row in eligible_rows]
                                flag_state.remove_scene_flags(session.player_id, flag_ids)
                                if flag_ids:
                                    rm = FlagUnLockRemoveNotice(status=StatusCode.StatusCode_OK)
                                    rm.flag_ids.extend(flag_ids)
                                    notices.append((MsgId.FlagUnLockRemoveNotice, rm, 0))
                                datas = res.get("TreasureBox", {}).get("encounter_treasure_box", {}).get("datas", ())
                                box = next(iter(datas), None) if datas else None
                                if box and box.get("i_d"):
                                    idx = db.incr_kv(session.player_id, "dyn_box", "seq")
                                    db.set_kv(session.player_id, "dyn_box", idx, box["i_d"])
                                    rsp.dynamic_treasure_box_base_info.box_id = box["i_d"]
                                    rsp.dynamic_treasure_box_base_info.index = idx
                                settled_now = True
                            rsp.status = StatusCode.StatusCode_OK
                    else:
                        if run is None or run.get("state") != "active":
                            rsp.status = StatusCode.StatusCode_BAD_REQ
                        else:
                            run["state"] = "failed"
                            _save_run(session.player_id, session.scene_id, req.battle_id, run)
                            rsp.status = StatusCode.StatusCode_OK
                            rsp.battle_data.state = req.battle_state
                activities._save_operation(
                    session.player_id,
                    "flag-state",
                    0,
                    packet_id,
                    fingerprint,
                    rsp,
                    pack,
                )

        deferred_session.flush()
        if settled_now:
            with achieve_track.batch(session):
                achieve_track.report(session, "battle_finish")
                achieve_track.add(session, 4, match=req.battle_id)
        if scenic_first:
            with achieve_track.batch(session):
                for row in eligible_rows:
                    achieve_track.scenic(session, row["i_d"])
                achieve_track.explore_collect(session)
                achieve_track.add(session, 4, match=req.battle_id)
        for notice in notices:
            session.send(*notice)
        session.send(MsgId.FlagBattleStateUpdateRsp, rsp, packet_id)
        if pack.items:
            session.send(MsgId.PackNotice, pack, 0)
