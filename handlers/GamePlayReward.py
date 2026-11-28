import hashlib
import json

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId

from proto.net_pb2 import GamePlayRewardReq, GamePlayRewardRsp, PackNotice, StatusCode
import utils.db as db
from utils.res_loader import res
from utils import item_mgr
from server import activities, minigames


"""
# 玩法领奖 1451 1452
资格只来自 server.minigames 已持久化的 settled run，且该 run 的模式、子模式、
玩法实例和成员关系都必须与 TeamGameplay 权威配置一致。请求本身不能创建资格。
每个 run 最多生成一个动态宝箱；同一 packet_id 重放返回首次响应。
"""

_SYSTEM = "gameplay_reward"
_OPERATION_SYSTEM = "gameplay"


def _box_id(reward_id):
    box_id = reward_id + 2000
    known = any(
        row.get("i_d") == box_id
        for row in res.get("TreasureBox", {})
        .get("game_treasure_box", {})
        .get("datas", ())
    )
    return box_id if known else None


def _mode_config(reward_id):
    return next(
        (
            row
            for row in res.get("TeamGameplay", {}).get("mode", {}).get("datas", ())
            if row.get("i_d") == reward_id
        ),
        None,
    )


def _load_run(run_id):
    """在调用方已持有的 DB 事务内读取，不反向获取 minigames.lock。"""
    raw = db.get_kv(minigames._SYSTEM_PLAYER_ID, minigames._NAMESPACE, run_id)
    if not raw:
        return None
    try:
        run = json.loads(bytes(raw).decode("utf-8"))
    except (TypeError, ValueError, UnicodeDecodeError):
        return None
    return run if isinstance(run, dict) and run.get("version") == 1 else None


def _eligible(player_id, reward_id, mode):
    """返回权威结算 run；已直接发奖的 run 不作为玩法箱资格。"""
    try:
        sub_modes = {
            int(entry.get("sub_mode_id", 0) or 0)
            for entry in mode.get("mode_items", ())
        }
    except (AttributeError, TypeError, ValueError):
        return []
    if not sub_modes:
        return []
    candidates = []
    for run_id in db.get_kv_keys(minigames._SYSTEM_PLAYER_ID, minigames._NAMESPACE):
        if str(run_id).startswith("current:"):
            continue
        run = _load_run(run_id)
        if not run or run.get("run_id") != run_id:
            continue
        try:
            if (
                run.get("status") != "settled"
                or int(run.get("star", 0) or 0) <= 0
                or int(run.get("mode_id", 0) or 0) != reward_id
                or int(run.get("sub_mode_id", 0) or 0) not in sub_modes
                or not int(run.get("start_ms", 0) or 0)
                or int(run.get("end_ms", 0) or 0) < int(run.get("start_ms", 0) or 0)
                or str(player_id) not in (run.get("members") or {})
                or str(player_id) not in (run.get("results") or {})
            ):
                continue
            gameplay_id = int(run.get("gameplay_id", 0) or 0)
            sub_cfg = next(
                (
                    row
                    for row in res.get("TeamGameplay", {})
                    .get("sub_mode", {})
                    .get("datas", ())
                    if row.get("i_d") == run.get("sub_mode_id")
                ),
                None,
            )
            if sub_cfg is None or not any(
                entry.get("index") == gameplay_id
                for entry in sub_cfg.get("sub_mode_items", ())
            ):
                continue
            result = run["results"][str(player_id)]
            if not isinstance(result, dict):
                continue
            # MP_ROOM_REWARDS 若已直接发放奖励，不能再通过玩法箱重复支付。
            if result.get("items") or result.get("extra_items"):
                continue
        except (TypeError, ValueError):
            continue
        candidates.append((int(run.get("end_ms", 0) or 0), str(run_id)))
    return [run_id for _, run_id in sorted(candidates)]


@packet_handler(MsgId.GamePlayRewardReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = GamePlayRewardReq()
        req.ParseFromString(data)

        rsp = GamePlayRewardRsp()
        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()
        if req.num != 1:
            rsp.status = StatusCode.StatusCode_BAD_REQ
            session.send(MsgId.GamePlayRewardRsp, rsp, packet_id)
            return

        box_id = _box_id(req.reward_id)
        if box_id is None:
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            session.send(MsgId.GamePlayRewardRsp, rsp, packet_id)
            return
        mode = _mode_config(req.reward_id)
        if mode is None:
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            session.send(MsgId.GamePlayRewardRsp, rsp, packet_id)
            return

        with item_mgr.player_lock(session.player_id), db.transaction():
            saved = activities._load_operation(
                session.player_id,
                _OPERATION_SYSTEM,
                0,
                packet_id,
                fingerprint,
            )
            if isinstance(saved, tuple):
                response, _pack = saved
                rsp.ParseFromString(response)
            elif saved == "conflict":
                rsp.status = StatusCode.StatusCode_BAD_REQ
            else:
                eligible = _eligible(session.player_id, req.reward_id, mode)
                unclaimed = [
                    run_id
                    for run_id in eligible
                    if not db.has_claimed(session.player_id, _SYSTEM, run_id)
                ]
                if not eligible:
                    # 没有服务端结算证据时一律拒绝，不把 reward_id 当资格。
                    rsp.status = StatusCode.StatusCode_NotFinishAllTask
                elif not unclaimed:
                    rsp.status = StatusCode.StatusCode_GamePlayRewardLimit
                else:
                    run_id = unclaimed[0]
                    if not db.try_claim(session.player_id, _SYSTEM, run_id):
                        rsp.status = StatusCode.StatusCode_GamePlayRewardLimit
                    else:
                        try:
                            idx = db.incr_kv(session.player_id, "dyn_box", "seq")
                            db.set_kv(session.player_id, "dyn_box", idx, box_id)
                            rsp.status = StatusCode.StatusCode_OK
                            rsp.dynamic_treasure_box_info.box_id = box_id
                            rsp.dynamic_treasure_box_info.index = idx
                            activities._save_operation(
                                session.player_id,
                                _OPERATION_SYSTEM,
                                0,
                                packet_id,
                                fingerprint,
                                rsp,
                                PackNotice(),
                            )
                        except Exception:
                            db.rollback_claim(session.player_id, _SYSTEM, run_id)
                            raise

        session.send(MsgId.GamePlayRewardRsp, rsp, packet_id)
