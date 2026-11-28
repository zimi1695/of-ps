import hashlib
import pickle

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId

from proto.net_pb2 import (
    ActivitySandbagRewardReq,
    ActivitySandbagRewardRsp,
    ActivitySandbagRewardType,
    StatusCode,
    PackNotice,
)
import utils.db as db
from handlers.ActivitySandbagHit import (
    interaction_cfg,
    player_state,
    lock_sandbag,
    sb_save,
)
from utils.reward_resolver import resolve_reward
from utils.res_loader import res
from utils import item_mgr
from server import activities

"""
# 打沙袋领奖 4501 4502
宝箱必须存在于服务端逐玩家持久状态且未使用; 全服档位若无权威进度证明则
拒绝领取。领取标记、used/server_claimed、奖励和 packet 指纹同事务提交。
"""


def _full_server_items(activity_id):
    rows = res.get("InteractionActivity", {}).get("full_server_activity", {}).get("datas", ())
    row = next((entry for entry in rows if entry.get("i_d") == activity_id), None)
    return row.get("full_server_activity_item", ()) if row else ()


@packet_handler(MsgId.ActivitySandbagRewardReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = ActivitySandbagRewardReq()
        req.ParseFromString(data)

        rsp = ActivitySandbagRewardRsp()
        rsp.status = StatusCode.StatusCode_BAD_REQ
        rsp.activity_id = req.activity_id
        rsp.seq_list.extend(req.seq_list)
        if not req.seq_list or req.reward_type not in (
            ActivitySandbagRewardType.RewardType_Box,
            ActivitySandbagRewardType.RewardType_Server,
        ):
            session.send(MsgId.ActivitySandbagRewardRsp, rsp, packet_id)
            return

        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()
        pack = PackNotice(status=StatusCode.StatusCode_OK)
        cfg = interaction_cfg(req.activity_id)
        server_items = {}
        if req.reward_type == ActivitySandbagRewardType.RewardType_Server:
            row = next(
                (
                    entry
                    for entry in res.get("InteractionActivity", {}).get("full_server_activity", {}).get("datas", ())
                    if entry.get("i_d") == req.activity_id
                ),
                None,
            )
            if row is None:
                rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                session.send(MsgId.ActivitySandbagRewardRsp, rsp, packet_id)
                return
            server_items = {it.get("index", 0): it for it in _full_server_items(req.activity_id)}
            # 本服没有全服贡献聚合; completion_count>0 的档位无法由客户端 seq 证明。
            if any(
                seq not in server_items
                or int(server_items[seq].get("completion_count", 0) or 0) > 0
                for seq in set(req.seq_list)
            ):
                rsp.status = StatusCode.StatusCode_NotFinishAllTask
                session.send(MsgId.ActivitySandbagRewardRsp, rsp, packet_id)
                return
        elif cfg is None:
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            session.send(MsgId.ActivitySandbagRewardRsp, rsp, packet_id)
            return

        claimed_any = False
        with (
            item_mgr.player_lock(session.player_id),
            item_mgr.deferred_notifications(),
            db.transaction(),
            lock_sandbag,
        ):
            saved = activities._load_operation(
                session.player_id,
                "sandbag-reward",
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
                try:
                    st = player_state(session.player_id)
                except (ValueError, TypeError, pickle.UnpicklingError, EOFError):
                    rsp.status = StatusCode.StatusCode_FAIL
                else:
                    rewards = []
                    if req.reward_type == ActivitySandbagRewardType.RewardType_Server:
                        for seq in dict.fromkeys(req.seq_list):
                            key = f"{req.activity_id}:{seq}"
                            if seq in st["server_claimed"] or not db.try_claim(
                                session.player_id, "sandbag_server", key
                            ):
                                continue
                            st["server_claimed"].add(seq)
                            claimed_any = True
                            for reward in server_items[seq].get("full_server_activity_reward_item", ()):
                                item_id = reward.get("item_i_d", 0)
                                count = reward.get("item_num", 0)
                                if item_id and count > 0:
                                    rewards.append((item_id, count))
                    else:
                        configured_pools = {
                            it.get("reward_pool_group_i_d", 0)
                            for it in cfg.get("activity_interaction_item", ())
                        }
                        row = db.get_players_info(session.player_id, "level")
                        player_level = row[0] if row else 0
                        claimed_pools = []
                        for seq in dict.fromkeys(req.seq_list):
                            box = next((b for b in st["boxes"] if b.get("seq") == seq), None)
                            if (
                                box is None
                                or box.get("used")
                                or box.get("activity_id") != req.activity_id
                                or box.get("pool_id") not in configured_pools
                                or not db.try_claim(
                                    session.player_id, "sandbag_box", f"{req.activity_id}:{seq}"
                                )
                            ):
                                continue
                            box["used"] = True
                            claimed_any = True
                            claimed_pools.append(box["pool_id"])
                        # 只解析本次新领取的箱子, 重放不能根据 used 状态再次发奖。
                        for pool_id in claimed_pools:
                            rewards.extend(resolve_reward(pool_id, player_level))
                    if claimed_any:
                        # used/server_claimed 与奖励同事务; 保存失败全部回滚。
                        sb_save(session.player_id, st)
                        item_mgr.grant_items(
                            session.player_id,
                            rewards,
                            rsp_items=rsp.rewards,
                            pack_notice=pack,
                        )
                        rsp.status = StatusCode.StatusCode_OK
                    else:
                        rsp.status = StatusCode.StatusCode_REPEAT_GET_REWARD
                activities._save_operation(
                    session.player_id,
                    "sandbag-reward",
                    0,
                    packet_id,
                    fingerprint,
                    rsp,
                    pack,
                )

        session.send(MsgId.ActivitySandbagRewardRsp, rsp, packet_id)
        if pack.items:
            session.send(MsgId.PackNotice, pack, 0)
