import json
import logging

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId

from proto.net_pb2 import (
    ActivityRecallRewardClaimReq,
    ActivityRecallRewardClaimRsp,
    StatusCode,
    PackNotice,
)
from config import Config
from server import redemption
from utils import achieve_track

logger = logging.getLogger(__name__)

"""
# 回归活动领取奖励 4511 4512
"""


@packet_handler(MsgId.ActivityRecallRewardClaimReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = ActivityRecallRewardClaimReq()
        req.ParseFromString(data)

        rsp = ActivityRecallRewardClaimRsp(
            status=StatusCode.StatusCode_OK,
            activity_id=req.activity_id,
            condition_id=req.condition_id,
        )
        rewards = Config.ACTIVITY_RECALL_REWARDS.get(
            (req.activity_id, req.condition_id),
            Config.ACTIVITY_RECALL_REWARDS.get(("*", "*"), ()),
        )
        fingerprint = json.dumps(
            [req.activity_id, req.condition_id], separators=(",", ":")
        )
        pack = PackNotice(status=StatusCode.StatusCode_OK)
        try:
            with achieve_track.batch(session):
                status, _ = redemption.claim_reward(
                    session.player_id,
                    "activity_recall",
                    f"{req.activity_id}:{req.condition_id}",
                    rewards,
                    rsp.rewards,
                    pack,
                    packet_id=packet_id,
                    fingerprint=fingerprint,
                    response=rsp,
                    on_success=lambda: achieve_track.report(session, "recall_player"),
                )
        except Exception:
            # Claim marker, rewards and packet operation all share the transaction.
            logger.exception("Recall reward fulfillment failed")
            rsp.Clear()
            rsp.activity_id = req.activity_id
            rsp.condition_id = req.condition_id
            rsp.status = StatusCode.StatusCode_SYS_ERR
            session.send(MsgId.ActivityRecallRewardClaimRsp, rsp, packet_id)
            return

        status_map = {
            "ok": StatusCode.StatusCode_OK,
            "already_claimed": StatusCode.StatusCode_REPEAT_GET_REWARD,
            "invalid_rewards": StatusCode.StatusCode_CFG_NOT_FOUND,
            "conflict": StatusCode.StatusCode_BAD_REQ,
        }
        rsp.status = status_map.get(status, StatusCode.StatusCode_SYS_ERR)
        session.send(MsgId.ActivityRecallRewardClaimRsp, rsp, packet_id)
        if status == "ok" and pack.items:
            session.send(MsgId.PackNotice, pack, 0)
