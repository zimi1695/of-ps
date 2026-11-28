import logging

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId

from proto.net_pb2 import (
    RecallCodeUseReq,
    RecallCodeUseRsp,
    StatusCode,
    PackNotice,
)
from server import redemption

logger = logging.getLogger(__name__)

"""
# 使用回归召回码 4513 4514
老玩家把召回码发给回归玩家(或反之)兑换奖励, 码库由 GM 写入 db.redeem_codes(code_type='recall')。
"""

_STATUS_MAP = {
    0: StatusCode.StatusCode_OK,
    1: StatusCode.StatusCode_REDEMPTION_CODE_NOT_EXIST,
    2: StatusCode.StatusCode_REDEMPTION_CODE_NOT_EXIST,
    3: StatusCode.StatusCode_REDEMPTION_CODE_NUM_IS_MAX,
    4: StatusCode.StatusCode_REDEMPTION_CODE_USED,
    5: StatusCode.StatusCode_REDEMPTION_CODE_FAIL,
    6: StatusCode.StatusCode_REDEMPTION_CODE_FAIL,
    "conflict": StatusCode.StatusCode_BAD_REQ,
}


@packet_handler(MsgId.RecallCodeUseReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = RecallCodeUseReq()
        req.ParseFromString(data)

        rsp = RecallCodeUseRsp(status=StatusCode.StatusCode_OK)
        pack = PackNotice(status=StatusCode.StatusCode_OK)
        try:
            status, _rewards = redemption.redeem_code(
                session.player_id,
                req.code,
                rsp.rewards,
                pack,
                code_type="recall",
                packet_id=packet_id,
                response=rsp,
            )
        except Exception:
            # The transaction rolls back the code counter/use marker and grant.
            logger.exception("Recall-code fulfillment failed")
            rsp.Clear()
            rsp.status = StatusCode.StatusCode_SYS_ERR
            session.send(MsgId.RecallCodeUseRsp, rsp, packet_id)
            return

        rsp.status = _STATUS_MAP.get(status, StatusCode.StatusCode_SYS_ERR)
        session.send(MsgId.RecallCodeUseRsp, rsp, packet_id)
        if status == 0 and pack.items:
            session.send(MsgId.PackNotice, pack, 0)
