import logging

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId

from proto.net_pb2 import (
    InheritCodeUseReq,
    InheritCodeUseRsp,
    StatusCode,
    PackNotice,
)
from server import redemption

logger = logging.getLogger(__name__)

"""
# 使用继承码 3001 3002
继承码用于跨渠道/换端继承账号奖励, 码库由 GM 写入 db.redeem_codes(code_type='inherit')。
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


@packet_handler(MsgId.InheritCodeUseReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = InheritCodeUseReq()
        req.ParseFromString(data)

        rsp = InheritCodeUseRsp(
            status=StatusCode.StatusCode_OK, code_str=req.code
        )
        pack = PackNotice(status=StatusCode.StatusCode_OK)
        try:
            status, _rewards = redemption.redeem_code(
                session.player_id,
                req.code,
                rsp.rewards,
                pack,
                code_type="inherit",
                packet_id=packet_id,
                response=rsp,
            )
        except Exception:
            # The transaction rolls back the code counter/use marker and grant.
            logger.exception("Inherit-code fulfillment failed")
            rsp.Clear()
            rsp.code_str = req.code
            rsp.status = StatusCode.StatusCode_SYS_ERR
            session.send(MsgId.InheritCodeUseRsp, rsp, packet_id)
            return

        rsp.status = _STATUS_MAP.get(status, StatusCode.StatusCode_SYS_ERR)
        session.send(MsgId.InheritCodeUseRsp, rsp, packet_id)
        if status == 0 and pack.items:
            session.send(MsgId.PackNotice, pack, 0)
