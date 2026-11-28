import hashlib

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId
from proto.net_pb2 import CollectCoinReq, CollectCoinRsp, StatusCode, PackNotice
from config import Config

import utils.db as db
from utils import achieve_track, dungeon_state, item_mgr
from utils.res_loader import res
from server import activities

_SYSTEM = "dungeon-collect-coin"


@packet_handler(MsgId.CollectCoinReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        # 与其他副本/经济操作保持相同锁顺序，状态和奖励共用一个事务。
        with (
            item_mgr.player_lock(session.player_id),
            dungeon_state.player_lock(session.player_id),
        ):
            self._handle(session, data, packet_id)

    def _handle(self, session, data: bytes, packet_id: int):
        req = CollectCoinReq()
        req.ParseFromString(data)

        rsp = CollectCoinRsp()
        rsp.status = StatusCode.StatusCode_OK
        rsp.coin_index = req.coin_index
        pack = PackNotice(status=StatusCode.StatusCode_OK)
        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()

        try:
            with (
                item_mgr.deferred_notifications(),
                achieve_track.batch(session),
                db.transaction(),
            ):
                saved = activities._load_operation(
                    session.player_id, _SYSTEM, 0, packet_id, fingerprint
                )
                if saved == "conflict":
                    rsp.status = StatusCode.StatusCode_BAD_REQ
                elif saved is not None:
                    response, saved_pack = saved
                    rsp.ParseFromString(response)
                    pack.ParseFromString(saved_pack)
                else:
                    dungeon_id = session.dungeon[0]
                    if not dungeon_id:
                        rsp.status = StatusCode.StatusCode_PlayerNotInDungeon
                    else:
                        scene = next(
                            (
                                row
                                for row in res.get("ScenesConfigAsset", {}).get(
                                    "Dungeons", ()
                                )
                                if row.get("ID") == dungeon_id
                            ),
                            None,
                        )
                        if not scene or not any(
                            row.get("Index", 0) == req.coin_index
                            for row in scene.get("CoinInfos", ())
                        ):
                            rsp.status = StatusCode.StatusCode_BAD_REQ
                        else:
                            # 奖励固定来自服务端配置；客户端只能选择已配置点位。
                            item_mgr.validate_rewards([Config.DUNGEON_COIN_REWARD])
                            dg = dungeon_state.load(session.player_id, dungeon_id)
                            if req.coin_index not in dg.coins:
                                with achieve_track.batch(session):
                                    item_mgr.grant_items(
                                        session.player_id,
                                        [Config.DUNGEON_COIN_REWARD],
                                        rsp_items=rsp.rewards,
                                        pack_notice=pack,
                                    )
                                    dg.coins.append(req.coin_index)
                                    achieve_track.report(
                                        session,
                                        "dungeon_coin",
                                        dungeon_id=dungeon_id,
                                    )
                                    dungeon_state.save(session, dg)
                            activities._save_operation(
                                session.player_id,
                                _SYSTEM,
                                0,
                                packet_id,
                                fingerprint,
                                rsp,
                                pack,
                            )
        except item_mgr.EconomyConfigError:
            rsp.Clear()
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            rsp.coin_index = req.coin_index
            pack.Clear()

        session.send(MsgId.CollectCoinRsp, rsp, packet_id)
        if pack.items:
            session.send(MsgId.PackNotice, pack, 0)
