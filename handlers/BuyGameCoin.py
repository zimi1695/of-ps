from config import Config
import json
import time

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId

from proto.net_pb2 import (
    BuyGameCoinReq,
    BuyGameCoinRsp,
    ItemDetail,
    PackNotice,
    StatusCode,
)

import utils.db as db
from server import activities
from utils import item_mgr
from utils.res_loader import res
from utils.pb_create import make_item


@packet_handler(MsgId.BuyGameCoinReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = BuyGameCoinReq()
        req.ParseFromString(data)

        rsp = BuyGameCoinRsp()
        rsp.status = StatusCode.StatusCode_OK
        pack = PackNotice(status=StatusCode.StatusCode_OK)

        if req.buy_count <= 0:
            rsp.status = StatusCode.StatusCode_BAD_REQ
            session.send(MsgId.BuyGameCoinRsp, rsp, packet_id)
            return

        cfg = {}
        if (
            "GlobalConfig" in res
            and "config" in res["GlobalConfig"]
            and "datas" in res["GlobalConfig"]["config"]
        ):
            for e in res["GlobalConfig"]["config"]["datas"]:
                if e.get("key") in (
                    "GameCoinBuy",
                    "GameCoinMax",
                    "GameCoinIni",
                    "GameCoinTimeRecover",
                ):
                    cfg[e["key"]] = e.get("values") or ()
        buy = cfg.get("GameCoinBuy", ())
        if len(buy) >= 3:
            currency_id, cost_per, coin_per = int(buy[0]), int(buy[1]), int(buy[2])
        else:
            currency_id, cost_per, coin_per = Config.BUY_GAME_COIN_FALLBACK_BUY
        coin_max = (
            int(cfg["GameCoinMax"][0])
            if cfg.get("GameCoinMax")
            else Config.GAME_COIN_MAX_FALLBACK
        )
        coin_ini = (
            int(cfg["GameCoinIni"][0])
            if cfg.get("GameCoinIni")
            else Config.GAME_COIN_INITIAL_FALLBACK
        )
        recover_sec = (
            int(cfg["GameCoinTimeRecover"][0])
            if cfg.get("GameCoinTimeRecover")
            else Config.GAME_COIN_RECOVER_SECONDS_FALLBACK
        )
        if recover_sec <= 0:
            recover_sec = Config.GAME_COIN_RECOVER_SECONDS_FALLBACK

        buy_count = req.buy_count
        now = int(time.time())
        player_id = session.player_id
        fingerprint = json.dumps([int(buy_count)], separators=(",", ":"))

        # 恢复、扣费、封顶发放和幂等结果在同一玩家事务内提交。
        with item_mgr.deferred_notifications(), item_mgr.player_lock(player_id), db.transaction():
            operation = activities._load_operation(
                player_id, "buy-game-coin", 0, packet_id, fingerprint
            )
            if operation == "conflict":
                rsp.status = StatusCode.StatusCode_BAD_REQ
            elif operation is not None:
                rsp.ParseFromString(operation[0])
                pack.ParseFromString(operation[1])
            else:
                coin_item = ItemDetail()
                coin_b = db.get_item_detail(player_id, Config.GAME_COIN_ITEM_ID)
                if coin_b:
                    coin_item.ParseFromString(coin_b)
                else:
                    make_item(Config.GAME_COIN_ITEM_ID, 0, player_id, coin_item)
                held = coin_item.main_item.base_item.num

                # 惰性恢复结算: kv ns=game_coin 存 last_recover_ts。
                last_ts = db.get_kv(player_id, "game_coin", "last_recover_ts")
                recovered = held
                if last_ts is None:
                    recovered = max(held, coin_ini)
                    db.set_kv(player_id, "game_coin", "last_recover_ts", now)
                elif held >= coin_max:
                    db.set_kv(player_id, "game_coin", "last_recover_ts", now)
                else:
                    gain = (now - int(last_ts)) // recover_sec
                    if gain > 0:
                        recovered = min(held + gain, coin_max)
                        if recovered >= coin_max:
                            db.set_kv(player_id, "game_coin", "last_recover_ts", now)
                        else:
                            db.set_kv(
                                player_id,
                                "game_coin",
                                "last_recover_ts",
                                int(last_ts) + gain * recover_sec,
                            )
                if recovered != held:
                    held = recovered
                    coin_item.main_item.base_item.num = held
                    db.set_item_detail(
                        player_id,
                        coin_item.SerializeToString(),
                        Config.GAME_COIN_ITEM_ID,
                        None,
                    )

                # 超过 GameCoinMax 的部分不允许购买。
                if held >= coin_max:
                    rsp.status = StatusCode.StatusCode_FAIL
                    rsp.item_details.add().CopyFrom(coin_item)
                else:
                    # 购买份数按剩余空间截断。
                    buy_count = min(
                        buy_count, (coin_max - held + coin_per - 1) // coin_per
                    )
                    cost = cost_per * buy_count
                    if not item_mgr.consume_items(
                        player_id,
                        [(currency_id, cost)],
                        pack_notice=pack,
                        session=session,
                    ):
                        rsp.status = StatusCode.StatusCode_ITEM_NOT_ENOUGH
                    else:
                        for detail in pack.items:
                            rsp.item_details.add().CopyFrom(detail)

                        new_num = min(held + coin_per * buy_count, coin_max)
                        coin_item.main_item.base_item.num = new_num
                        item_mgr.mark_new_item(player_id, coin_item)
                        db.set_item_detail(
                            player_id,
                            coin_item.SerializeToString(),
                            Config.GAME_COIN_ITEM_ID,
                            None,
                        )
                        if new_num >= coin_max:
                            db.set_kv(
                                player_id, "game_coin", "last_recover_ts", now
                            )
                        rsp.item_details.add().CopyFrom(coin_item)
                        pack.items.add().CopyFrom(coin_item)
                        activities._save_operation(
                            player_id,
                            "buy-game-coin",
                            0,
                            packet_id,
                            fingerprint,
                            rsp,
                            pack,
                        )

        session.send(MsgId.BuyGameCoinRsp, rsp, packet_id)
        if pack.items:
            session.send(MsgId.PackNotice, pack, 0)
