from config import Config

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId

from proto.net_pb2 import (
    PlayerEnergyBuyReq,
    PlayerEnergyBuyRsp,
    ItemDetail,
    PackNotice,
    StatusCode,
)

import utils.db as db
from server import activities
from utils import item_mgr
from utils.res_loader import res
from utils.pb_create import make_item
from utils import refresh
from utils import achieve_track


@packet_handler(MsgId.PlayerEnergyBuyReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = PlayerEnergyBuyReq()
        req.ParseFromString(data)

        rsp = PlayerEnergyBuyRsp()
        rsp.status = StatusCode.StatusCode_OK
        pack = PackNotice(status=StatusCode.StatusCode_OK)

        player_id = session.player_id
        purchased = False
        # 读取次数、上限、扣费、发精力和幂等结果共用同一事务。
        with item_mgr.deferred_notifications(), item_mgr.player_lock(player_id), db.transaction():
            operation = activities._load_operation(
                player_id, "player-energy-buy", 0, packet_id, "buy-energy"
            )
            if operation == "conflict":
                rsp.status = StatusCode.StatusCode_BAD_REQ
            elif operation is not None:
                rsp.ParseFromString(operation[0])
                pack.ParseFromString(operation[1])
            else:
                # 当日购买次数持久化(kv ns=buy, 按天 key 自然过期)。
                day = str(refresh.day_index())
                buy_key = f"energy:{day}"
                times = int(db.get_kv(player_id, "buy", buy_key, 0) or 0)
                rsp.energy_buy_num = times

                # 精力上限按玩家等级查 Level.player_level.player_energy。
                energy_max = Config.ENERGY_MAX_FALLBACK
                row = db.get_players_info(player_id, "level")
                if row:
                    level = row[0]
                    for e in res.get("Level", {}).get("player_level", {}).get("datas") or ():
                        if e.get("i_d") == level:
                            energy_max = e.get(
                                "player_energy", Config.ENERGY_MAX_FALLBACK
                            )
                            break

                item = ItemDetail()
                item_b = db.get_item_detail(player_id, Config.ENERGY_ITEM_ID)
                if item_b:
                    item.ParseFromString(item_b)
                else:
                    make_item(Config.ENERGY_ITEM_ID, 0, player_id, item)
                held = item.main_item.base_item.num

                # 已满时不扣星石直接失败。
                if held >= energy_max:
                    rsp.status = StatusCode.StatusCode_FAIL
                else:
                    currency_id, cost, add = Config.PLAYER_ENERGY_BUY_FALLBACK_TIER
                    for e in res.get("GlobalConfig", {}).get("config", {}).get("datas") or ():
                        if e.get("key") == "Player_Energy_Buy_Times":
                            values = e.get("values") or ()
                            if values:
                                parts = values[min(times, len(values) - 1)].split(":")
                                if len(parts) == 3:
                                    currency_id, cost, add = (
                                        int(parts[0]),
                                        int(parts[1]),
                                        int(parts[2]),
                                    )
                            break

                    if not item_mgr.consume_items(
                        player_id,
                        [(currency_id, cost)],
                        pack_notice=pack,
                        session=session,
                    ):
                        rsp.status = StatusCode.StatusCode_ITEM_NOT_ENOUGH
                    else:
                        item.main_item.base_item.num = min(held + add, energy_max)
                        db.set_item_detail(
                            player_id,
                            item.SerializeToString(),
                            Config.ENERGY_ITEM_ID,
                            None,
                        )
                        rsp.items.add().CopyFrom(item)
                        pack.items.add().CopyFrom(item)
                        rsp.energy_buy_num = db.incr_kv(
                            player_id, "buy", buy_key, 1
                        )
                        activities._save_operation(
                            player_id,
                            "player-energy-buy",
                            0,
                            packet_id,
                            "buy-energy",
                            rsp,
                            pack,
                        )
                        purchased = True

        session.send(MsgId.PlayerEnergyBuyRsp, rsp, packet_id)
        if pack.items:
            session.send(MsgId.PackNotice, pack, 0)
        if purchased:
            achieve_track.report(session, "energy_restore")
