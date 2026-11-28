import copy
import hashlib
import pickle
from contextlib import ExitStack

from config import Config
from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId
from proto.net_pb2 import (
    WishListBuyReq,
    WishListBuyRsp,
    WishListBuyNotice,
    PackNotice,
    StatusCode,
)

import utils.db as db
from server import activities
from server.scene_data import get_session
from server import photo_share as ps
from utils import item_mgr
from utils.res_loader import res


_SYSTEM = "wish-list-buy"
_GAME_COIN_ITEM_ID = 102
_PREMIUM_ITEM_ID = 108


class _Reject(Exception):
    def __init__(self, status):
        self.status = status


def _rows(sheet, table):
    section = res.get(sheet, {})
    section = section.get(table, {}) if isinstance(section, dict) else {}
    rows = section.get("datas", ()) if isinstance(section, dict) else ()
    return rows if isinstance(rows, (list, tuple)) else ()


def _one(rows, label):
    matches = [row for row in rows if isinstance(row, dict)]
    if len(matches) != 1:
        raise item_mgr.EconomyConfigError(f"missing or ambiguous {label}")
    return matches[0]


def _configuration(req):
    """只按服务端商店/奖池配置计算礼物，忽略客户端价格与创建时间。"""
    try:
        shop_id = item_mgr.validate_counter(req.info.shop_id, allow_zero=False, label="wish shop id")
        grid_id = item_mgr.validate_counter(req.info.grid_id, allow_zero=False, label="wish grid id")
        grid = _one(
            (row for row in _rows("Shop", "grid") if row.get("i_d") == shop_id),
            "wish shop",
        )
        grid_item = _one(
            (
                row
                for row in grid.get("items", ())
                if isinstance(row, dict) and row.get("grid_i_d") == grid_id
            ),
            "wish grid",
        )
        pool_id = item_mgr.validate_counter(
            grid_item.get("shop_pool_i_d"), allow_zero=False, label="wish pool id"
        )
        pool = _one(
            (row for row in _rows("Shop", "pool") if row.get("i_d") == pool_id),
            "wish pool",
        )
        if req.info.pool_id and req.info.pool_id != pool_id:
            raise item_mgr.EconomyConfigError("wish pool does not match shop grid")
        products = pool.get("items")
        if not isinstance(products, (list, tuple)) or not products:
            raise item_mgr.EconomyConfigError("wish pool has no products")

        configured_costs = {}
        rewards = {}
        for product in products:
            if not isinstance(product, dict):
                raise item_mgr.EconomyConfigError("invalid wish product")
            currencies = product.get("shop_currency_item", ())
            if not isinstance(currencies, (list, tuple)):
                raise item_mgr.EconomyConfigError("invalid wish currencies")
            for currency in currencies:
                if not isinstance(currency, dict):
                    raise item_mgr.EconomyConfigError("invalid wish currency")
                currency_id = currency.get("currency_i_d", 101)
                item_mgr.validate_stackable_item_id(currency_id)
                price = item_mgr.validate_count(
                    currency.get("price"), label="wish price"
                )
                configured_costs[currency_id] = item_mgr.checked_add(
                    configured_costs.get(currency_id, 0), price
                )

            item_id = product.get("item_i_d")
            item_mgr.validate_item_id(item_id)
            item_num = item_mgr.validate_count(
                product.get("item_num", 1), label="wish item count"
            )
            rewards[item_id] = item_mgr.checked_add(rewards.get(item_id, 0), item_num)

        # 整批奖励先校验，未知物品/实例数量错误不能发生在扣费之后。
        item_mgr.validate_rewards(tuple(rewards.items()))
        return pool_id, configured_costs, tuple(rewards.items())
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError) as error:
        raise item_mgr.EconomyConfigError("invalid wish configuration") from error


def _premium_rate():
    """菱石补差比例来自 GameCoinBuy，不使用客户端值或代码内回退值。"""
    try:
        row = _one(
            (row for row in _rows("GlobalConfig", "config") if row.get("key") == "GameCoinBuy"),
            "GameCoinBuy configuration",
        )
        values = row.get("values")
        if not isinstance(values, (list, tuple)) or len(values) < 3:
            raise item_mgr.EconomyConfigError("invalid GameCoinBuy configuration")
        parsed = []
        for value in values[:3]:
            if isinstance(value, bool) or not isinstance(value, (int, str)):
                raise item_mgr.EconomyConfigError("invalid GameCoinBuy value")
            parsed.append(int(value))
        currency_id, premium_per, coin_per = parsed
        item_mgr.validate_stackable_item_id(currency_id)
        item_mgr.validate_stackable_item_id(_PREMIUM_ITEM_ID)
        item_mgr.validate_count(premium_per, label="premium price")
        item_mgr.validate_count(coin_per, label="game coin amount")
        if currency_id != _GAME_COIN_ITEM_ID:
            raise item_mgr.EconomyConfigError("unexpected GameCoinBuy currency")
        return premium_per, coin_per
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError) as error:
        raise item_mgr.EconomyConfigError("invalid GameCoinBuy configuration") from error


def _costs(buyer_id, configured_costs):
    """合并全部礼物的成本，再一次性计算本局金币与菱石补差。"""
    costs = dict(configured_costs)
    game_coin_cost = costs.get(_GAME_COIN_ITEM_ID, 0)
    if game_coin_cost:
        premium_per, coin_per = _premium_rate()
        held = item_mgr.get_num(buyer_id, _GAME_COIN_ITEM_ID)
        direct = min(held, game_coin_cost)
        costs[_GAME_COIN_ITEM_ID] = direct
        shortfall = game_coin_cost - direct
        if shortfall:
            numerator = item_mgr.checked_mul(shortfall, premium_per)
            quotient, remainder = divmod(numerator, coin_per)
            premium = item_mgr.validate_count(
                quotient + (1 if remainder else 0), label="premium shortfall"
            )
            costs[_PREMIUM_ITEM_ID] = item_mgr.checked_add(
                costs.get(_PREMIUM_ITEM_ID, 0), premium
            )
    return tuple((item_id, amount) for item_id, amount in costs.items() if amount)


def _wish_snapshot(wish_lists, week_send_count):
    """在社交态锁内生成与 photo_share 兼容的事务快照。"""
    return copy.deepcopy(
        {
            "photos": ps.photos,
            "extra_upload_grid": ps.extra_upload_grid,
            "express_address": ps.express_address,
            "wish_lists": wish_lists,
            "week_send_count": week_send_count,
            "sp_state": ps.sp_state,
            "next_code": ps._next_code[0],
        }
    )


@packet_handler(MsgId.WishListBuyReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = WishListBuyReq()
        req.ParseFromString(data)

        rsp = WishListBuyRsp()
        rsp.player_id = req.player_id
        rsp.info.CopyFrom(req.info)
        buyer_notice = PackNotice(status=StatusCode.StatusCode_OK)
        target_notice = PackNotice(status=StatusCode.StatusCode_OK)
        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()
        buyer_id = session.player_id
        target_id = req.player_id
        replay = False
        pool_id = 0
        staged_wishes = None
        staged_counts = None

        try:
            with ExitStack() as stack:
                # 所有参与者按 player_id 升序持锁，反向送礼并发也不会死锁。
                for player_id in sorted({buyer_id, target_id}):
                    stack.enter_context(item_mgr.player_lock(player_id))
                # 与 photo_share 自动保存使用相同的社会态锁顺序，且先于 DB 锁。
                for state_lock in (
                    ps.lock_photo,
                    ps.lock_express,
                    ps.lock_wish,
                    ps.lock_sp,
                ):
                    stack.enter_context(state_lock)

                with db.transaction():
                    saved = activities._load_operation(
                        buyer_id, _SYSTEM, 0, packet_id, fingerprint
                    )
                    if saved == "conflict":
                        raise _Reject(StatusCode.StatusCode_BAD_REQ)
                    if saved is not None:
                        rsp.ParseFromString(saved[0])
                        buyer_notice.ParseFromString(saved[1])
                        replay = True
                    else:
                        if buyer_id <= 0 or target_id <= 0 or buyer_id == target_id:
                            raise _Reject(StatusCode.StatusCode_BAD_REQ)
                        for player_id in (buyer_id, target_id):
                            if not db.get_players_info(player_id, "player_id"):
                                raise _Reject(StatusCode.StatusCode_PLAYER_NOT_FOUND)

                        pool_id, configured_costs, rewards = _configuration(req)
                        key = (req.info.shop_id, req.info.grid_id)
                        wish = ps.wish_lists.get(target_id, {})
                        if key not in wish:
                            raise _Reject(StatusCode.StatusCode_WISH_LIST_NOT_EXIST)

                        costs = _costs(buyer_id, configured_costs)
                        if not item_mgr.consume_items(
                            buyer_id, costs, pack_notice=buyer_notice
                        ):
                            raise _Reject(StatusCode.StatusCode_ITEM_NOT_ENOUGH)
                        # 扣费与全部实例/物品发放嵌套在同一个 SQLite 事务中。
                        item_mgr.grant_items(
                            target_id, rewards, pack_notice=target_notice
                        )

                        staged_wishes = dict(ps.wish_lists)
                        target_wish = dict(wish)
                        del target_wish[key]
                        if target_wish:
                            staged_wishes[target_id] = target_wish
                        else:
                            staged_wishes.pop(target_id, None)
                        staged_counts = dict(ps.week_send_count)
                        current_count = item_mgr.validate_counter(
                            staged_counts.get(buyer_id, 0),
                            label="weekly wish send count",
                        )
                        new_count = item_mgr.checked_add(
                            current_count,
                            1,
                            limit=Config.ECONOMY_UINT32_MAX,
                        )
                        staged_counts[buyer_id] = new_count

                        rsp.status = StatusCode.StatusCode_OK
                        rsp.info.pool_id = pool_id
                        rsp.week_send_count = new_count
                        if not ps._snapshot_valid[0]:
                            raise RuntimeError("photo_share snapshot is not valid")
                        db.set_kv(
                            0,
                            *ps._PERSIST_KEY,
                            pickle.dumps(_wish_snapshot(staged_wishes, staged_counts)),
                        )
                        activities._save_operation(
                            buyer_id,
                            _SYSTEM,
                            0,
                            packet_id,
                            fingerprint,
                            rsp,
                            buyer_notice,
                        )

                # 仅在 SQLite 提交后同步内存，失败时不会留下已移除的心愿。
                if staged_wishes is not None:
                    ps.wish_lists.clear()
                    ps.wish_lists.update(staged_wishes)
                    ps.week_send_count.clear()
                    ps.week_send_count.update(staged_counts)
        except _Reject as error:
            rsp.Clear()
            rsp.status = error.status
            rsp.player_id = req.player_id
            rsp.info.CopyFrom(req.info)
            buyer_notice.Clear()
            target_notice.Clear()
        except item_mgr.EconomyConfigError:
            rsp.Clear()
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            rsp.player_id = req.player_id
            rsp.info.CopyFrom(req.info)
            buyer_notice.Clear()
            target_notice.Clear()

        session.send(MsgId.WishListBuyRsp, rsp, packet_id)
        if rsp.status == StatusCode.StatusCode_OK:
            session.send(MsgId.PackNotice, buyer_notice, 0)

        # 重放只恢复买家响应/背包结果，不重复推送受赠好友通知。
        if replay or rsp.status != StatusCode.StatusCode_OK:
            return
        for target_session in get_session():
            if target_session.player_id != target_id:
                continue
            notice = WishListBuyNotice(status=StatusCode.StatusCode_OK)
            notice.shop_id = req.info.shop_id
            notice.grids.id = req.info.shop_id
            notice.grids.grid_id = req.info.grid_id
            notice.grids.pool_id = pool_id
            target_session.send(MsgId.WishListBuyNotice, notice, 0)
            target_session.send(MsgId.PackNotice, target_notice, 0)
            break
