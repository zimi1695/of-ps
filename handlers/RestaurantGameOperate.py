import hashlib
import random

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId
from proto.net_pb2 import (
    RestaurantGameOperateReq,
    RestaurantGameOperateRsp,
    RestaurantGameOperateType,
    StatusCode,
    PackNotice,
)

import utils.db as db
from config import Config
from utils import item_mgr as items_util
from utils import restaurant_game_store as store
from server import activities

_SYSTEM = "restaurant-operate"
_T = RestaurantGameOperateType


class _Reject(Exception):
    def __init__(self, status):
        self.status = status


def _integer(value, *, minimum=0):
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or value > Config.ECONOMY_UINT32_MAX
    ):
        raise items_util.EconomyConfigError("invalid restaurant integer")
    return value


def _bags(activity_id):
    bags = store.get_desk_bags(activity_id)
    if not bags:
        raise items_util.EconomyConfigError("missing restaurant bags")
    result = {}
    for bag in bags:
        index = _integer(bag.get("bag_i_d"), minimum=1)
        if index in result:
            raise items_util.EconomyConfigError("duplicate restaurant bag")
        result[index] = bag
    return result


@packet_handler(MsgId.RestaurantGameOperateReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = RestaurantGameOperateReq()
        req.ParseFromString(data)

        rsp = RestaurantGameOperateRsp()
        rsp.status = StatusCode.StatusCode_OK
        rsp.activity_id = req.activity_id
        pack = PackNotice(status=StatusCode.StatusCode_OK)
        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()

        replay = False
        try:
            with (
                items_util.player_lock(session.player_id),
                items_util.deferred_notifications(),
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
                    replay = True
                else:
                    try:
                        bag_cfgs = _bags(req.activity_id)
                        coin_item = store.get_coin_item_id(req.activity_id)
                        items_util.validate_stackable_item_id(coin_item)
                        state = store.get_state(session.player_id, req.activity_id)
                        grids = state["grids"]
                        changed = []

                        def unlocked(index):
                            return index in bag_cfgs and store.grid_unlocked(
                                state, bag_cfgs[index]
                            )

                        if req.type == _T.RestaurantGameOperateType_Buy:
                            pool = store.get_pool_cfg(req.activity_id, req.buy_pool_id)
                            foods = [
                                _integer(group.get("food_id"), minimum=1)
                                for group in (pool or {}).get(
                                    "restaurant_random_pool_group_info", ()
                                )
                            ]
                            target = next(
                                (
                                    index
                                    for index in sorted(bag_cfgs)
                                    if grids.get(index, 0) == 0 and unlocked(index)
                                ),
                                0,
                            )
                            if not pool or not foods:
                                raise _Reject(StatusCode.StatusCode_CFG_NOT_FOUND)
                            if not target:
                                raise _Reject(StatusCode.StatusCode_FAIL)
                            if any(
                                store.get_food_cfg(req.activity_id, food) is None
                                for food in foods
                            ):
                                raise items_util.EconomyConfigError(
                                    "missing restaurant food config"
                                )
                            low = _integer(pool.get("rate_min", 0))
                            high = _integer(pool.get("rate_max", 0))
                            cost = random.randint(low, max(low, high))
                            if cost and not items_util.consume_items(
                                session.player_id,
                                [(coin_item, cost)],
                                pack_notice=pack,
                            ):
                                raise _Reject(StatusCode.StatusCode_ITEM_NOT_ENOUGH)
                            food = random.choice(foods)
                            grids[target] = food
                            changed.append((target, food))
                        elif req.type == _T.RestaurantGameOperateType_Mix:
                            if len(req.grid) != 2 or len(set(req.grid)) != 2:
                                raise _Reject(StatusCode.StatusCode_BAD_REQ)
                            if not all(unlocked(index) for index in req.grid):
                                raise _Reject(StatusCode.StatusCode_BAD_REQ)
                            foods = [grids.get(index, 0) for index in req.grid]
                            if not foods[0] or foods[0] != foods[1]:
                                raise _Reject(StatusCode.StatusCode_BAD_REQ)
                            food_cfg = store.get_food_cfg(req.activity_id, foods[0])
                            if not food_cfg:
                                raise items_util.EconomyConfigError(
                                    "missing restaurant food config"
                                )
                            result = (
                                _integer(food_cfg.get("synthetic_target"), minimum=1)
                                if food_cfg
                                else 0
                            )
                            if not result:
                                raise _Reject(
                                    StatusCode.StatusCode_MINIGAME_RESTAURANT_FOOD_MAX_LEVEL
                                )
                            special = _integer(
                                food_cfg.get("synthesis_special_food_i_d", 0)
                            )
                            if special:
                                weight = _integer(state.get("special_weight", 0))
                                if random.randint(1, 100) <= weight:
                                    result = special
                                    state["special_weight"] = _integer(
                                        store.get_global_int(
                                            "RestaurantGameBaseWeight", 10
                                        )
                                    )
                                else:
                                    increment = _integer(
                                        store.get_global_int(
                                            "RestaurantGameWeightNumber", 3
                                        )
                                    )
                                    limit = _integer(
                                        store.get_global_int(
                                            "RestaurantGameWeightLimit", 90
                                        )
                                    )
                                    state["special_weight"] = min(weight + increment, limit)
                            for index in req.grid:
                                grids[index] = 0
                                changed.append((index, 0))
                            target = req.grid[-1]
                            grids[target] = result
                            changed[-1] = (target, result)
                        elif req.type == _T.RestaurantGameOperateType_Recycle:
                            if not req.grid or len(set(req.grid)) != len(req.grid):
                                raise _Reject(StatusCode.StatusCode_BAD_REQ)
                            if not all(unlocked(index) for index in req.grid):
                                raise _Reject(StatusCode.StatusCode_BAD_REQ)
                            selected = []
                            for index in req.grid:
                                food = grids.get(index, 0)
                                if not food:
                                    raise _Reject(StatusCode.StatusCode_BAD_REQ)
                                cfg = store.get_food_cfg(req.activity_id, food)
                                if not cfg:
                                    raise items_util.EconomyConfigError(
                                        "missing restaurant food config"
                                    )
                                low = _integer(cfg.get("recycling_price_min", 0))
                                high = _integer(cfg.get("recycling_price_max", 0))
                                selected.append((index, low, max(low, high)))
                            refund = 0
                            for index, low, high in selected:
                                amount = random.randint(low, high)
                                if amount:
                                    refund = items_util.checked_add(refund, amount)
                                grids[index] = 0
                                changed.append((index, 0))
                            if refund:
                                items_util.grant_items(
                                    session.player_id,
                                    [(coin_item, refund)],
                                    rsp_items=rsp.item_details,
                                    pack_notice=pack,
                                )
                        else:
                            raise _Reject(StatusCode.StatusCode_BAD_REQ)

                        for index, food in changed:
                            grid = rsp.grid.add()
                            grid.index = index
                            grid.food = food
                            grid.is_un_lock = True
                        store.save_state(session.player_id, req.activity_id, state)
                        activities._save_operation(
                            session.player_id,
                            _SYSTEM,
                            0,
                            packet_id,
                            fingerprint,
                            rsp,
                            pack,
                        )
                    except (KeyError, TypeError, ValueError, OverflowError) as error:
                        raise items_util.EconomyConfigError(
                            "invalid restaurant state or config"
                        ) from error
        except _Reject as error:
            rsp.Clear()
            rsp.status = error.status
            rsp.activity_id = req.activity_id
            pack.Clear()
        except items_util.EconomyConfigError:
            rsp.Clear()
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            rsp.activity_id = req.activity_id
            pack.Clear()

        session.send(MsgId.RestaurantGameOperateRsp, rsp, packet_id)
        if pack.items:
            session.send(MsgId.PackNotice, pack, 0)
