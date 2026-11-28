import concurrent.futures
import pickle
import threading
from unittest.mock import patch

import pytest

from config import Config
from network.msg_id import MsgId
from proto.net_pb2 import (
    ItemDetail,
    PackNotice,
    StatusCode,
    WishListBuyReq,
    WishListBuyRsp,
    WishListInfo,
)
import utils.db as db
from utils import item_mgr, pb_create, res_loader
from handlers import WishListBuy as wish_buy
from server import photo_share as ps


OK = StatusCode.StatusCode_OK
BAD_REQ = StatusCode.StatusCode_BAD_REQ
NOT_ENOUGH = StatusCode.StatusCode_ITEM_NOT_ENOUGH
CFG_NOT_FOUND = StatusCode.StatusCode_CFG_NOT_FOUND
SHOP = 300
GRID = 1
POOL = 301
BUYER = 1
TARGET = 2
COIN = 101
GAME_COIN = 102
PREMIUM = 108
MATERIAL = 104
GIFT_A = 900001
GIFT_B = 900002
WEAPON = 900003


class FakeSession:
    def __init__(self, player_id):
        self.player_id = player_id
        self.sent = []
        self._send_lock = threading.Lock()

    def send(self, message_id, message, packet_id=0):
        with self._send_lock:
            self.sent.append((message_id, message, packet_id))

    def response(self, message_id):
        return [message for mid, message, _ in self.sent if mid == message_id][-1]

    def messages(self, message_id):
        return [message for mid, message, _ in self.sent if mid == message_id]

    def packs(self):
        return self.messages(MsgId.PackNotice)


def product(item_id=GIFT_A, num=1, currencies=None):
    return {
        "item_i_d": item_id,
        "item_num": num,
        "shop_currency_item": currencies
        if currencies is not None
        else [{"currency_i_d": COIN, "price": 10}],
    }


@pytest.fixture(scope="module", autouse=True)
def load_resources():
    res_loader.init()


@pytest.fixture(autouse=True)
def isolated_state(monkeypatch, tmp_path):
    old_path = Config.DB_PATH
    old_memory = Config.IN_MEMORY
    db.exit()
    Config.DB_PATH = str(tmp_path / "wish-list.db")
    Config.IN_MEMORY = False
    db.init()
    for player_id in (1, 2, 3, 4):
        db.db.execute(
            "INSERT INTO players(player_id, player_name, level) VALUES (?, ?, ?)",
            (player_id, f"wish-{player_id}", 10),
        )

    monkeypatch.setattr(ps, "wish_lists", {})
    monkeypatch.setattr(ps, "week_send_count", {})
    monkeypatch.setattr(ps, "_snapshot_valid", [True])
    for cache in ("_item_tags", "_character_ids", "_player_unlock_item_ids"):
        monkeypatch.setattr(item_mgr, cache, None)

    item_rows = [
        {"i_d": COIN, "new_bag_item_tag": 9},
        {"i_d": GAME_COIN, "new_bag_item_tag": 9},
        {"i_d": PREMIUM, "new_bag_item_tag": 9},
        {"i_d": MATERIAL, "new_bag_item_tag": 9},
        {"i_d": GIFT_A, "new_bag_item_tag": 9},
        {"i_d": GIFT_B, "new_bag_item_tag": 9},
        {"i_d": WEAPON, "new_bag_item_tag": 2},
    ]
    monkeypatch.setitem(res_loader.res, "Item", {"item": {"datas": item_rows}})
    monkeypatch.setitem(res_loader.res, "Character", {"character": {"datas": []}})
    monkeypatch.setitem(res_loader.res, "PlayerUnlock", {"player_unlock": {"datas": []}})
    monkeypatch.setitem(
        res_loader.res,
        "Weapon",
        {
            "weapon": {"datas": [{"i_d": WEAPON, "item_i_d": WEAPON}]},
            "weapon_property": {"datas": []},
        },
    )
    monkeypatch.setitem(
        res_loader.res,
        "GlobalConfig",
        {"config": {"datas": [{"key": "GameCoinBuy", "values": ("102", "5", "1")}]}},
    )
    monkeypatch.setitem(
        res_loader.res,
        "Shop",
        {
            "grid": {
                "datas": [
                    {"i_d": SHOP, "items": [{"grid_i_d": GRID, "shop_pool_i_d": POOL}]}
                ]
            },
            "pool": {"datas": [{"i_d": POOL, "items": [product()]}]},
        },
    )
    online = {player_id: FakeSession(player_id) for player_id in (1, 2, 3, 4)}
    monkeypatch.setattr(wish_buy, "get_session", lambda: list(online.values()))
    yield online
    db.exit()
    Config.DB_PATH = old_path
    Config.IN_MEMORY = old_memory


def set_amount(player_id, item_id, amount):
    detail = pb_create.make_item(item_id, amount, player_id)
    db.set_item_detail(player_id, detail.SerializeToString(), item_id)


def amount(player_id, item_id):
    return item_mgr.get_num(player_id, item_id)


def stored_amount(player_id, item_id):
    raw = db.get_item_detail(player_id, item_id)
    if not raw:
        return 0
    return ItemDetail.FromString(raw).main_item.base_item.num


def add_wish(player_id=TARGET, shop_id=SHOP, grid_id=GRID, pool_id=POOL):
    with ps.lock_wish:
        ps.wish_lists.setdefault(player_id, {})[(shop_id, grid_id)] = {
            "shop_id": shop_id,
            "grid_id": grid_id,
            "pool_id": pool_id,
            "create_time": 123,
        }


def set_products(products):
    res_loader.res["Shop"]["pool"]["datas"][0]["items"] = products


def invoke(
    packet_id=1,
    buyer_id=BUYER,
    target_id=TARGET,
    shop_id=SHOP,
    grid_id=GRID,
    pool_id=0,
):
    session = FakeSession(buyer_id)
    req = WishListBuyReq(
        player_id=target_id,
        info=WishListInfo(shop_id=shop_id, grid_id=grid_id, pool_id=pool_id),
    )
    wish_buy.Handler().handle(session, req.SerializeToString(), packet_id)
    return session.response(MsgId.WishListBuyRsp), session


def operation_keys():
    return db.get_kv_keys(BUYER, "activity_operations")


def test_single_player_success_removes_wish_and_notifies_target(isolated_state):
    add_wish()
    set_amount(BUYER, COIN, 100)

    rsp, buyer = invoke()

    assert rsp.status == OK
    assert rsp.player_id == TARGET
    assert rsp.info.pool_id == POOL
    assert rsp.week_send_count == 1
    assert amount(BUYER, COIN) == 90
    assert amount(TARGET, GIFT_A) == 1
    assert (SHOP, GRID) not in ps.wish_lists.get(TARGET, {})
    assert ps.week_send_count[BUYER] == 1
    assert len(buyer.packs()) == 1
    target = isolated_state[TARGET]
    assert len(target.messages(MsgId.WishListBuyNotice)) == 1
    assert target.messages(MsgId.WishListBuyNotice)[0].grids.pool_id == POOL
    assert len(target.packs()) == 1
    assert len(operation_keys()) == 1


def test_multiple_items_and_materials_use_aggregated_server_cost(isolated_state):
    add_wish()
    set_amount(BUYER, COIN, 100)
    set_amount(BUYER, MATERIAL, 2)
    set_amount(TARGET, GIFT_A, 5)
    set_products(
        [
            product(
                GIFT_A,
                2,
                [
                    {"currency_i_d": COIN, "price": 4},
                    {"currency_i_d": MATERIAL, "price": 2},
                ],
            ),
            product(GIFT_B, 3, [{"currency_i_d": COIN, "price": 6}]),
        ]
    )

    rsp, buyer = invoke()

    assert rsp.status == OK
    assert (amount(BUYER, COIN), amount(BUYER, MATERIAL)) == (90, 0)
    assert (amount(TARGET, GIFT_A), amount(TARGET, GIFT_B)) == (7, 3)
    assert {item.main_item.item_id for item in buyer.packs()[0].items} == {COIN, MATERIAL}
    assert {item.main_item.item_id for item in isolated_state[TARGET].packs()[0].items} == {
        GIFT_A,
        GIFT_B,
    }


def test_game_coin_shortfall_uses_premium_not_extra_game_coin():
    add_wish()
    set_amount(BUYER, GAME_COIN, 4)
    set_amount(BUYER, PREMIUM, 100)
    set_products(
        [
            product(GIFT_A, 1, [{"currency_i_d": GAME_COIN, "price": 3}]),
            product(GIFT_B, 2, [{"currency_i_d": GAME_COIN, "price": 3}]),
        ]
    )

    rsp, buyer = invoke()

    assert rsp.status == OK
    assert (amount(BUYER, GAME_COIN), amount(BUYER, PREMIUM)) == (0, 90)
    assert (amount(TARGET, GIFT_A), amount(TARGET, GIFT_B)) == (1, 2)
    charged = {item.main_item.item_id: item.main_item.base_item.num for item in buyer.packs()[0].items}
    assert charged == {GAME_COIN: 0, PREMIUM: 90}


def test_insufficient_material_or_premium_has_no_side_effects():
    add_wish()
    set_amount(BUYER, COIN, 100)
    set_amount(BUYER, MATERIAL, 1)
    set_products(
        [product(GIFT_A, 1, [{"price": 10}, {"currency_i_d": MATERIAL, "price": 2}])]
    )
    before = db.get_item_detail(BUYER, MATERIAL)

    rsp, buyer = invoke()

    assert rsp.status == NOT_ENOUGH
    assert buyer.packs() == []
    assert db.get_item_detail(BUYER, MATERIAL) == before
    assert amount(BUYER, COIN) == 100
    assert amount(TARGET, GIFT_A) == 0
    assert (SHOP, GRID) in ps.wish_lists[TARGET]
    assert ps.week_send_count.get(BUYER, 0) == 0
    assert operation_keys() == []


def test_insufficient_premium_shortfall_has_no_side_effects():
    add_wish()
    set_amount(BUYER, GAME_COIN, 1)
    set_amount(BUYER, PREMIUM, 4)
    set_products([product(GIFT_A, 1, [{"currency_i_d": GAME_COIN, "price": 2}])])

    rsp, buyer = invoke(11)

    assert rsp.status == NOT_ENOUGH
    assert buyer.packs() == []
    assert (amount(BUYER, GAME_COIN), amount(BUYER, PREMIUM)) == (1, 4)
    assert amount(TARGET, GIFT_A) == 0
    assert (SHOP, GRID) in ps.wish_lists[TARGET]
    assert ps.week_send_count.get(BUYER, 0) == 0
    assert operation_keys() == []


def test_same_fingerprint_replays_original_response_and_conflict_is_rejected(isolated_state):
    add_wish()
    set_amount(BUYER, COIN, 100)
    first, first_buyer = invoke(10)

    db.exit()
    db.init()
    replay, replay_buyer = invoke(10)
    conflict, conflict_buyer = invoke(10, target_id=3)
    replay_again, _ = invoke(10)

    assert first.status == OK
    assert replay.SerializeToString() == first.SerializeToString()
    assert replay_again.SerializeToString() == first.SerializeToString()
    assert replay_buyer.packs()[0].SerializeToString() == first_buyer.packs()[0].SerializeToString()
    assert conflict.status == BAD_REQ
    assert conflict_buyer.packs() == []
    assert amount(BUYER, COIN) == 90
    assert amount(TARGET, GIFT_A) == 1
    assert amount(3, GIFT_A) == 0
    assert ps.week_send_count[BUYER] == 1
    assert len(operation_keys()) == 1
    assert len(isolated_state[TARGET].messages(MsgId.WishListBuyNotice)) == 1


def test_concurrent_same_packet_applies_once(isolated_state):
    add_wish()
    set_amount(BUYER, COIN, 100)
    barrier = threading.Barrier(8)

    def call(_):
        barrier.wait()
        return invoke(20)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(call, range(8)))

    assert all(rsp.status == OK for rsp, _ in results)
    assert len({rsp.SerializeToString() for rsp, _ in results}) == 1
    assert amount(BUYER, COIN) == 90
    assert amount(TARGET, GIFT_A) == 1
    assert ps.week_send_count[BUYER] == 1
    assert len(operation_keys()) == 1
    assert len(isolated_state[TARGET].messages(MsgId.WishListBuyNotice)) == 1


def test_concurrent_distinct_packets_remove_wish_once():
    add_wish()
    set_amount(BUYER, COIN, 100)
    barrier = threading.Barrier(8)

    def call(index):
        barrier.wait()
        return invoke(index + 1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(call, range(8)))

    statuses = [rsp.status for rsp, _ in results]
    assert statuses.count(OK) == 1
    assert statuses.count(StatusCode.StatusCode_WISH_LIST_NOT_EXIST) == 7
    assert amount(BUYER, COIN) == 90
    assert amount(TARGET, GIFT_A) == 1
    assert ps.week_send_count[BUYER] == 1
    assert len(operation_keys()) == 1


def test_concurrent_reverse_gifts_lock_players_in_stable_order(isolated_state):
    add_wish(BUYER)
    add_wish(TARGET)
    set_amount(BUYER, COIN, 100)
    set_amount(TARGET, COIN, 100)
    barrier = threading.Barrier(2)

    def gift(buyer_id, target_id):
        barrier.wait()
        return invoke(30, buyer_id=buyer_id, target_id=target_id)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(gift, BUYER, TARGET), executor.submit(gift, TARGET, BUYER)]
        results = [future.result(timeout=10) for future in futures]

    assert all(rsp.status == OK for rsp, _ in results)
    assert (amount(BUYER, COIN), amount(TARGET, COIN)) == (90, 90)
    assert (amount(BUYER, GIFT_A), amount(TARGET, GIFT_A)) == (1, 1)
    assert ps.week_send_count == {BUYER: 1, TARGET: 1}
    assert (SHOP, GRID) not in ps.wish_lists.get(BUYER, {})
    assert (SHOP, GRID) not in ps.wish_lists.get(TARGET, {})


def test_missing_player_fails_without_charge():
    add_wish()
    set_amount(BUYER, COIN, 100)

    rsp, buyer = invoke(40, target_id=9999)

    assert rsp.status == StatusCode.StatusCode_PLAYER_NOT_FOUND
    assert buyer.packs() == []
    assert amount(BUYER, COIN) == 100
    assert operation_keys() == []


@pytest.mark.parametrize(
    "case",
    ("missing_grid", "missing_pool", "missing_item", "missing_currency", "negative_price", "bad_rate", "ambiguous_pool"),
)
def test_configuration_errors_fail_closed(case):
    add_wish()
    set_amount(BUYER, COIN, 100)
    set_amount(BUYER, MATERIAL, 10)
    set_products([product(GIFT_A, 1, [{"price": 10}, {"currency_i_d": MATERIAL, "price": 2}])])
    if case == "missing_grid":
        res_loader.res["Shop"]["grid"]["datas"] = []
    elif case == "missing_pool":
        res_loader.res["Shop"]["pool"]["datas"] = []
    elif case == "missing_item":
        res_loader.res["Item"]["item"]["datas"] = [
            row for row in res_loader.res["Item"]["item"]["datas"] if row["i_d"] != GIFT_A
        ]
    elif case == "missing_currency":
        res_loader.res["Item"]["item"]["datas"] = [
            row for row in res_loader.res["Item"]["item"]["datas"] if row["i_d"] != MATERIAL
        ]
    elif case == "negative_price":
        set_products([product(GIFT_A, 1, [{"price": -1}])])
    elif case == "bad_rate":
        res_loader.res["GlobalConfig"]["config"]["datas"][0]["values"] = ("102", "0", "1")
        set_products([product(GIFT_A, 1, [{"currency_i_d": GAME_COIN, "price": 1}])])
    else:
        res_loader.res["Shop"]["pool"]["datas"].append({"i_d": POOL, "items": [product()]})

    rsp, buyer = invoke(50)

    assert rsp.status == CFG_NOT_FOUND
    assert buyer.packs() == []
    assert (stored_amount(BUYER, COIN), stored_amount(BUYER, MATERIAL)) == (100, 10)
    assert stored_amount(TARGET, GIFT_A) == 0
    assert (SHOP, GRID) in ps.wish_lists[TARGET]
    assert ps.week_send_count.get(BUYER, 0) == 0
    assert operation_keys() == []


def test_later_grant_failure_rolls_back_charge_and_earlier_gift(monkeypatch):
    add_wish()
    set_amount(BUYER, COIN, 100)
    set_products([product(GIFT_A, 1), product(GIFT_B, 1)])
    original = db.set_item_detail

    def fail_second_gift(player_id, blob, item_id=None, instance_id=None):
        if player_id == TARGET and item_id == GIFT_B:
            raise OSError("second gift failed")
        return original(player_id, blob, item_id, instance_id)

    monkeypatch.setattr(db, "set_item_detail", fail_second_gift)
    with pytest.raises(OSError, match="second gift failed"):
        invoke(60)

    assert amount(BUYER, COIN) == 100
    assert (amount(TARGET, GIFT_A), amount(TARGET, GIFT_B)) == (0, 0)
    assert (SHOP, GRID) in ps.wish_lists[TARGET]
    assert ps.week_send_count.get(BUYER, 0) == 0
    assert operation_keys() == []
    assert db.get_kv(0, "photo_share", "snapshot") is None
    assert not db.conn.in_transaction


def test_instance_grant_success_uses_instance_table():
    add_wish()
    set_amount(BUYER, COIN, 100)
    set_products([product(WEAPON, 1)])

    rsp, _ = invoke(70)

    assert rsp.status == OK
    rows = db.db.execute(
        "SELECT item_detail_blob FROM items_s WHERE player_id=?", (TARGET,)
    ).fetchall()
    assert len(rows) == 1
    detail = ItemDetail.FromString(rows[0][0])
    assert detail.main_item.weapon.weapon_id == WEAPON
    assert detail.main_item.weapon.instance_id == 1
    assert db.get_item_detail(TARGET, WEAPON) is None
    assert db.db.execute("SELECT instance_id FROM instance WHERE player_id=?", (TARGET,)).fetchone()[0] == 1


def test_instance_grant_is_rolled_back_when_later_item_fails(monkeypatch):
    add_wish()
    set_amount(BUYER, COIN, 100)
    set_products([product(WEAPON, 1), product(GIFT_B, 1)])
    original = db.set_item_detail

    def fail_stackable(player_id, blob, item_id=None, instance_id=None):
        if player_id == TARGET and item_id == GIFT_B:
            raise OSError("stackable failed")
        return original(player_id, blob, item_id, instance_id)

    monkeypatch.setattr(db, "set_item_detail", fail_stackable)
    with pytest.raises(OSError, match="stackable failed"):
        invoke(80)

    assert amount(BUYER, COIN) == 100
    assert db.db.execute("SELECT COUNT(*) FROM items_s WHERE player_id=?", (TARGET,)).fetchone()[0] == 0
    assert db.db.execute("SELECT COUNT(*) FROM instance WHERE player_id=?", (TARGET,)).fetchone()[0] == 0
    assert amount(TARGET, GIFT_B) == 0
    assert (SHOP, GRID) in ps.wish_lists[TARGET]
    assert operation_keys() == []


def test_idempotency_write_failure_rolls_back_all_state(monkeypatch):
    add_wish()
    set_amount(BUYER, COIN, 100)
    original = db.set_kv

    def fail_operation(player_id, ns, key, value):
        if ns == "activity_operations":
            raise OSError("operation write failed")
        return original(player_id, ns, key, value)

    monkeypatch.setattr(db, "set_kv", fail_operation)
    with pytest.raises(OSError, match="operation write failed"):
        invoke(90)

    assert amount(BUYER, COIN) == 100
    assert amount(TARGET, GIFT_A) == 0
    assert (SHOP, GRID) in ps.wish_lists[TARGET]
    assert ps.week_send_count.get(BUYER, 0) == 0
    assert operation_keys() == []
    assert db.get_kv(0, "photo_share", "snapshot") is None


def test_request_uses_one_transaction_and_persists_wish_state():
    add_wish()
    set_amount(BUYER, COIN, 100)
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        rsp, _ = invoke(100)
    finally:
        db.conn.set_trace_callback(None)

    assert rsp.status == OK
    assert sum(statement == "BEGIN IMMEDIATE" for statement in statements) == 1
    snapshot = pickle.loads(db.get_kv(0, "photo_share", "snapshot"))
    assert (SHOP, GRID) not in snapshot["wish_lists"].get(TARGET, {})
    assert snapshot["week_send_count"][BUYER] == 1
    assert snapshot["wish_lists"] == ps.wish_lists
    assert snapshot["week_send_count"] == ps.week_send_count
