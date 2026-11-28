import concurrent.futures
import dataclasses
import threading
import time
from unittest.mock import Mock

import pytest

from config import Config
from network.msg_id import MsgId
from proto.net_pb2 import (
    BuyGameCoinReq,
    BuyGameCoinRsp,
    ItemDetail,
    PackNotice,
    PlayerEnergyBuyReq,
    PlayerEnergyBuyRsp,
    PlayerVitalityBuyReq,
    PlayerVitalityBuyRsp,
    StatusCode,
)

import utils.db as db
from utils import achieve_track, item_mgr, refresh, res_loader
from handlers import BuyGameCoin as coin
from handlers import PlayerEnergyBuy as energy
from handlers import PlayerVitalityBuy as vitality


PLAYER = 77
NOW = 1_800_000_000
CURRENCY = 102
OK = StatusCode.StatusCode_OK
BAD_REQ = StatusCode.StatusCode_BAD_REQ
NOT_ENOUGH = StatusCode.StatusCode_ITEM_NOT_ENOUGH


@dataclasses.dataclass(frozen=True)
class Purchase:
    name: str
    module: object
    request_type: object
    response_id: int
    response_type: object
    item_id: int
    amount: int
    maximum: int
    cost: int
    second_cost: int
    count_prefix: str
    full_status: int


PURCHASES = (
    Purchase(
        "coin", coin, BuyGameCoinReq, MsgId.BuyGameCoinRsp, BuyGameCoinRsp,
        Config.GAME_COIN_ITEM_ID, 1, 10, 5, 5, "", StatusCode.StatusCode_FAIL,
    ),
    Purchase(
        "energy", energy, PlayerEnergyBuyReq, MsgId.PlayerEnergyBuyRsp,
        PlayerEnergyBuyRsp, Config.ENERGY_ITEM_ID, 20, 100, 10, 15, "energy",
        StatusCode.StatusCode_FAIL,
    ),
    Purchase(
        "vitality", vitality, PlayerVitalityBuyReq, MsgId.PlayerVitalityBuyRsp,
        PlayerVitalityBuyRsp, Config.VITALITY_ITEM_ID, 30, 100, 12, 18,
        "vitality", StatusCode.StatusCode_VITALITY_FULL,
    ),
)
COIN = PURCHASES[0]
COUNTED = PURCHASES[1:]


class FakeSession:
    def __init__(self):
        self.player_id = PLAYER
        self.scene_id = 1
        self.sent = []

    def send(self, message_id, message, packet_id=0):
        self.sent.append((message_id, message, packet_id))

    def response(self, message_id):
        return [message for mid, message, _ in self.sent if mid == message_id][-1]

    def packs(self):
        return [message for mid, message, _ in self.sent if mid == MsgId.PackNotice]


@pytest.fixture(scope="module", autouse=True)
def load_resources():
    res_loader.init()


@pytest.fixture(autouse=True)
def database(monkeypatch, tmp_path):
    old_path = Config.DB_PATH
    old_memory = Config.IN_MEMORY
    db.exit()
    Config.DB_PATH = str(tmp_path / "purchase.db")
    Config.IN_MEMORY = False
    db.init()
    db.db.execute(
        "INSERT INTO players(player_id, player_name, level) VALUES (?, ?, ?)",
        (PLAYER, "purchase-test", 10),
    )
    monkeypatch.setattr(time, "time", lambda: NOW)
    report = Mock()
    monkeypatch.setattr(achieve_track, "report", report)
    yield report
    db.exit()
    Config.DB_PATH = old_path
    Config.IN_MEMORY = old_memory


@pytest.fixture(autouse=True)
def resources(monkeypatch):
    monkeypatch.setitem(res_loader.res, "GlobalConfig", {"config": {"datas": (
        {"key": "GameCoinBuy", "values": ("102", "5", "1")},
        {"key": "GameCoinMax", "values": ("10",)},
        {"key": "GameCoinIni", "values": ("0",)},
        {"key": "GameCoinTimeRecover", "values": ("3600",)},
        {"key": "Player_Energy_Buy_Times", "values": ("102:10:20", "102:15:20")},
        {"key": "Player_Vitality_Buy_Times", "values": ("102:12:30", "102:18:30")},
    )}})
    monkeypatch.setitem(res_loader.res, "Level", {"player_level": {"datas": (
        {"i_d": 10, "player_energy": 100, "player_vitality": 100},
    )}})


def invoke(spec, packet_id, buy_count=1):
    session = FakeSession()
    request = spec.request_type(buy_count=buy_count) if spec is COIN else spec.request_type()
    spec.module.Handler().handle(session, request.SerializeToString(), packet_id)
    return session.response(spec.response_id), session.packs()


def store_amount(item_id, amount):
    detail = ItemDetail()
    raw = db.get_item_detail(PLAYER, item_id)
    if raw:
        detail.ParseFromString(raw)
    else:
        item_mgr.make_item(item_id, 0, PLAYER, detail)
    detail.main_item.base_item.num = amount
    db.set_item_detail(PLAYER, detail.SerializeToString(), item_id)


def amount(item_id):
    return item_mgr.get_num(PLAYER, item_id)


def count(spec):
    if spec is COIN:
        return None
    key = f"{spec.count_prefix}:{refresh.day_index()}"
    return int(db.get_kv(PLAYER, "buy", key, 0) or 0)


def operations():
    return db.get_kv_keys(PLAYER, "activity_operations")


def prepare(spec, currency=100):
    store_amount(CURRENCY, currency)
    store_amount(spec.item_id, 0)
    if spec is COIN:
        db.set_kv(PLAYER, "game_coin", "last_recover_ts", NOW)


@pytest.mark.parametrize("spec", PURCHASES, ids=lambda spec: spec.name)
def test_success_and_same_fingerprint_replay(spec, database):
    prepare(spec)
    first, first_packs = invoke(spec, 101)
    replay, replay_packs = invoke(spec, 101)

    assert first.status == OK
    assert first.SerializeToString() == replay.SerializeToString()
    assert len(first_packs) == len(replay_packs) == 1
    assert first_packs[0].SerializeToString() == replay_packs[0].SerializeToString()
    assert amount(CURRENCY) == 100 - spec.cost
    assert amount(spec.item_id) == spec.amount
    assert count(spec) == (None if spec is COIN else 1)
    assert len(operations()) == 1
    events = [call.args[1] for call in database.call_args_list]
    assert events.count("energy_restore") == (1 if spec.name == "energy" else 0)
    assert events.count("vitality_restore") == (1 if spec.name == "vitality" else 0)


def test_coin_packet_id_with_different_fingerprint_conflicts():
    prepare(COIN)
    first, _ = invoke(COIN, 201, buy_count=1)
    conflict, packs = invoke(COIN, 201, buy_count=2)
    replay, replay_packs = invoke(COIN, 201, buy_count=1)

    assert first.status == OK
    assert conflict.status == BAD_REQ
    assert packs == []
    assert replay.SerializeToString() == first.SerializeToString()
    assert amount(CURRENCY) == 95
    assert amount(COIN.item_id) == 1


@pytest.mark.parametrize("spec", PURCHASES, ids=lambda spec: spec.name)
def test_concurrent_same_packet_replays_once(spec):
    prepare(spec, currency=1000)
    barrier = threading.Barrier(8)

    def call(_):
        barrier.wait()
        return invoke(spec, 301)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(call, range(8)))

    assert all(rsp.status == OK for rsp, _ in results)
    assert len({rsp.SerializeToString() for rsp, _ in results}) == 1
    assert len({packs[0].SerializeToString() for _, packs in results}) == 1
    assert amount(CURRENCY) == 1000 - spec.cost
    assert amount(spec.item_id) == spec.amount
    assert count(spec) == (None if spec is COIN else 1)
    assert len(operations()) == 1


@pytest.mark.parametrize("spec", PURCHASES, ids=lambda spec: spec.name)
def test_concurrent_distinct_packets_apply_both_purchases(spec):
    prepare(spec, currency=1000)
    if spec is not COIN:
        store_amount(spec.item_id, 0)
    barrier = threading.Barrier(2)

    def call(packet_id):
        barrier.wait()
        return invoke(spec, packet_id)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(call, (302, 303)))

    assert all(rsp.status == OK for rsp, _ in results)
    assert amount(CURRENCY) == 1000 - spec.cost - spec.second_cost
    assert amount(spec.item_id) == spec.amount * 2
    assert count(spec) == (None if spec is COIN else 2)
    assert len(operations()) == 2


@pytest.mark.parametrize("spec", PURCHASES, ids=lambda spec: spec.name)
def test_capacity_truncates_charge_and_rejects_next_purchase(spec):
    prepare(spec)
    store_amount(spec.item_id, spec.maximum - 1)
    first, _ = invoke(spec, 401, buy_count=5)
    full, full_packs = invoke(spec, 402)

    assert first.status == OK
    assert amount(CURRENCY) == 100 - spec.cost
    assert amount(spec.item_id) == spec.maximum
    assert full.status == spec.full_status
    assert full_packs == []
    assert amount(CURRENCY) == 100 - spec.cost
    assert count(spec) == (None if spec is COIN else 1)


@pytest.mark.parametrize("spec", PURCHASES, ids=lambda spec: spec.name)
def test_insufficient_currency_has_no_side_effects(spec):
    prepare(spec, currency=spec.cost - 1)
    rsp, packs = invoke(spec, 501)

    assert rsp.status == NOT_ENOUGH
    assert packs == []
    assert amount(CURRENCY) == spec.cost - 1
    assert amount(spec.item_id) == 0
    assert count(spec) == (None if spec is COIN else 0)
    assert operations() == []


@pytest.mark.parametrize("spec", PURCHASES, ids=lambda spec: spec.name)
def test_balance_write_failure_rolls_back_charge(spec, monkeypatch):
    prepare(spec)
    original = db.set_item_detail

    def fail_balance(player_id, blob, item_id=None, instance_id=None):
        if item_id == spec.item_id:
            raise OSError("balance write failed")
        return original(player_id, blob, item_id, instance_id)

    monkeypatch.setattr(db, "set_item_detail", fail_balance)
    with pytest.raises(OSError, match="balance write failed"):
        invoke(spec, 601)

    assert amount(CURRENCY) == 100
    assert amount(spec.item_id) == 0
    assert count(spec) == (None if spec is COIN else 0)
    assert operations() == []


@pytest.mark.parametrize("spec", COUNTED, ids=lambda spec: spec.name)
def test_purchase_count_write_failure_rolls_back_everything(spec, monkeypatch):
    prepare(spec)
    monkeypatch.setattr(db, "incr_kv", Mock(side_effect=OSError("count write failed")))
    with pytest.raises(OSError, match="count write failed"):
        invoke(spec, 602)

    assert amount(CURRENCY) == 100
    assert amount(spec.item_id) == 0
    assert count(spec) == 0
    assert operations() == []


@pytest.mark.parametrize("spec", PURCHASES, ids=lambda spec: spec.name)
def test_idempotency_write_failure_rolls_back_everything(spec, monkeypatch):
    prepare(spec)
    if spec.name == "coin":
        db.set_kv(PLAYER, "game_coin", "last_recover_ts", NOW - 7200)
    monkeypatch.setattr(
        coin.activities, "_save_operation", Mock(side_effect=OSError("operation write failed"))
    )
    with pytest.raises(OSError, match="operation write failed"):
        invoke(spec, 603)

    assert amount(CURRENCY) == 100
    assert amount(spec.item_id) == 0
    assert count(spec) == (None if spec is COIN else 0)
    assert operations() == []
    if spec.name == "coin":
        assert db.get_kv(PLAYER, "game_coin", "last_recover_ts") == NOW - 7200


@pytest.mark.parametrize("spec", PURCHASES, ids=lambda spec: spec.name)
def test_zero_packet_id_remains_supported_without_idempotency_record(spec):
    prepare(spec)
    rsp, packs = invoke(spec, 0)

    assert rsp.status == OK
    assert len(packs) == 1
    assert amount(CURRENCY) == 100 - spec.cost
    assert amount(spec.item_id) == spec.amount
    assert operations() == []


@pytest.mark.parametrize("spec", PURCHASES, ids=lambda spec: spec.name)
def test_request_uses_one_sqlite_transaction(spec):
    prepare(spec)
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        rsp, _ = invoke(spec, 701)
    finally:
        db.conn.set_trace_callback(None)

    assert rsp.status == OK
    assert sum(statement == "BEGIN IMMEDIATE" for statement in statements) == 1
