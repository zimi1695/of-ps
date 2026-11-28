import concurrent.futures
import dataclasses
import threading
from unittest.mock import Mock, patch

import pytest

from config import Config
from network.msg_id import MsgId
from proto.net_pb2 import GachaReq, PackNotice, ShopBuyReq, StatusCode

import utils.db as db
from handlers import Gacha as gacha
from handlers import ShopBuy as shop
from server import activities
from utils import achieve_track, item_mgr, res_loader


PLAYER = 1
OK = StatusCode.StatusCode_OK
BAD_REQ = StatusCode.StatusCode_BAD_REQ
NOT_ENOUGH = StatusCode.StatusCode_ITEM_NOT_ENOUGH


class FakeSession:
    def __init__(self):
        self.player_id = PLAYER
        self.sent = []

    def send(self, message_id, message, packet_id=0):
        saved = type(message)()
        saved.CopyFrom(message)
        self.sent.append((message_id, saved, packet_id))

    def response(self, message_id):
        return [message for mid, message, _ in self.sent if mid == message_id][-1]

    def packs(self):
        return [message for mid, message, _ in self.sent if mid == MsgId.PackNotice]


@dataclasses.dataclass(frozen=True)
class HandlerCase:
    name: str
    module: object
    request: object
    response_id: int
    altered_request: object
    system: str
    cost: int
    cost_count: int
    reward: int


CASES = (
    HandlerCase(
        "shop", shop, ShopBuyReq(shop_id=1, grid_id=1, buy_times=1, item_id=104),
        MsgId.ShopBuyRsp, ShopBuyReq(shop_id=1, grid_id=1, buy_times=2, item_id=104),
        "shop-buy", 101, 10, 104,
    ),
    HandlerCase(
        "gacha", gacha, GachaReq(gacha_id=10, is_single=True),
        MsgId.GachaRsp, GachaReq(gacha_id=10, is_single=False),
        "gacha", 104, 1, 101,
    ),
)


def resources(case, *, shop_limit=-1, gacha_cap=2):
    if case.name == "shop":
        return {"Shop": {
            "grid": {"datas": [{
                "i_d": 1,
                "items": [{
                    "grid_i_d": 1, "shop_pool_i_d": 2,
                    "buy_max_count": shop_limit,
                }],
            }]},
            "pool": {"datas": [{
                "i_d": 2,
                "items": [{
                    "index": 1, "item_i_d": 104, "item_num": 1,
                    "shop_currency_item": [{"currency_i_d": 101, "price": 10}],
                }],
            }]},
            "increasing": {"datas": []},
        }}
    return {"Gacha": {
        "info": {"datas": [{
            "i_d": 10, "max_times": gacha_cap, "big_guarantee_pool_i_d": 22,
            "consume_item1_i_d": 104, "method1_count": 1, "consume_item1_num": 1,
            "consume_item2_i_d": 104, "consume_item2_num": 10,
        }]},
        "pool": {"datas": [{"i_d": 10, "items": [{"free_gacha_pool_i_d": 21}]}]},
        "reward_pool": {"datas": [
            {"i_d": 21, "items": [{"item_i_d": 101}]},
            {"i_d": 22, "items": [{"item_i_d": 102}]},
        ]},
    }}


@pytest.fixture(scope="module", autouse=True)
def load_resources():
    res_loader.init()


@pytest.fixture(autouse=True)
def game_db(tmp_path, monkeypatch):
    old_path, old_memory = Config.DB_PATH, Config.IN_MEMORY
    db.exit()
    Config.DB_PATH = str(tmp_path / "idempotency.db")
    Config.IN_MEMORY = False
    db.init()
    db.db.execute(
        "INSERT INTO players(player_id,player_name,level,world_level,unlock_functions) "
        "VALUES(?,?,10,1,?)",
        (PLAYER, "idempotency-test", b""),
    )
    monkeypatch.setattr(Config, "SKIP_QUESTS", True)
    monkeypatch.setattr(achieve_track, "report", Mock())
    yield
    db.exit()
    Config.DB_PATH, Config.IN_MEMORY = old_path, old_memory


def amount(item_id):
    return item_mgr.get_num(PLAYER, item_id)


def snapshot():
    tables = [row[0] for row in db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )]
    return tuple(
        (table, tuple(db.conn.execute(f'SELECT * FROM "{table}" ORDER BY rowid')))
        for table in tables
    )


def invoke(case, packet_id, request=None, session=None):
    session = session or FakeSession()
    request = case.request if request is None else request
    with patch.dict(res_loader.res, resources(case)), patch.object(
        gacha.random, "choice", side_effect=lambda values: values[0]
    ):
        case.module.Handler().handle(session, request.SerializeToString(), packet_id)
    return session.response(case.response_id), session.packs()


def concurrently(calls):
    barrier = threading.Barrier(len(calls))

    def call(function):
        barrier.wait()
        return function()

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(calls)) as executor:
        return list(executor.map(call, calls))


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_success_saves_original_response_and_pack(case):
    item_mgr.grant_items(PLAYER, [(case.cost, 100)])
    rsp, packs = invoke(case, 101)

    assert rsp.status == OK
    assert len(packs) == 1
    assert packs[0].status == OK
    assert amount(case.cost) == 100 - case.cost_count
    assert amount(case.reward) == 1
    assert db.get_kv_keys(PLAYER, "activity_operations") == [f"{case.system}:0:101"]


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_same_fingerprint_replays_without_random_or_side_effects(case):
    item_mgr.grant_items(PLAYER, [(case.cost, 100)])
    first, first_packs = invoke(case, 102)
    committed = snapshot()
    choice = Mock(side_effect=lambda values: values[0])
    achieve_track.report.reset_mock()

    with patch.dict(res_loader.res, resources(case)), patch.object(gacha.random, "choice", choice):
        replay_session = FakeSession()
        case.module.Handler().handle(replay_session, case.request.SerializeToString(), 102)
    replay = replay_session.response(case.response_id)

    assert replay.SerializeToString() == first.SerializeToString()
    assert len(replay_session.packs()) == 1
    assert replay_session.packs()[0].SerializeToString() == first_packs[0].SerializeToString()
    assert snapshot() == committed
    choice.assert_not_called()
    achieve_track.report.assert_not_called()


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_same_packet_different_fingerprint_conflicts(case):
    item_mgr.grant_items(PLAYER, [(case.cost, 100)])
    first, _ = invoke(case, 103)
    committed = snapshot()
    conflict, packs = invoke(case, 103, case.altered_request)
    replay, replay_packs = invoke(case, 103)

    assert first.status == OK
    assert conflict.status == BAD_REQ
    assert packs == []
    assert replay.SerializeToString() == first.SerializeToString()
    assert replay_packs[0].SerializeToString()
    assert snapshot() == committed


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_concurrent_same_packet_applies_once(case):
    item_mgr.grant_items(PLAYER, [(case.cost, 100)])
    choice = Mock(side_effect=lambda values: values[0])
    sessions = [FakeSession() for _ in range(6)]

    def call(session):
        with patch.dict(res_loader.res, resources(case)), patch.object(gacha.random, "choice", choice):
            case.module.Handler().handle(session, case.request.SerializeToString(), 104)

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        list(executor.map(call, sessions))
    responses = [session.response(case.response_id) for session in sessions]

    assert [rsp.status for rsp in responses] == [OK] * 6
    assert len({rsp.SerializeToString() for rsp in responses}) == 1
    assert len({session.packs()[0].SerializeToString() for session in sessions}) == 1
    assert amount(case.cost) == 100 - case.cost_count
    assert amount(case.reward) == 1
    assert choice.call_count == (1 if case.name == "gacha" else 0)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_concurrent_distinct_packets_apply_both(case):
    item_mgr.grant_items(PLAYER, [(case.cost, 100)])
    sessions = [FakeSession() for _ in range(2)]

    def call(index):
        session = sessions[index]
        with patch.dict(res_loader.res, resources(case)), patch.object(
            gacha.random, "choice", side_effect=lambda values: values[0]
        ):
            case.module.Handler().handle(session, case.request.SerializeToString(), 105 + index)

    concurrently([lambda index=index: call(index) for index in range(2)])

    assert [session.response(case.response_id).status for session in sessions] == [OK, OK]
    assert amount(case.cost) == 100 - 2 * case.cost_count
    if case.name == "gacha":
        assert db.get_total_gacha_num(PLAYER, 10) == 2
    else:
        assert amount(case.reward) == 2
    assert len(db.get_kv_keys(PLAYER, "activity_operations")) == 2


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_same_packet_is_isolated_by_player(case):
    db.db.execute(
        "INSERT INTO players(player_id,player_name,level,world_level,unlock_functions) "
        "VALUES(2,?,10,1,?)",
        ("idempotency-player-2", b""),
    )
    item_mgr.grant_items(PLAYER, [(case.cost, 100)])
    item_mgr.grant_items(2, [(case.cost, 100)])
    assert invoke(case, 115)[0].status == OK
    session = FakeSession()
    session.player_id = 2
    with patch.dict(res_loader.res, resources(case)), patch.object(
        gacha.random, "choice", side_effect=lambda values: values[0]
    ):
        case.module.Handler().handle(session, case.request.SerializeToString(), 115)

    assert session.response(case.response_id).status == OK
    assert amount(case.cost) == 100 - case.cost_count
    assert item_mgr.get_num(2, case.cost) == 100 - case.cost_count
    assert set(db.get_kv_keys(PLAYER, "activity_operations")) == {f"{case.system}:0:115"}
    assert db.get_kv_keys(2, "activity_operations") == [f"{case.system}:0:115"]


def test_same_packet_is_isolated_by_message_system():
    item_mgr.grant_items(PLAYER, [(101, 100)])
    item_mgr.grant_items(PLAYER, [(104, 100)])
    assert invoke(CASES[0], 116)[0].status == OK
    assert invoke(CASES[1], 116)[0].status == OK

    assert amount(101) == 91
    assert amount(104) == 100
    assert set(db.get_kv_keys(PLAYER, "activity_operations")) == {
        "shop-buy:0:116", "gacha:0:116",
    }


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_insufficient_material_has_no_side_effects_or_idempotency_record(case):
    item_mgr.grant_items(PLAYER, [(case.cost, case.cost_count - 1)])
    before = snapshot()
    rsp, packs = invoke(case, 106)

    assert rsp.status == NOT_ENOUGH
    assert packs == []
    assert snapshot() == before
    assert db.get_kv_keys(PLAYER, "activity_operations") == []


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_zero_packet_id_remains_usable_without_idempotency(case):
    item_mgr.grant_items(PLAYER, [(case.cost, 100)])
    first, first_packs = invoke(case, 0)
    second, second_packs = invoke(case, 0)

    assert first.status == second.status == OK
    assert len(first_packs) == len(second_packs) == 1
    assert amount(case.cost) == 100 - 2 * case.cost_count
    if case.name == "gacha":
        assert db.get_total_gacha_num(PLAYER, 10) == 2
    else:
        assert amount(case.reward) == 2
    assert db.get_kv_keys(PLAYER, "activity_operations") == []


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
@pytest.mark.parametrize("failure", ("operation", "state"))
def test_save_failure_rolls_back_everything(case, failure, monkeypatch):
    item_mgr.grant_items(PLAYER, [(case.cost, 100)])
    before = snapshot()
    session = FakeSession()
    with monkeypatch.context() as patcher:
        if failure == "operation":
            target, attr, message = activities, "_save_operation", "operation save failed"
        elif case.name == "shop":
            target, attr, message = db, "set_kv", "purchase state save failed"
        else:
            target, attr, message = db, "set_gacha_guarantee", "guarantee save failed"
        original = getattr(target, attr)

        def fail(*args, **kwargs):
            original(*args, **kwargs)
            raise OSError(message)

        patcher.setattr(target, attr, fail)
        with patch.dict(res_loader.res, resources(case)), patch.object(
            gacha.random, "choice", side_effect=lambda values: values[0]
        ), pytest.raises(OSError, match="save failed"):
            case.module.Handler().handle(session, case.request.SerializeToString(), 107)

    assert snapshot() == before
    assert session.sent == []
    assert invoke(case, 107)[0].status == OK


def test_gacha_random_failure_rolls_back_without_charge():
    case = CASES[1]
    item_mgr.grant_items(PLAYER, [(case.cost, 100)])
    before = snapshot()
    session = FakeSession()
    with patch.dict(res_loader.res, resources(case)), patch.object(
        gacha.random, "choice", side_effect=RuntimeError("random failed")
    ), pytest.raises(RuntimeError, match="random failed"):
        gacha.Handler().handle(session, case.request.SerializeToString(), 108)

    assert snapshot() == before
    assert session.sent == []
    assert invoke(case, 108)[0].status == OK


def test_shop_purchase_limit_is_preserved_for_distinct_packets():
    case = CASES[0]
    item_mgr.grant_items(PLAYER, [(case.cost, 100)])
    assert invoke(case, 109)[0].status == OK
    committed = snapshot()
    with patch.dict(res_loader.res, resources(case, shop_limit=1)):
        session = FakeSession()
        shop.Handler().handle(session, case.request.SerializeToString(), 110)
    rsp = session.response(case.response_id)

    assert rsp.status == StatusCode.StatusCode_SHOP_GRID_BUY_NUM_LIMIT
    assert session.packs() == []
    assert snapshot() == committed


def test_gacha_guarantee_and_ten_pull_are_preserved():
    case = CASES[1]
    item_mgr.grant_items(PLAYER, [(case.cost, 100)])
    db.set_gacha_guarantee(PLAYER, 10, 1)
    first, _ = invoke(case, 111)
    assert first.status == OK
    assert [item.main_item.item_id for item in first.items] == [102]
    assert db.get_gacha_guarantee(PLAYER, 10) == 0

    db.db.execute("DELETE FROM gacha_record WHERE player_id=?", (PLAYER,))
    db.set_gacha_guarantee(PLAYER, 10, 0)
    request = GachaReq(gacha_id=10, is_single=False)
    with patch.dict(res_loader.res, resources(case, gacha_cap=3)), patch.object(
        gacha.random, "choice", side_effect=lambda values: values[0]
    ):
        session = FakeSession()
        gacha.Handler().handle(session, request.SerializeToString(), 112)
    rsp = session.response(MsgId.GachaRsp)
    rewards = [item.main_item.item_id for item in rsp.items]

    assert rsp.status == OK
    assert rewards.count(101) == 7
    assert rewards.count(102) == 3
    assert db.get_gacha_guarantee(PLAYER, 10) == 1
    assert amount(case.cost) == 89
