import concurrent.futures
import dataclasses
import pickle
import threading

import pytest

# Keep the repository's established db -> protobuf import order.
import utils.db as db
from config import Config
from network.msg_id import MsgId
from proto.net_pb2 import (
    CollectMoonReq,
    CommonParam,
    GenericGameAReq,
    ItemDetail,
    StatusCode,
)
import handlers.CollectMoon as collect_moon
import handlers.GenericGameA as generic_game_a
from server import activities
from utils import achieve_track, item_mgr, res_loader


PLAYER = 1
MOON = 11001001
OTHER_MOON = 11001002
OUTFIT = 4100292
CURRENCY = 102
MOON_REWARD = 124
OK = StatusCode.StatusCode_OK
BAD_REQ = StatusCode.StatusCode_BAD_REQ
CFG_NOT_FOUND = StatusCode.StatusCode_CFG_NOT_FOUND
NOT_ENOUGH = StatusCode.StatusCode_ITEM_NOT_ENOUGH


class FakeSession:
    def __init__(self):
        self.player_id = PLAYER
        self.scene_id = 1
        self.channel_id = PLAYER
        self.color_data = [None]
        self.sent = []

    def send(self, message_id, message, packet_id=0):
        saved = type(message)()
        saved.CopyFrom(message)
        self.sent.append((message_id, saved, packet_id))

    def response(self, message_id):
        return [message for mid, message, _ in self.sent if mid == message_id][-1]

    def packs(self):
        return [message for mid, message, _ in self.sent if mid == MsgId.PackNotice]


@pytest.fixture(scope="module", autouse=True)
def resources():
    res_loader.init()


@pytest.fixture(autouse=True)
def database(tmp_path, monkeypatch):
    old_path, old_memory = Config.DB_PATH, Config.IN_MEMORY
    db.exit()
    Config.DB_PATH = str(tmp_path / "moon-dye.db")
    Config.IN_MEMORY = False
    db.init()
    db.db.execute(
        "INSERT INTO players(player_id,player_name,level,world_level,unlock_functions) "
        "VALUES(?,?,10,1,?)",
        (PLAYER, "moon-dye-test", pickle.dumps([])),
    )
    monkeypatch.setattr(achieve_track, "report", lambda *args, **kwargs: None)
    yield
    db.exit()
    Config.DB_PATH, Config.IN_MEMORY = old_path, old_memory


def moon_request(moon_id=MOON):
    return CollectMoonReq(moon_id=moon_id)


def dye_request(outfit_id=OUTFIT, marker=None):
    request = GenericGameAReq(
        generic_msg_id=5,
        params=[CommonParam(int_value=outfit_id)],
    )
    if marker is not None:
        request.param_map["marker"] = marker
    return request


@dataclasses.dataclass(frozen=True)
class HandlerCase:
    name: str
    module: object
    request: object
    altered_request: object
    response_id: int


CASES = (
    HandlerCase(
        "moon",
        collect_moon,
        moon_request(),
        moon_request(OTHER_MOON),
        MsgId.CollectMoonRsp,
    ),
    HandlerCase(
        "dye",
        generic_game_a,
        dye_request(),
        dye_request(marker="different-fingerprint"),
        MsgId.GenericGameARsp,
    ),
)


def invoke(case, packet_id, request=None, session=None):
    session = session or FakeSession()
    request = request or case.request
    case.module.Handler().handle(session, request.SerializeToString(), packet_id)
    return session.response(case.response_id)


def item_num(item_id):
    raw = db.get_item_detail(PLAYER, item_id)
    if not raw:
        return 0
    detail = ItemDetail.FromString(raw)
    return (
        detail.main_item.base_item.num
        if detail.main_item.WhichOneof("item") == "base_item"
        else 1
    )


def outfit_detail(outfit_id=OUTFIT):
    return ItemDetail.FromString(db.get_item_detail(PLAYER, outfit_id))


def operations():
    return db.get_kv_keys(PLAYER, "activity_operations")


def database_snapshot():
    return tuple(
        tuple(
            db.db.execute(f"SELECT * FROM {table} ORDER BY 1, 2").fetchall()
        )
        for table in (
            "items", "items_s", "player_kv", "claimed_rewards", "achieve", "instance"
        )
    )


def prepare(case, currency=100):
    if case.name == "dye":
        item_mgr.grant_items(PLAYER, [(CURRENCY, currency), (OUTFIT, 1)])


def assert_success(case, response, session):
    assert response.status == OK
    assert len(session.packs()) == 1
    assert session.packs()[0].items
    assert len(operations()) == 1
    if case.name == "moon":
        assert item_num(MOON_REWARD) == 5
        assert len(response.rewards) == 1
        assert response.rewards[0].main_item.base_item.num == 5
        assert db.get_kv(PLAYER, "collected_moons", "ids") == str(MOON).encode()
    else:
        assert item_num(CURRENCY) == 0
        schemes = outfit_detail().main_item.outfit.dye_schemes
        assert len(schemes) == 2
        assert [scheme.scheme_index for scheme in schemes] == [0, 1]
        assert all(scheme.is_un_lock for scheme in schemes)


def concurrently(calls):
    barrier = threading.Barrier(len(calls))

    def run(call):
        barrier.wait()
        return call()

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(calls)) as executor:
        return list(executor.map(run, calls))


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_success_commits_state_economy_and_idempotency(case):
    prepare(case)
    session = FakeSession()
    response = invoke(case, 101, session=session)
    assert_success(case, response, session)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_zero_packet_id_remains_usable_without_operation_record(case):
    prepare(case)
    session = FakeSession()
    response = invoke(case, 0, session=session)
    assert response.status == OK
    assert session.packs()[0].items
    assert operations() == []
    assert item_num(MOON_REWARD if case.name == "moon" else CURRENCY) == (
        5 if case.name == "moon" else 0
    )


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_same_packet_and_fingerprint_replays_without_side_effects(case):
    prepare(case)
    first_session = FakeSession()
    first = invoke(case, 102, session=first_session)
    committed = database_snapshot()
    first_pack = first_session.packs()[0].SerializeToString()

    replay_session = FakeSession()
    replay = invoke(case, 102, session=replay_session)

    assert replay.SerializeToString() == first.SerializeToString()
    assert replay_session.packs()[0].SerializeToString() == first_pack
    assert database_snapshot() == committed


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_same_packet_with_different_fingerprint_conflicts(case):
    prepare(case)
    assert invoke(case, 103).status == OK
    committed = database_snapshot()
    session = FakeSession()

    response = invoke(case, 103, case.altered_request, session)

    assert response.status == BAD_REQ
    assert session.packs() == []
    assert database_snapshot() == committed


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_concurrent_same_packet_applies_exactly_once(case):
    prepare(case)
    sessions = [FakeSession() for _ in range(8)]

    responses = concurrently([
        (lambda session=session: invoke(case, 104, session=session))
        for session in sessions
    ])

    assert [response.status for response in responses] == [OK] * 8
    assert len({response.SerializeToString() for response in responses}) == 1
    assert len({session.packs()[0].SerializeToString() for session in sessions}) == 1
    assert item_num(MOON_REWARD if case.name == "moon" else CURRENCY) == (
        5 if case.name == "moon" else 0
    )
    if case.name == "dye":
        assert len(outfit_detail().main_item.outfit.dye_schemes) == 2
    assert len(operations()) == 1


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_request_uses_one_sqlite_transaction(case):
    prepare(case)
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        response = invoke(case, 105)
    finally:
        db.conn.set_trace_callback(None)

    assert response.status == OK
    assert statements.count("BEGIN IMMEDIATE") == 1


def test_concurrent_distinct_moons_merge_collected_state():
    sessions = [FakeSession(), FakeSession()]
    responses = concurrently([
        (lambda session=session, moon_id=moon_id, packet_id=packet_id: invoke(
            CASES[0], packet_id, moon_request(moon_id), session
        ))
        for session, moon_id, packet_id in zip(
            sessions, (MOON, OTHER_MOON), (106, 109)
        )
    ])

    assert [response.status for response in responses] == [OK, OK]
    assert item_num(MOON_REWARD) == 10
    assert db.get_kv(PLAYER, "collected_moons", "ids") == (
        f"{MOON},{OTHER_MOON}".encode()
    )
    assert len(operations()) == 2


def test_concurrent_distinct_dye_purchases_do_not_lose_unlock_or_currency():
    prepare(CASES[1], currency=200)
    responses = concurrently([
        (lambda packet_id=packet_id: invoke(CASES[1], packet_id))
        for packet_id in (107, 108)
    ])

    assert [response.status for response in responses] == [OK, OK]
    assert item_num(CURRENCY) == 0
    assert len(outfit_detail().main_item.outfit.dye_schemes) == 3
    assert len(operations()) == 2


def test_already_collected_moon_with_new_packet_is_rejected_without_changes():
    assert invoke(CASES[0], 109).status == OK
    committed = database_snapshot()
    session = FakeSession()

    response = invoke(CASES[0], 110, session=session)

    assert response.status == BAD_REQ
    assert session.packs() == []
    assert database_snapshot() == committed


def test_fully_unlocked_outfit_is_idempotent_without_charge():
    prepare(CASES[1])
    outfit = outfit_detail()
    schemes = outfit.main_item.outfit.dye_schemes
    while len(schemes) < Config.MAX_DYE_SCHEME_NUM:
        scheme = schemes.add()
        scheme.scheme_index = len(schemes) - 1
        scheme.is_un_lock = True
    db.set_item_detail(PLAYER, outfit.SerializeToString(), OUTFIT)
    committed_outfit = db.get_item_detail(PLAYER, OUTFIT)
    session = FakeSession()

    response = invoke(CASES[1], 111, session=session)

    assert response.status == OK
    assert item_num(CURRENCY) == 100
    assert db.get_item_detail(PLAYER, OUTFIT) == committed_outfit
    assert session.packs() == []
    assert len(operations()) == 1


def test_insufficient_dye_currency_has_no_side_effects():
    prepare(CASES[1], currency=99)
    committed = database_snapshot()
    session = FakeSession()

    response = invoke(CASES[1], 112, session=session)

    assert response.status == NOT_ENOUGH
    assert session.packs() == []
    assert database_snapshot() == committed


def test_non_moon_collection_entry_cannot_claim_moon_reward():
    row = next(
        row
        for row in res_loader.res["CollectionItem"]["collection_item"]["datas"]
        if row.get("new_collection_type") != 10
    )
    committed = database_snapshot()
    session = FakeSession()

    response = invoke(CASES[0], 113, moon_request(row["i_d"]), session)

    assert response.status == BAD_REQ
    assert session.packs() == []
    assert database_snapshot() == committed


def test_missing_moon_reward_item_config_fails_closed(monkeypatch):
    rows = tuple(
        row
        for row in res_loader.res["Item"]["item"]["datas"]
        if row.get("i_d") != MOON_REWARD
    )
    monkeypatch.setitem(res_loader.res, "Item", {"item": {"datas": rows}})
    committed = database_snapshot()
    session = FakeSession()

    response = invoke(CASES[0], 114, session=session)

    assert response.status == CFG_NOT_FOUND
    assert session.packs() == []
    assert database_snapshot() == committed


@pytest.mark.parametrize(
    "values",
    (None, ("102:bad",), ("999999999:100",), ("102:0",)),
    ids=("missing", "malformed", "unknown-currency", "zero-price"),
)
def test_invalid_dye_price_config_fails_closed(monkeypatch, values):
    prepare(CASES[1])
    rows = [
        row
        for row in res_loader.res["GlobalConfig"]["config"]["datas"]
        if row.get("key") != "Fashion_ColoringPlan_Price"
    ]
    if values is not None:
        rows.append({"key": "Fashion_ColoringPlan_Price", "values": values})
    monkeypatch.setitem(res_loader.res, "GlobalConfig", {"config": {"datas": tuple(rows)}})
    committed = database_snapshot()
    session = FakeSession()

    response = invoke(CASES[1], 115, session=session)

    assert response.status == CFG_NOT_FOUND
    assert session.packs() == []
    assert database_snapshot() == committed


def test_missing_outfit_config_fails_closed(monkeypatch):
    prepare(CASES[1])
    rows = tuple(
        row
        for row in res_loader.res["Item"]["item"]["datas"]
        if row.get("i_d") != OUTFIT
    )
    monkeypatch.setitem(res_loader.res, "Item", {"item": {"datas": rows}})
    committed = database_snapshot()
    session = FakeSession()

    response = invoke(CASES[1], 116, session=session)

    assert response.status == CFG_NOT_FOUND
    assert session.packs() == []
    assert database_snapshot() == committed


def test_client_fields_cannot_create_unconfigured_rewards():
    prepare(CASES[1])
    request = GenericGameAReq(
        generic_msg_id=5,
        params=[CommonParam(int_value=OUTFIT), CommonParam(int_value=999999999)],
        param_map={"reward_item": "999999999", "reward_count": "999"},
    )

    response = invoke(CASES[1], 117, request)

    assert response.status == OK
    assert item_num(CURRENCY) == 0
    assert db.get_item_detail(PLAYER, 999999999) is None
    assert len(outfit_detail().main_item.outfit.dye_schemes) == 2


def test_moon_grant_failure_rolls_back_state_and_operation(monkeypatch):
    committed = database_snapshot()
    session = FakeSession()
    original = item_mgr.grant_items

    def fail_after_grant(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("synthetic grant failure")

    monkeypatch.setattr(item_mgr, "grant_items", fail_after_grant)
    with pytest.raises(RuntimeError, match="synthetic grant failure"):
        invoke(CASES[0], 118, session=session)

    assert database_snapshot() == committed
    assert session.sent == []


def test_moon_achievement_failure_rolls_back_reward_state_and_operation(monkeypatch):
    committed = database_snapshot()
    session = FakeSession()

    def fail_report(*args, **kwargs):
        raise RuntimeError("synthetic achievement failure")

    monkeypatch.setattr(achieve_track, "report", fail_report)
    with pytest.raises(RuntimeError, match="synthetic achievement failure"):
        invoke(CASES[0], 123, session=session)

    assert database_snapshot() == committed
    assert session.sent == []


def test_moon_state_save_failure_rolls_back_reward_and_operation(monkeypatch):
    committed = database_snapshot()
    session = FakeSession()
    original = db.set_kv

    def fail_after_state(player_id, namespace, key, value):
        original(player_id, namespace, key, value)
        if namespace == "collected_moons" and key == "ids":
            raise RuntimeError("synthetic state save failure")
        return None

    monkeypatch.setattr(db, "set_kv", fail_after_state)
    with pytest.raises(RuntimeError, match="synthetic state save failure"):
        invoke(CASES[0], 119, session=session)

    assert database_snapshot() == committed
    assert session.sent == []


def test_dye_debit_failure_rolls_back_everything(monkeypatch):
    prepare(CASES[1])
    committed = database_snapshot()
    session = FakeSession()
    original = item_mgr.consume_items

    def fail_after_debit(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("synthetic debit failure")

    monkeypatch.setattr(item_mgr, "consume_items", fail_after_debit)
    with pytest.raises(RuntimeError, match="synthetic debit failure"):
        invoke(CASES[1], 120, session=session)

    assert database_snapshot() == committed
    assert session.sent == []


def test_dye_unlock_save_failure_rolls_back_debit_and_operation(monkeypatch):
    prepare(CASES[1])
    committed = database_snapshot()
    session = FakeSession()
    original = db.set_item_detail

    def fail_after_unlock(player_id, blob, item_id=None, instance_id=None):
        original(player_id, blob, item_id, instance_id)
        if item_id == OUTFIT:
            raise RuntimeError("synthetic unlock save failure")
        return None

    monkeypatch.setattr(db, "set_item_detail", fail_after_unlock)
    with pytest.raises(RuntimeError, match="synthetic unlock save failure"):
        invoke(CASES[1], 121, session=session)

    assert database_snapshot() == committed
    assert session.sent == []


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_operation_save_failure_rolls_back_everything(case, monkeypatch):
    prepare(case)
    committed = database_snapshot()
    session = FakeSession()
    original = activities._save_operation

    def fail_after_operation(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("synthetic operation save failure")

    monkeypatch.setattr(activities, "_save_operation", fail_after_operation)
    with pytest.raises(RuntimeError, match="synthetic operation save failure"):
        invoke(case, 122, session=session)

    assert database_snapshot() == committed
    assert session.sent == []
