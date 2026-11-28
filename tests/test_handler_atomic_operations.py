import concurrent.futures
import contextlib
import pickle
import threading
from dataclasses import dataclass
from unittest.mock import patch

import pytest

# Keep the repository's established db -> protobuf import order.
import utils.db as db
from config import Config
from network.msg_id import MsgId
from proto.net_pb2 import (
    AbilityBadgePageBoxActiveReq,
    BlessTreeUnlockReq,
    FurnitureDetailsInfo,
    ItemDetail,
    PlayerAbilityLevelUpReq,
    StatusCode,
    TakeOutFurnitureReq,
)
import handlers.AbilityBadgePageBoxActive as badge
import handlers.BlessTreeUnlock as bless
import handlers.PlayerAbilityLevelUp as ability
import handlers.TakeOutFurniture as furniture
from server import activities
from utils import item_mgr, res_loader


PLAYER = 1
COST = 101
REWARD = 102
FURNITURE_ITEM = 5040001
OK = StatusCode.StatusCode_OK
BAD_REQ = StatusCode.StatusCode_BAD_REQ
CFG_NOT_FOUND = StatusCode.StatusCode_CFG_NOT_FOUND
ITEM_NOT_ENOUGH = StatusCode.StatusCode_ITEM_NOT_ENOUGH


class FakeSession:
    def __init__(self, player_id=PLAYER, scene_id=9999, channel_id=PLAYER):
        self.player_id = player_id
        self.scene_id = scene_id
        self.channel_id = channel_id
        self.sent = []

    def send(self, message_id, message, packet_id=0):
        saved = type(message)()
        saved.CopyFrom(message)
        self.sent.append((message_id, saved, packet_id))

    def response(self, message_id):
        return [message for mid, message, _ in self.sent if mid == message_id][-1]

    def messages(self, message_id):
        return [message for mid, message, _ in self.sent if mid == message_id]


@pytest.fixture(scope="module", autouse=True)
def resources():
    res_loader.init()


@pytest.fixture(autouse=True)
def game_db(tmp_path, monkeypatch):
    old_path, old_memory = Config.DB_PATH, Config.IN_MEMORY
    db.exit()
    Config.DB_PATH = str(tmp_path / "atomic-handlers.db")
    Config.IN_MEMORY = False
    db.init()
    db.db.execute(
        "INSERT INTO players(player_id,player_name,level,world_level,unlock_functions) "
        "VALUES(?,?,10,1,?)",
        (PLAYER, "atomic-handlers", pickle.dumps([])),
    )
    monkeypatch.setattr(item_mgr.achieve_track, "report", lambda *args, **kwargs: None)
    yield
    db.exit()
    Config.DB_PATH, Config.IN_MEMORY = old_path, old_memory


def grant(item_id, count):
    item_mgr.grant_items(PLAYER, [(item_id, count)])


def item_num(item_id):
    raw = db.get_item_detail(PLAYER, item_id)
    if not raw:
        return 0
    detail = ItemDetail.FromString(raw)
    if detail.main_item.WhichOneof("item") == "base_item":
        return detail.main_item.base_item.num
    return 1


def database_snapshot():
    instance = db.db.execute(
        "SELECT instance_id FROM instance WHERE player_id=?", (PLAYER,)
    ).fetchone()
    return (
        tuple(sorted(db.get_item_detail(PLAYER, table="items"))),
        tuple(sorted(db.get_item_detail(PLAYER, table="items_s"))),
        db.get_bless_tree(PLAYER),
        db.get_ability_tree(PLAYER),
        db.get_kv_ns(PLAYER, "ability"),
        db.get_garden_info(PLAYER),
        db.get_furniture(9999, PLAYER),
        tuple(sorted(db.get_kv_keys(PLAYER, "activity_operations"))),
        tuple(sorted(db.get_claimed(PLAYER, "got_ids"))),
        instance,
    )


def concurrently(calls):
    barrier = threading.Barrier(len(calls))

    def invoke(call):
        barrier.wait()
        return call()

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(calls)) as executor:
        return list(executor.map(invoke, calls))


def bless_resources(reward_item=REWARD):
    return {
        "BlessingTree": {
            "blessing_tree_info": {
                "datas": ({
                    "i_d": 1,
                    "blessing_tree_info_group_info": ({
                        "box_i_d": 10,
                        "item_i_d": COST,
                        "item_count": 2,
                        "reward_item_i_d": reward_item,
                        "reward_item_count": 5,
                    },),
                },),
            }
        }
    }


def badge_resources(cost_item=COST):
    return {
        "MoonStone": {
            "stone_area_data": {
                "datas": ({"i_d": 1, "scene_i_d": 1, "unlock_i_d": 100},),
            },
            "stone_unlock_data": {
                "datas": ({
                    "i_d": 100,
                    "stone_unlock_data_group_info": ({
                        "box_i_d": 1,
                        "item_i_d": cost_item,
                        "item_count": 2,
                    },),
                },),
            },
        }
    }


def ability_resources(reward_item=REWARD):
    return {
        "Ability": {
            "ability": {
                "datas": ({
                    "i_d": 10,
                    "need_item_i_d": 104,
                    "level_info": ({
                        "level": 1,
                        "cost_item_i_d": COST,
                        "cost_count": 2,
                        "reward_item_i_d": reward_item,
                    },),
                },),
            }
        }
    }


def prepare_bless():
    grant(COST, 10)
    db.set_bless_tree(PLAYER, 1, [])


def prepare_badge():
    grant(COST, 10)


def prepare_ability():
    grant(COST, 10)
    db.set_kv(PLAYER, "ability", 10, 0)


def prepare_furniture():
    db.set_garden_info(PLAYER, 0, 0, 1, 10, True)
    details = FurnitureDetailsInfo(furniture_id=50, furniture_item_id=FURNITURE_ITEM)
    db.set_furniture(9999, PLAYER, PLAYER, 50, details.SerializeToString())


def assert_bless_success():
    assert item_num(COST) == 8
    assert item_num(REWARD) == 5
    assert db.get_bless_tree(PLAYER, 1) == [10]
    assert len(db.get_kv_keys(PLAYER, "activity_operations")) == 1


def assert_badge_success():
    assert item_num(COST) == 8
    assert item_num(REWARD) == 5
    assert db.get_ability_tree(PLAYER, 1) == [1]
    assert len(db.get_kv_keys(PLAYER, "activity_operations")) == 1


def assert_ability_success():
    assert item_num(COST) == 8
    assert item_num(REWARD) == 1
    assert db.get_kv(PLAYER, "ability", 10) == 1
    assert len(db.get_kv_keys(PLAYER, "activity_operations")) == 1


def assert_furniture_success():
    assert db.get_furniture(9999, PLAYER, PLAYER, 50) is None
    assert db.get_garden_info(PLAYER)[2] == 0
    assert item_num(FURNITURE_ITEM) == 1
    assert len(db.get_kv_keys(PLAYER, "activity_operations")) == 1


def corrupt_furniture_config():
    details = FurnitureDetailsInfo(furniture_id=50, furniture_item_id=999999999)
    db.set_furniture(9999, PLAYER, PLAYER, 50, details.SerializeToString())


@dataclass
class HandlerCase:
    name: str
    handler: object
    request: object
    response_id: int
    resources: dict
    prepare: object
    assert_success: object
    altered_request: object
    bad_resources: object = None
    make_bad_config: object = None


CASES = (
    HandlerCase(
        "bless",
        bless,
        BlessTreeUnlockReq(def_id=1, tree_id=10),
        MsgId.BlessTreeUnlockRsp,
        bless_resources(),
        prepare_bless,
        assert_bless_success,
        BlessTreeUnlockReq(def_id=1, tree_id=11),
        bad_resources=bless_resources(999999999),
    ),
    HandlerCase(
        "badge",
        badge,
        AbilityBadgePageBoxActiveReq(page=1, box_id=1),
        MsgId.AbilityBadgePageBoxActiveRsp,
        badge_resources(),
        prepare_badge,
        assert_badge_success,
        AbilityBadgePageBoxActiveReq(page=1, box_id=2),
        bad_resources=badge_resources(999999999),
    ),
    HandlerCase(
        "ability",
        ability,
        PlayerAbilityLevelUpReq(ability_id=10),
        MsgId.PlayerAbilityLevelUpRsp,
        ability_resources(),
        prepare_ability,
        assert_ability_success,
        PlayerAbilityLevelUpReq(ability_id=11),
        bad_resources=ability_resources(999999999),
    ),
    HandlerCase(
        "furniture",
        furniture,
        TakeOutFurnitureReq(furniture_id=50, is_put_in_package=True),
        MsgId.TakeOutFurnitureRsp,
        {},
        prepare_furniture,
        assert_furniture_success,
        TakeOutFurnitureReq(furniture_id=50, is_put_in_package=False),
        make_bad_config=corrupt_furniture_config,
    ),
)


@contextlib.contextmanager
def configured(case):
    with patch.dict(res_loader.res, case.resources):
        if case.name == "badge":
            with patch.object(Config, "BADGE_NODE_REWARDS", {1: {1: ((REWARD, 5),)}}):
                yield
        else:
            yield


def invoke(case, session, request, packet_id):
    case.handler.Handler().handle(session, request.SerializeToString(), packet_id)
    return session.response(case.response_id)


def raise_after(original, message):
    def wrapped(*args, **kwargs):
        result = original(*args, **kwargs)
        raise RuntimeError(message)

    return wrapped


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_normal_operation_commits_cost_state_reward_and_response(case):
    case.prepare()
    session = FakeSession()
    with configured(case):
        response = invoke(case, session, case.request, 101)
    assert response.status == OK
    case.assert_success()
    assert len(session.messages(MsgId.PackNotice)) == 1
    assert session.messages(MsgId.PackNotice)[0].items


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_zero_packet_id_remains_supported_without_idempotency_record(case):
    case.prepare()
    session = FakeSession()
    with configured(case):
        response = invoke(case, session, case.request, 0)
    assert response.status == OK
    assert db.get_kv_keys(PLAYER, "activity_operations") == []


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_same_packet_and_fingerprint_replays_original_response_without_side_effects(case):
    case.prepare()
    first_session = FakeSession()
    with configured(case):
        first = invoke(case, first_session, case.request, 102)
    committed = database_snapshot()
    first_pack = first_session.messages(MsgId.PackNotice)[0].SerializeToString()

    replay_session = FakeSession()
    with configured(case):
        replay = invoke(case, replay_session, case.request, 102)
    assert replay.SerializeToString() == first.SerializeToString()
    assert replay_session.messages(MsgId.PackNotice)[0].SerializeToString() == first_pack
    assert database_snapshot() == committed


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_same_packet_with_different_fingerprint_conflicts_without_side_effects(case):
    case.prepare()
    with configured(case):
        assert invoke(case, FakeSession(), case.request, 103).status == OK
    committed = database_snapshot()
    session = FakeSession()
    with configured(case):
        response = invoke(case, session, case.altered_request, 103)
    assert response.status == BAD_REQ
    assert database_snapshot() == committed
    assert not session.messages(MsgId.PackNotice)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_concurrent_same_packet_applies_exactly_once(case):
    case.prepare()
    sessions = [FakeSession() for _ in range(6)]
    with configured(case):
        responses = concurrently([
            (lambda session=session: invoke(case, session, case.request, 104))
            for session in sessions
        ])
    assert [response.status for response in responses] == [OK] * 6
    assert len({response.SerializeToString() for response in responses}) == 1
    case.assert_success()


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_unknown_configuration_fails_closed_before_any_write(case):
    case.prepare()
    before = database_snapshot()
    session = FakeSession()
    with configured(case):
        if case.bad_resources is not None:
            with patch.dict(res_loader.res, case.bad_resources):
                response = invoke(case, session, case.request, 105)
        else:
            case.make_bad_config()
            before = database_snapshot()
            response = invoke(case, session, case.request, 105)
    assert response.status == CFG_NOT_FOUND
    assert database_snapshot() == before
    assert not session.messages(MsgId.PackNotice)


@pytest.mark.parametrize("case", CASES[:3], ids=lambda case: case.name)
def test_consume_failure_after_debit_rolls_back_everything(case):
    case.prepare()
    before = database_snapshot()
    session = FakeSession()
    original = item_mgr.consume_items
    with configured(case), patch.object(
        item_mgr,
        "consume_items",
        side_effect=raise_after(original, "synthetic consume failure"),
    ):
        with pytest.raises(RuntimeError, match="synthetic consume failure"):
            invoke(case, session, case.request, 106)
    assert database_snapshot() == before
    assert session.sent == []


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_grant_failure_after_grant_rolls_back_everything(case):
    case.prepare()
    before = database_snapshot()
    session = FakeSession()
    original = item_mgr.grant_items
    with configured(case), patch.object(
        item_mgr,
        "grant_items",
        side_effect=raise_after(original, "synthetic grant failure"),
    ):
        with pytest.raises(RuntimeError, match="synthetic grant failure"):
            invoke(case, session, case.request, 107)
    assert database_snapshot() == before
    assert session.sent == []


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_state_save_failure_rolls_back_debit_reward_and_operation(case):
    case.prepare()
    before = database_snapshot()
    session = FakeSession()
    if case.name == "bless":
        target, attr, original = db, "set_bless_tree", db.set_bless_tree
    elif case.name == "badge":
        target, attr, original = db, "set_ability_tree", db.set_ability_tree
    elif case.name == "ability":
        target, attr, original = db, "set_kv", db.set_kv
    else:
        target, attr, original = db, "set_garden_info", db.set_garden_info

    def fail_state(*args, **kwargs):
        result = original(*args, **kwargs)
        if case.name != "ability" or (len(args) > 2 and args[1] == "ability"):
            raise RuntimeError("synthetic state save failure")
        return result

    with configured(case), patch.object(target, attr, side_effect=fail_state):
        with pytest.raises(RuntimeError, match="synthetic state save failure"):
            invoke(case, session, case.request, 108)
    assert database_snapshot() == before
    assert session.sent == []


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_operation_record_save_failure_rolls_back_everything(case):
    case.prepare()
    before = database_snapshot()
    session = FakeSession()
    original = activities._save_operation
    with configured(case), patch.object(
        activities,
        "_save_operation",
        side_effect=raise_after(original, "synthetic operation save failure"),
    ):
        with pytest.raises(RuntimeError, match="synthetic operation save failure"):
            invoke(case, session, case.request, 109)
    assert database_snapshot() == before
    assert session.sent == []


@pytest.mark.parametrize("case", CASES[:3], ids=lambda case: case.name)
def test_insufficient_cost_never_partially_consumes_or_activates(case):
    case.prepare()
    raw = db.get_item_detail(PLAYER, COST)
    detail = ItemDetail.FromString(raw)
    detail.main_item.base_item.num = 1
    db.set_item_detail(PLAYER, detail.SerializeToString(), COST)
    before = database_snapshot()
    with configured(case):
        response = invoke(case, FakeSession(), case.request, 110)
    assert response.status == ITEM_NOT_ENOUGH
    assert database_snapshot() == before


def test_bless_tree_requires_configured_prerequisite_before_payment():
    resources = bless_resources()
    resources["BlessingTree"]["blessing_tree_info"]["datas"][0][
        "blessing_tree_info_group_info"
    ] += ({
        "box_i_d": 20,
        "relate_box_i_d": 10,
        "item_i_d": COST,
        "item_count": 2,
        "reward_item_i_d": REWARD,
        "reward_item_count": 5,
    },)
    prepare_bless()
    before = database_snapshot()
    with patch.dict(res_loader.res, resources):
        response = invoke(
            CASES[0],
            FakeSession(),
            BlessTreeUnlockReq(def_id=1, tree_id=20),
            111,
        )
    assert response.status == StatusCode.StatusCode_PLAYER_BLESS_NEED_NODE
    assert database_snapshot() == before


def test_ability_must_be_unlocked_before_level_up():
    grant(COST, 10)
    before = database_snapshot()
    with configured(CASES[2]):
        response = invoke(CASES[2], FakeSession(), CASES[2].request, 112)
    assert response.status == StatusCode.StatusCode_FAIL
    assert database_snapshot() == before


def test_badge_duplicate_with_new_packet_is_idempotent_without_reward():
    grant(COST, 10)
    db.set_ability_tree(PLAYER, 1, [1])
    before = database_snapshot()
    session = FakeSession()
    with configured(CASES[1]):
        response = invoke(CASES[1], session, CASES[1].request, 113)
    assert response.status == OK
    assert database_snapshot() == before
    assert not session.messages(MsgId.PackNotice)


def test_furniture_remove_without_inventory_return_does_not_grant():
    prepare_furniture()
    session = FakeSession()
    request = TakeOutFurnitureReq(furniture_id=50, is_put_in_package=False)
    with patch.object(furniture.scene_data, "get_session", return_value=[]):
        response = invoke(CASES[3], session, request, 114)
    assert response.status == OK
    assert db.get_furniture(9999, PLAYER, PLAYER, 50) is None
    assert db.get_garden_info(PLAYER)[2] == 0
    assert item_num(FURNITURE_ITEM) == 0
    assert not session.messages(MsgId.PackNotice)


def test_furniture_replay_does_not_rebroadcast_removal():
    prepare_furniture()
    watcher = FakeSession()
    with patch.object(furniture.scene_data, "get_session", return_value=[watcher]):
        assert invoke(CASES[3], FakeSession(), CASES[3].request, 115).status == OK
        assert invoke(CASES[3], FakeSession(), CASES[3].request, 115).status == OK
    assert len(watcher.messages(MsgId.SceneGardenFurnitureRemoveNotice)) == 1
