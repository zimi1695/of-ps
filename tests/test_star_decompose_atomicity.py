import concurrent.futures
import copy
import pickle
import threading
from unittest.mock import Mock, patch

import pytest

# Keep the repository's established db -> protobuf import order.
import utils.db as db
from config import Config
from network.msg_id import MsgId
from proto.net_pb2 import (
    Character,
    CharacterStarUpReq,
    EquipDisassembleReq,
    ItemDetail,
    PetDecomposeReq,
    StatusCode,
)
import handlers.CharacterStarUp as star_up
import handlers.EquipDisassemble as equip_disassemble
import handlers.PetDecompose as pet_decompose
from server import activities, character_growth as growth
from utils import achieve_track, item_mgr, res_loader


PLAYER = 1
CHARACTER = 101001
SHARD = 31101
COIN = 101
REWARD = 102
WEAPON = 1000001
ARMOR = 2111001
PET = 200001001
WEAPON_INSTANCE = 7001
ARMOR_INSTANCE = 7002
PET_INSTANCE_1 = 7101
PET_INSTANCE_2 = 7102
OK = StatusCode.StatusCode_OK
BAD_REQ = StatusCode.StatusCode_BAD_REQ
CFG_NOT_FOUND = StatusCode.StatusCode_CFG_NOT_FOUND
ITEM_NOT_ENOUGH = StatusCode.StatusCode_ITEM_NOT_ENOUGH
KINDS = ("star", "equip", "pet")
SYSTEMS = {
    "star": "character-star-up",
    "equip": "equip-disassemble",
    "pet": "pet-decompose",
}
HANDLERS = {
    "star": star_up,
    "equip": equip_disassemble,
    "pet": pet_decompose,
}
RESPONSE_IDS = {
    "star": MsgId.CharacterStarUpRsp,
    "equip": MsgId.EquipDisassembleRsp,
    "pet": MsgId.PetDecomposeRsp,
}


class FakeSession:
    def __init__(self):
        self.player_id = PLAYER
        self.scene_id = 9999
        self.channel_id = PLAYER
        self.sent = []
        self._send_lock = threading.Lock()

    def send(self, message_id, message, packet_id=0):
        saved = type(message)()
        saved.CopyFrom(message)
        with self._send_lock:
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
    Config.DB_PATH = str(tmp_path / "star-decompose.db")
    Config.IN_MEMORY = False
    db.init()
    db.db.execute(
        "INSERT INTO players(player_id,player_name,level,world_level,unlock_functions) "
        "VALUES(?,?,10,1,?)",
        (PLAYER, "star-decompose", pickle.dumps([])),
    )
    monkeypatch.setattr(item_mgr, "_item_tags", None)
    monkeypatch.setattr(item_mgr, "_character_ids", None)
    monkeypatch.setattr(item_mgr, "_player_unlock_item_ids", None)
    monkeypatch.setattr(achieve_track, "report", Mock())
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
    return (
        detail.main_item.base_item.num
        if detail.main_item.WhichOneof("item") == "base_item"
        else 1
    )


def database_snapshot():
    tables = [
        row[0]
        for row in db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    ]
    return tuple(
        (
            table,
            tuple(
                sorted(
                    repr(row)
                    for row in db.conn.execute(f'SELECT * FROM "{table}"').fetchall()
                )
            ),
        )
        for table in tables
    )


def configured_resources(kind):
    character = dict(res_loader.res["Character"])
    character["character_star"] = {
        "datas": (
            {
                "i_d": CHARACTER,
                "star_info": (
                    {"star": 1, "item_i_d": SHARD, "item_num": 2},
                    {"star": 2, "item_i_d": SHARD, "item_num": 2},
                ),
            },
        )
    }
    weapon = {
        "weapon": {
            "datas": (
                {
                    "i_d": WEAPON,
                    "item_i_d": WEAPON,
                    "weapon_property_i_d": 1,
                },
            )
        },
        "weapon_property": {
            "datas": (
                {
                    "i_d": 1,
                    "weapon_property_group_info": (
                        {
                            "min_level": 1,
                            "max_level": 1,
                            "disassemble_i_d": 1,
                        },
                    ),
                },
            )
        },
        "weapon_disassemble": {
            "datas": (
                {
                    "i_d": 1,
                    "weapon_disassemble_item": (
                        {
                            "item_i_d": REWARD,
                            "item_rate": 1.0,
                            "item_min_count": 2,
                            "item_max_count": 2,
                        },
                    ),
                },
            )
        },
    }
    armor = {
        "armor": {
            "datas": (
                {
                    "i_d": ARMOR,
                    "item_i_d": ARMOR,
                    "armor_property_i_d": 1,
                },
            )
        },
        "armor_property": {
            "datas": (
                {
                    "i_d": 1,
                    "armor_property_group_info": (
                        {
                            "min_level": 1,
                            "max_level": 1,
                            "disassemble_i_d": 1,
                        },
                    ),
                },
            )
        },
        "armor_disassemble": {
            "datas": (
                {
                    "i_d": 1,
                    "armor_disassemble_item": (
                        {
                            "item_i_d": REWARD,
                            "item_rate": 1.0,
                            "item_min_count": 1,
                            "item_max_count": 1,
                        },
                    ),
                },
            )
        },
    }
    pet = {
        "pet": {
            "datas": (
                {"i_d": PET, "release_reward": (1,)},
            )
        },
        "release_reward": {
            "datas": (
                {
                    "i_d": 1,
                    "release_reward_item": (
                        {
                            "release_reward_i_d": REWARD,
                            "release_reward_count": 4,
                        },
                    ),
                },
            )
        },
    }
    return {
        "Item": res_loader.res["Item"],
        "Character": character,
        "Weapon": weapon,
        "Armor": armor,
        "Pet": pet,
        "PlayerUnlock": res_loader.res["PlayerUnlock"],
        "Ability": {"ability": {"datas": ()}},
    }


def add_equipment(kind, item_id, instance_id):
    detail = ItemDetail()
    detail.main_item.item_id = item_id
    instance = getattr(detail.main_item, kind)
    setattr(instance, f"{kind}_id", item_id)
    instance.instance_id = instance_id
    instance.level = 1
    instance.property_index = 1
    db.set_item_detail(
        PLAYER, detail.SerializeToString(), None, instance_id
    )
    return detail


def add_pet(instance_id):
    detail = ItemDetail()
    detail.main_item.item_id = PET
    detail.main_item.pet.item_id = PET
    detail.main_item.pet.instance_id = instance_id
    db.set_item_detail(
        PLAYER, detail.SerializeToString(), None, instance_id
    )
    return detail


def request_for(kind):
    if kind == "star":
        return CharacterStarUpReq(char_id=CHARACTER)
    if kind == "equip":
        return EquipDisassembleReq(
            weapon_ids=[WEAPON_INSTANCE], armor_ids=[ARMOR_INSTANCE]
        )
    return PetDecomposeReq(pet_instance_ids=[PET_INSTANCE_1, PET_INSTANCE_2])


def altered_request(kind):
    request = type(request_for(kind))()
    request.CopyFrom(request_for(kind))
    if kind == "star":
        request.char_id = 101002
    elif kind == "equip":
        request.armor_ids.append(9999)
    else:
        request.pet_instance_ids.append(9999)
    return request


def prepare(kind, *, conversion=False, shard_count=2):
    if kind == "star":
        db.set_character(
            PLAYER,
            CHARACTER,
            Character(character_id=CHARACTER, star=2 if conversion else 0).SerializeToString(),
        )
        grant(SHARD, shard_count)
    elif kind == "equip":
        add_equipment("weapon", WEAPON, WEAPON_INSTANCE)
        add_equipment("armor", ARMOR, ARMOR_INSTANCE)
    else:
        add_pet(PET_INSTANCE_1)
        add_pet(PET_INSTANCE_2)


def invoke(kind, request, packet_id, session=None, *, resources=None, deterministic=True):
    session = session or FakeSession()
    with patch.dict(res_loader.res, resources or configured_resources(kind)):
        if deterministic:
            with patch.object(equip_disassemble.random, "random", return_value=0), patch.object(
                equip_disassemble.random, "randint", side_effect=lambda low, high: high
            ):
                HANDLERS[kind].Handler().handle(
                    session, request.SerializeToString(), packet_id
                )
        else:
            HANDLERS[kind].Handler().handle(
                session, request.SerializeToString(), packet_id
            )
    return session.response(RESPONSE_IDS[kind])


def assert_success(kind):
    if kind == "star":
        assert Character.FromString(db.get_characters(PLAYER, CHARACTER)[0]).star == 1
        assert item_num(SHARD) == 0
        assert item_num(REWARD) == 0
    elif kind == "equip":
        assert db.get_item_detail(PLAYER, None, WEAPON_INSTANCE) is None
        assert db.get_item_detail(PLAYER, None, ARMOR_INSTANCE) is None
        assert item_num(REWARD) == 3
    else:
        assert db.get_item_detail(PLAYER, None, PET_INSTANCE_1) is None
        assert db.get_item_detail(PLAYER, None, PET_INSTANCE_2) is None
        assert item_num(REWARD) == 8


def concurrently(calls):
    barrier = threading.Barrier(len(calls))

    def run(call):
        barrier.wait()
        return call()

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(calls)) as executor:
        return list(executor.map(run, calls))


def raise_after(original, message):
    def wrapped(*args, **kwargs):
        result = original(*args, **kwargs)
        raise RuntimeError(message)

    return wrapped


@pytest.mark.parametrize("kind", KINDS)
def test_success_commits_consumption_deletion_reward_state_and_operation(kind):
    prepare(kind)
    session = FakeSession()
    response = invoke(kind, request_for(kind), 101, session)
    assert response.status == OK
    assert_success(kind)
    assert db.get_kv(
        PLAYER, "activity_operations", f"{SYSTEMS[kind]}:0:101"
    ) is not None
    if kind == "equip":
        assert not session.messages(MsgId.PackNotice)
    else:
        assert session.messages(MsgId.PackNotice)


@pytest.mark.parametrize("kind", KINDS)
def test_zero_packet_id_remains_usable_without_operation_record(kind):
    prepare(kind)
    response = invoke(kind, request_for(kind), 0)
    assert response.status == OK
    assert_success(kind)
    assert db.get_kv(
        PLAYER, "activity_operations", f"{SYSTEMS[kind]}:0:0"
    ) is None


@pytest.mark.parametrize("kind", KINDS)
def test_same_packet_replays_original_response_without_side_effects(kind):
    prepare(kind)
    first_session = FakeSession()
    first = invoke(kind, request_for(kind), 102, first_session)
    before = database_snapshot()
    first_packs = [
        message.SerializeToString()
        for message in first_session.messages(MsgId.PackNotice)
    ]
    achieve_track.report.reset_mock()

    replay_session = FakeSession()
    with patch.object(
        equip_disassemble.random,
        "random",
        side_effect=AssertionError("replay must not roll rewards again"),
    ):
        replay = invoke(
            kind, request_for(kind), 102, replay_session, deterministic=False
        )
    assert replay.SerializeToString() == first.SerializeToString()
    assert [
        message.SerializeToString()
        for message in replay_session.messages(MsgId.PackNotice)
    ] == first_packs
    assert database_snapshot() == before
    achieve_track.report.assert_not_called()


@pytest.mark.parametrize("kind", KINDS)
def test_same_packet_with_different_fingerprint_conflicts(kind):
    prepare(kind)
    assert invoke(kind, request_for(kind), 103).status == OK
    before = database_snapshot()
    session = FakeSession()
    response = invoke(kind, altered_request(kind), 103, session)
    assert response.status == BAD_REQ
    assert database_snapshot() == before
    assert not session.messages(MsgId.PackNotice)


@pytest.mark.parametrize("kind", KINDS)
def test_concurrent_same_packet_applies_exactly_once(kind):
    prepare(kind)
    sessions = [FakeSession() for _ in range(6)]
    responses = concurrently([
        lambda session=session: invoke(kind, request_for(kind), 104, session)
        for session in sessions
    ])
    assert [response.status for response in responses] == [OK] * 6
    assert len({response.SerializeToString() for response in responses}) == 1
    assert_success(kind)


@pytest.mark.parametrize("kind", KINDS)
def test_concurrent_distinct_packets_cannot_pay_twice(kind):
    prepare(kind)
    responses = concurrently([
        lambda packet_id=200 + index: invoke(
            kind, request_for(kind), packet_id, FakeSession()
        )
        for index in range(6)
    ])
    if kind == "star":
        assert [response.status for response in responses].count(OK) == 1
        assert [response.status for response in responses].count(ITEM_NOT_ENOUGH) == 5
    elif kind == "equip":
        assert [response.status for response in responses] == [OK] * 6
        assert sum(bool(response.weapon_ids or response.armor_ids) for response in responses) == 1
    else:
        assert [response.status for response in responses].count(OK) == 1
        assert [response.status for response in responses].count(BAD_REQ) == 5
    assert_success(kind)


@pytest.mark.parametrize("kind", KINDS)
def test_insufficient_material_or_instance_never_deletes_or_grants(kind):
    if kind == "star":
        prepare(kind, shard_count=1)
        request = request_for(kind)
        expected = ITEM_NOT_ENOUGH
    elif kind == "equip":
        request = EquipDisassembleReq(weapon_ids=[9999], armor_ids=[9999])
        expected = OK
    else:
        request = PetDecomposeReq(pet_instance_ids=[9999])
        expected = BAD_REQ
    before = database_snapshot()
    response = invoke(kind, request, 105)
    assert response.status == expected
    assert item_num(REWARD) == 0
    assert db.get_item_detail(PLAYER, None, WEAPON_INSTANCE) is None
    assert db.get_item_detail(PLAYER, None, ARMOR_INSTANCE) is None
    assert db.get_item_detail(PLAYER, None, PET_INSTANCE_1) is None
    if kind == "equip":
        # 批量分解保留跳过不存在实例的语义；只记录本次空结果。
        assert db.get_kv(
            PLAYER, "activity_operations", f"{SYSTEMS[kind]}:0:105"
        ) is not None
    else:
        assert database_snapshot() == before


@pytest.mark.parametrize("kind", KINDS)
def test_configuration_error_fails_closed_before_any_write(kind):
    prepare(kind)
    resources = copy.deepcopy(configured_resources(kind))
    if kind == "star":
        resources["Character"]["character_star"]["datas"][0]["star_info"][0]["item_i_d"] = 999999999
    elif kind == "equip":
        resources["Weapon"]["weapon_disassemble"]["datas"][0]["weapon_disassemble_item"][0]["item_i_d"] = 999999999
    else:
        resources["Pet"]["pet"]["datas"][0]["release_reward"] = (999999999,)
    before = database_snapshot()
    session = FakeSession()
    response = invoke(kind, request_for(kind), 106, session, resources=resources)
    assert response.status == CFG_NOT_FOUND
    assert database_snapshot() == before
    assert session.sent and not session.messages(MsgId.PackNotice)


@pytest.mark.parametrize("kind", KINDS)
def test_grant_failure_after_grant_rolls_back_everything(kind):
    prepare(kind, conversion=kind == "star", shard_count=3)
    before = database_snapshot()
    session = FakeSession()
    original = item_mgr.grant_items
    with patch.object(
        item_mgr,
        "grant_items",
        side_effect=raise_after(original, "synthetic grant failure"),
    ):
        with pytest.raises(RuntimeError, match="synthetic grant failure"):
            invoke(kind, request_for(kind), 107, session)
    assert database_snapshot() == before
    assert session.sent == []
    assert invoke(kind, request_for(kind), 107).status == OK


def test_consume_failure_after_debit_rolls_back_character_and_material():
    prepare("star")
    before = database_snapshot()
    session = FakeSession()
    original = item_mgr.consume_items
    with patch.object(
        item_mgr,
        "consume_items",
        side_effect=raise_after(original, "synthetic consume failure"),
    ):
        with pytest.raises(RuntimeError, match="synthetic consume failure"):
            invoke("star", request_for("star"), 108, session)
    assert database_snapshot() == before
    assert session.sent == []


@pytest.mark.parametrize("kind", KINDS)
def test_state_save_failure_rolls_back_consumption_reward_and_operation(kind):
    prepare(kind)
    before = database_snapshot()
    session = FakeSession()
    if kind == "star":
        target, attribute, original = growth, "save_character", growth.save_character
        calls = 0

        def fail_save(*args, **kwargs):
            nonlocal calls
            calls += 1
            result = original(*args, **kwargs)
            raise RuntimeError("synthetic state save failure")
    else:
        target, attribute, original = db, "del_item_detail", db.del_item_detail
        calls = 0

        def fail_save(*args, **kwargs):
            nonlocal calls
            calls += 1
            result = original(*args, **kwargs)
            if calls == 2:
                raise RuntimeError("synthetic state save failure")
            return result

    with patch.object(target, attribute, side_effect=fail_save):
        with pytest.raises(RuntimeError, match="synthetic state save failure"):
            invoke(kind, request_for(kind), 109, session)
    assert database_snapshot() == before
    assert session.sent == []


@pytest.mark.parametrize("kind", KINDS)
def test_operation_save_failure_rolls_back_everything(kind):
    prepare(kind)
    before = database_snapshot()
    session = FakeSession()
    original = activities._save_operation
    with patch.object(
        activities,
        "_save_operation",
        side_effect=raise_after(original, "synthetic operation save failure"),
    ):
        with pytest.raises(RuntimeError, match="synthetic operation save failure"):
            invoke(kind, request_for(kind), 110, session)
    assert database_snapshot() == before
    assert session.sent == []


@pytest.mark.parametrize("kind", KINDS)
def test_replay_survives_sqlite_restart(kind):
    prepare(kind)
    first = invoke(kind, request_for(kind), 111)
    before = database_snapshot()
    db.exit()
    db.init()
    replay = invoke(kind, request_for(kind), 111)
    assert replay.SerializeToString() == first.SerializeToString()
    assert database_snapshot() == before


def test_full_star_conversion_uses_all_shards_and_fixed_server_reward():
    prepare("star", conversion=True, shard_count=3)
    response = invoke("star", request_for("star"), 112)
    assert response.status == OK
    assert response.star == 2
    assert item_num(SHARD) == 0
    assert item_num(COIN) == 1500
    assert Character.FromString(db.get_characters(PLAYER, CHARACTER)[0]).star == 2


@pytest.mark.parametrize("kind", ("equip", "pet"))
def test_duplicate_client_instance_ids_pay_once(kind):
    prepare(kind)
    request = request_for(kind)
    if kind == "equip":
        request.weapon_ids.extend([WEAPON_INSTANCE, WEAPON_INSTANCE])
        request.armor_ids.extend([ARMOR_INSTANCE, ARMOR_INSTANCE])
    else:
        request.pet_instance_ids.extend([PET_INSTANCE_1, PET_INSTANCE_2])
    response = invoke(kind, request, 113)
    assert response.status == OK
    assert_success(kind)


def test_locked_or_worn_equipment_and_active_pet_are_not_deleted():
    prepare("equip")
    weapon = ItemDetail.FromString(db.get_item_detail(PLAYER, None, WEAPON_INSTANCE))
    weapon.main_item.weapon.is_lock = True
    db.set_item_detail(PLAYER, weapon.SerializeToString(), None, WEAPON_INSTANCE)
    armor = ItemDetail.FromString(db.get_item_detail(PLAYER, None, ARMOR_INSTANCE))
    armor.main_item.armor.wearer_id = CHARACTER
    db.set_item_detail(PLAYER, armor.SerializeToString(), None, ARMOR_INSTANCE)
    response = invoke("equip", request_for("equip"), 114)
    assert response.status == OK
    assert not response.weapon_ids and not response.armor_ids
    assert item_num(REWARD) == 0
    assert db.get_item_detail(PLAYER, None, WEAPON_INSTANCE)
    assert db.get_item_detail(PLAYER, None, ARMOR_INSTANCE)

    prepare("pet")
    db.set_players_info(PLAYER, "pet_instance_id", PET_INSTANCE_1)
    before = database_snapshot()
    response = invoke("pet", request_for("pet"), 115)
    assert response.status == BAD_REQ
    assert database_snapshot() == before
    assert db.get_item_detail(PLAYER, None, PET_INSTANCE_1)
    assert db.get_item_detail(PLAYER, None, PET_INSTANCE_2)
