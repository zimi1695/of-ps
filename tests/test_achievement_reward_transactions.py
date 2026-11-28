import concurrent.futures
import contextlib
import pickle
import threading
from dataclasses import dataclass
from unittest.mock import patch

import pytest

from config import Config
import utils.db as db
from network.msg_id import MsgId
from proto.net_pb2 import (
    Achieve,
    Character,
    EArchaeologyType,
    GetAchieveGroupAwardReq,
    GetCharacterAchievementAwardReq,
    GetCharacterAchievementBadgeAwardReq,
    GetLifeAchievementRewardReq,
    ManualFlagRewardReq,
    PlayerLevelRewardReq,
    StatusCode,
    TreasureIllustrationRewardReq,
)
import handlers.GetAchieveGroupAward as achieve_group
import handlers.GetCharacterAchievementAward as character_award
import handlers.GetCharacterAchievementBadgeAward as character_badge
import handlers.GetLifeAchievementReward as life_award
import handlers.ManualFlagReward as manual_flag
import handlers.PlayerLevelReward as player_level
import handlers.TreasureIllustrationReward as treasure
from server import activities
from utils import achieve_track, flag_state, item_mgr, res_loader


PLAYER = 1
REWARD = 102
OK = StatusCode.StatusCode_OK
BAD_REQ = StatusCode.StatusCode_BAD_REQ
FAIL = StatusCode.StatusCode_FAIL
GROUP_ID = 60010000
LIFE_ACHIEVE_ID = 150220011
CHARACTER_ID = 101001
BADGE_ACHIEVE_ID = 41010010
FLAG_ID = 15101


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

    def messages(self, message_id):
        return [message for mid, message, _ in self.sent if mid == message_id]


@pytest.fixture(scope="module", autouse=True)
def resources():
    res_loader.init()


@pytest.fixture(autouse=True)
def game_db(tmp_path, monkeypatch):
    old_path, old_memory = Config.DB_PATH, Config.IN_MEMORY
    db.exit()
    Config.DB_PATH = str(tmp_path / "achievement-rewards.db")
    Config.IN_MEMORY = False
    db.init()
    db.db.execute(
        "INSERT INTO players(player_id,player_name,level,world_level,unlock_functions) "
        "VALUES(?,?,10,1,?)",
        (PLAYER, "achievement-rewards", pickle.dumps([])),
    )
    monkeypatch.setattr(achieve_track, "report", lambda *args, **kwargs: None)
    yield
    db.exit()
    Config.DB_PATH, Config.IN_MEMORY = old_path, old_memory


def item_num(item_id):
    raw = db.get_item_detail(PLAYER, item_id)
    if not raw:
        return 0
    detail = item_mgr.ItemDetail.FromString(raw)
    if detail.main_item.WhichOneof("item") != "base_item":
        return 1
    return detail.main_item.base_item.num


def database_snapshot():
    return tuple(db.conn.iterdump())


def set_character():
    db.set_character(
        PLAYER,
        CHARACTER_ID,
        Character(character_id=CHARACTER_ID).SerializeToString(),
    )


def set_achieve(achieve_id, count):
    db.set_achieve(
        PLAYER,
        achieve_id,
        Achieve(achieve_id=achieve_id, count=count).SerializeToString(),
    )


def group_conditions():
    return achieve_group._group_config(GROUP_ID)[1]


def prepare_group():
    for achieve_id, target in group_conditions():
        set_achieve(achieve_id, target)


def revoke_group():
    for index, (achieve_id, target) in enumerate(group_conditions()):
        set_achieve(achieve_id, target - 1 if index == 0 else target)


def prepare_life():
    set_achieve(LIFE_ACHIEVE_ID, achieve_track.target_of(LIFE_ACHIEVE_ID))


def revoke_life():
    set_achieve(LIFE_ACHIEVE_ID, achieve_track.target_of(LIFE_ACHIEVE_ID) - 1)


def prepare_character_award():
    set_character()
    item_mgr.grant_items(PLAYER, [(51101, 1)])


def revoke_character_award():
    item_mgr.consume_items(PLAYER, [(51101, item_num(51101))])


def prepare_character_badge():
    set_character()
    set_achieve(BADGE_ACHIEVE_ID, achieve_track.target_of(BADGE_ACHIEVE_ID))


def revoke_character_badge():
    set_achieve(BADGE_ACHIEVE_ID, achieve_track.target_of(BADGE_ACHIEVE_ID) - 1)


def prepare_manual():
    flag_state.unlock_flags(
        FakeSession(), [FLAG_ID], namespace=Config.MANUAL_FLAGS_NS, notify=False
    )


def revoke_manual():
    db.set_kv(PLAYER, Config.MANUAL_FLAGS_NS, "ids", b"")


def prepare_player():
    pass


def revoke_player():
    db.set_players_info(PLAYER, "level", 9)


def prepare_treasure():
    db.set_kv(PLAYER, "archaeology", "5090001", 1)


def revoke_treasure():
    db.set_kv(PLAYER, "archaeology", "5090001", 0)
    db.set_kv(PLAYER, "archaeology", "5090002", 1)


@dataclass
class HandlerCase:
    name: str
    module: object
    request: object
    altered_request: object
    response_id: int
    operation_system: str
    claim_system: str
    claim_key: str
    reward_count: int
    prepare: object
    revoke: object
    duplicate_status: int = StatusCode.StatusCode_REPEAT_GET_REWARD
    state_save: object = None
    reward_item: int = REWARD


CASES = (
    HandlerCase(
        "achieve-group",
        achieve_group,
        GetAchieveGroupAwardReq(group_id=GROUP_ID),
        GetAchieveGroupAwardReq(group_id=60020000),
        MsgId.GetAchieveGroupAwardRsp,
        achieve_group._OPERATION_SYSTEM,
        "achieve_group",
        str(GROUP_ID),
        5,
        prepare_group,
        revoke_group,
    ),
    HandlerCase(
        "life-achievement",
        life_award,
        GetLifeAchievementRewardReq(achieve_id=LIFE_ACHIEVE_ID),
        GetLifeAchievementRewardReq(achieve_id=LIFE_ACHIEVE_ID + 1),
        MsgId.GetLifeAchievementRewardRsp,
        life_award._OPERATION_SYSTEM,
        "life_achieve",
        str(LIFE_ACHIEVE_ID),
        5,
        prepare_life,
        revoke_life,
    ),
    HandlerCase(
        "character-achievement",
        character_award,
        GetCharacterAchievementAwardReq(character_id=CHARACTER_ID, reward_index=1),
        GetCharacterAchievementAwardReq(character_id=CHARACTER_ID, reward_index=2),
        MsgId.GetCharacterAchievementAwardRsp,
        character_award._OPERATION_SYSTEM,
        "chr_achieve",
        f"{CHARACTER_ID}:1",
        10,
        prepare_character_award,
        revoke_character_award,
        state_save=(db, "set_character"),
    ),
    HandlerCase(
        "character-badge",
        character_badge,
        GetCharacterAchievementBadgeAwardReq(
            character_id=CHARACTER_ID, achievement_id=BADGE_ACHIEVE_ID
        ),
        GetCharacterAchievementBadgeAwardReq(
            character_id=CHARACTER_ID, achievement_id=BADGE_ACHIEVE_ID + 1
        ),
        MsgId.GetCharacterAchievementBadgeAwardRsp,
        character_badge._OPERATION_SYSTEM,
        "chr_badge",
        f"{CHARACTER_ID}:{BADGE_ACHIEVE_ID}",
        1,
        prepare_character_badge,
        revoke_character_badge,
        state_save=(db, "set_character"),
        reward_item=51101,
    ),
    HandlerCase(
        "manual-flag",
        manual_flag,
        ManualFlagRewardReq(flag_id=FLAG_ID),
        ManualFlagRewardReq(flag_id=FLAG_ID + 1),
        MsgId.ManualFlagRewardRsp,
        manual_flag._OPERATION_SYSTEM,
        "manual_flag",
        str(FLAG_ID),
        5,
        prepare_manual,
        revoke_manual,
        duplicate_status=StatusCode.StatusCode_MANUAL_REWARDED,
    ),
    HandlerCase(
        "player-level",
        player_level,
        PlayerLevelRewardReq(level=10),
        PlayerLevelRewardReq(level=9),
        MsgId.PlayerLevelRewardRsp,
        player_level._OPERATION_SYSTEM,
        "player_level",
        "10",
        7,
        prepare_player,
        revoke_player,
    ),
    HandlerCase(
        "treasure-illustration",
        treasure,
        TreasureIllustrationRewardReq(
            archaeology_type=EArchaeologyType.EArchaeologyType_Book, id=1
        ),
        TreasureIllustrationRewardReq(
            archaeology_type=EArchaeologyType.EArchaeologyType_Book, id=2
        ),
        MsgId.TreasureIllustrationRewardRsp,
        treasure._OPERATION_SYSTEM,
        "treasure_illustration",
        "0:1",
        5,
        prepare_treasure,
        revoke_treasure,
    ),
)


@contextlib.contextmanager
def configured(case):
    with contextlib.ExitStack() as stack:
        if case.name in ("achieve-group", "life-achievement", "treasure-illustration"):
            stack.enter_context(
                patch.object(case.module, "resolve_reward", return_value=[(REWARD, case.reward_count)])
            )
        elif case.name == "manual-flag":
            stack.enter_context(
                patch.object(manual_flag, "flag_reward_list", return_value=[(REWARD, case.reward_count)])
            )
        elif case.name == "player-level":
            stack.enter_context(
                patch.dict(
                    res_loader.res,
                    {"Level": {"player_level": {"datas": [{"i_d": 10, "reward_i_d": 1}]}}},
                )
            )
            stack.enter_context(
                patch.object(player_level, "resolve_reward", return_value=[(REWARD, case.reward_count)])
            )
        yield


def invoke(case, session, packet_id, request=None):
    request = case.request if request is None else request
    with configured(case):
        case.module.Handler().handle(session, request.SerializeToString(), packet_id)
    return session.response(case.response_id)


def operation_key(case, packet_id):
    return f"{case.operation_system}:0:{packet_id}"


def assert_success(case, packet_id):
    assert item_num(case.reward_item) == case.reward_count
    assert db.has_claimed(PLAYER, case.claim_system, case.claim_key)
    assert db.get_kv(PLAYER, activities._OPERATION_NS, operation_key(case, packet_id)) is not None
    if case.name == "character-achievement":
        chr = Character.FromString(db.get_characters(PLAYER, CHARACTER_ID)[0])
        assert list(chr.reward_index_lst) == [1]
    elif case.name == "character-badge":
        chr = Character.FromString(db.get_characters(PLAYER, CHARACTER_ID)[0])
        assert list(chr.rewarded_achievement_id_lst) == [BADGE_ACHIEVE_ID]


def concurrently(calls):
    barrier = threading.Barrier(len(calls))

    def invoke_call(call):
        barrier.wait()
        return call()

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(calls)) as executor:
        return list(executor.map(invoke_call, calls))


def raise_after(original, message):
    def wrapped(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError(message)

    return wrapped


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_success_commits_reward_claim_state_and_response(case):
    case.prepare()
    session = FakeSession()
    response = invoke(case, session, 101)
    assert response.status == OK
    assert_success(case, 101)
    assert len(session.messages(MsgId.PackNotice)) == 1
    assert session.messages(MsgId.PackNotice)[0].items


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_same_packet_replays_response_without_side_effects(case):
    case.prepare()
    first_session = FakeSession()
    first = invoke(case, first_session, 102)
    first_pack = first_session.messages(MsgId.PackNotice)[0].SerializeToString()
    committed = database_snapshot()

    replay_session = FakeSession()
    replay = invoke(case, replay_session, 102)
    assert replay.SerializeToString() == first.SerializeToString()
    assert replay_session.messages(MsgId.PackNotice)[0].SerializeToString() == first_pack
    assert database_snapshot() == committed


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_same_packet_with_different_fingerprint_conflicts(case):
    case.prepare()
    assert invoke(case, FakeSession(), 103).status == OK
    committed = database_snapshot()
    session = FakeSession()
    response = invoke(case, session, 103, case.altered_request)
    assert response.status == BAD_REQ
    assert database_snapshot() == committed
    assert not session.messages(MsgId.PackNotice)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_concurrent_same_packet_applies_exactly_once(case):
    case.prepare()
    sessions = [FakeSession() for _ in range(6)]
    responses = concurrently([
        (lambda session=session: invoke(case, session, 104))
        for session in sessions
    ])
    assert [response.status for response in responses] == [OK] * 6
    assert len({response.SerializeToString() for response in responses}) == 1
    assert_success(case, 104)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_concurrent_distinct_packets_claim_once(case):
    case.prepare()
    responses = concurrently([
        (lambda index=index: invoke(case, FakeSession(), 200 + index).status)
        for index in range(6)
    ])
    assert responses.count(OK) == 1
    assert responses.count(case.duplicate_status) == 5
    assert item_num(case.reward_item) == case.reward_count
    assert db.has_claimed(PLAYER, case.claim_system, case.claim_key)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_missing_server_qualification_fails_closed(case):
    case.prepare()
    case.revoke()
    before = database_snapshot()
    session = FakeSession()
    response = invoke(case, session, 105)
    assert response.status == FAIL
    assert database_snapshot() == before
    assert not db.has_claimed(PLAYER, case.claim_system, case.claim_key)
    assert not session.messages(MsgId.PackNotice)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_grant_failure_after_claim_rolls_back_and_can_retry(case):
    case.prepare()
    before = database_snapshot()
    session = FakeSession()
    original = item_mgr.grant_items
    with patch.object(
        item_mgr,
        "grant_items",
        side_effect=raise_after(original, "synthetic grant failure"),
    ):
        with pytest.raises(RuntimeError, match="synthetic grant failure"):
            invoke(case, session, 106)
    assert database_snapshot() == before
    assert session.sent == []
    assert invoke(case, FakeSession(), 106).status == OK
    assert_success(case, 106)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_state_or_response_save_failure_rolls_back_and_can_retry(case):
    case.prepare()
    before = database_snapshot()
    session = FakeSession()
    target, attr = case.state_save or (activities, "_save_operation")
    original = getattr(target, attr)
    with patch.object(
        target,
        attr,
        side_effect=raise_after(original, "synthetic save failure"),
    ):
        with pytest.raises(RuntimeError, match="synthetic save failure"):
            invoke(case, session, 107)
    assert database_snapshot() == before
    assert session.sent == []
    assert invoke(case, FakeSession(), 107).status == OK
    assert_success(case, 107)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_zero_packet_id_remains_supported_without_operation_record(case):
    case.prepare()
    response = invoke(case, FakeSession(), 0)
    assert response.status == OK
    assert item_num(case.reward_item) == case.reward_count
    assert db.has_claimed(PLAYER, case.claim_system, case.claim_key)
    assert db.get_kv(PLAYER, activities._OPERATION_NS, operation_key(case, 0)) is None


def test_treasure_furniture_requires_matching_fragment_quantity():
    item_mgr.grant_items(PLAYER, [(5060001, 49)])
    case = CASES[-1]
    request = TreasureIllustrationRewardReq(
        archaeology_type=EArchaeologyType.EArchaeologyType_Furniture, id=1
    )
    before = database_snapshot()
    assert invoke(case, FakeSession(), 301, request).status == FAIL
    assert database_snapshot() == before
    item_mgr.grant_items(PLAYER, [(5060001, 1)])
    assert invoke(case, FakeSession(), 302, request).status == OK
    assert item_num(case.reward_item) == case.reward_count


def test_paid_character_tier_requires_server_payment_unlock():
    case = CASES[2]
    case.prepare()
    item_mgr.grant_items(PLAYER, [(51101, 8)])
    request = GetCharacterAchievementAwardReq(character_id=CHARACTER_ID, reward_index=9)
    before = database_snapshot()
    assert invoke(case, FakeSession(), 303, request).status == FAIL
    assert database_snapshot() == before
