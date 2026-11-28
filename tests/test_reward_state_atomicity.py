import concurrent.futures
import pickle
import threading
from unittest.mock import patch

import pytest

# Keep the repository's established db -> protobuf import order.
import utils.db as db
from config import Config
from network.msg_id import MsgId
from proto.net_pb2 import (
    ActivitySandbagHitReq,
    ActivitySandbagRewardReq,
    ActivitySandbagRewardType,
    BattleEncounterStateUpdateReq,
    BattleState,
    EMissionType,
    FlagBattleStateUpdateReq,
    ItemDetail,
    RiddleStateUpdateReq,
    StatusCode,
)
import handlers.ActivitySandbagHit as sandbag_hit
import handlers.ActivitySandbagReward as sandbag_reward
import handlers.BattleEncounterStateUpdate as encounter
import handlers.FlagBattleStateUpdate as flag_battle
import handlers.RiddleStateUpdate as riddle
from utils import battle_rewards, flag_state, item_mgr, res_loader


PLAYER = 1
REWARD_ITEM = 102
REWARD_COUNT = 5
OK = StatusCode.StatusCode_OK
BAD_REQ = StatusCode.StatusCode_BAD_REQ
REPEAT = StatusCode.StatusCode_REPEAT_GET_REWARD


class FakeSession:
    def __init__(self, player_id=PLAYER, scene_id=9999):
        self.player_id = player_id
        self.scene_id = scene_id
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
    Config.DB_PATH = str(tmp_path / "reward-state.db")
    Config.IN_MEMORY = False
    db.init()
    db.db.execute(
        "INSERT INTO players(player_id,player_name,level,world_level,team,unlock_functions) "
        "VALUES(?,?,10,1,?,?)",
        (PLAYER, "reward-state", pickle.dumps((0, 0, 0)), pickle.dumps([])),
    )
    monkeypatch.setattr(riddle, "resolve_reward", lambda *args: [(REWARD_ITEM, REWARD_COUNT)])
    monkeypatch.setattr(sandbag_reward, "resolve_reward", lambda *args: [(REWARD_ITEM, REWARD_COUNT)])
    monkeypatch.setattr(flag_battle, "resolve_drop", lambda *args: [(REWARD_ITEM, REWARD_COUNT)])
    monkeypatch.setattr(item_mgr.achieve_track, "report", lambda *args, **kwargs: None)
    monkeypatch.setattr(item_mgr.achieve_track, "add", lambda *args, **kwargs: None)
    monkeypatch.setattr(item_mgr.achieve_track, "scenic", lambda *args, **kwargs: None)
    monkeypatch.setattr(item_mgr.achieve_track, "explore_collect", lambda *args, **kwargs: None)

    def grant_exp(session, kind, battle_id):
        db.set_kv(session.player_id, "test_exp", f"{kind}:{battle_id}", 1)

    monkeypatch.setattr(battle_rewards, "grant_character_exp", grant_exp)
    yield
    db.exit()
    Config.DB_PATH, Config.IN_MEMORY = old_path, old_memory


def item_num(item_id=REWARD_ITEM):
    raw = db.get_item_detail(PLAYER, item_id)
    if not raw:
        return 0
    detail = ItemDetail.FromString(raw)
    return detail.main_item.base_item.num


def database_snapshot():
    tables = [
        row[0]
        for row in db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    ]
    return tuple(
        (table, tuple(sorted((repr(row) for row in db.conn.execute(f'SELECT * FROM "{table}"').fetchall()))))
        for table in tables
    )


def invoke(handler, session, request, packet_id):
    handler.Handler().handle(session, request.SerializeToString(), packet_id)
    return session


def riddle_start(session=None, packet_id=10):
    session = session or FakeSession()
    return invoke(
        riddle,
        session,
        RiddleStateUpdateReq(riddle_id=100001, battle_state=BattleState.Start),
        packet_id,
    )


def riddle_finish(session=None, packet_id=11):
    session = session or FakeSession()
    invoke(
        riddle,
        session,
        RiddleStateUpdateReq(
            riddle_id=100001, battle_state=BattleState.Finish, is_win=True
        ),
        packet_id,
    )
    return session


def flag_case():
    row = next(
        row
        for row in res_loader.res["Flag"]["flag"]["datas"]
        if any(g.get("relation_i_d") == 110300 for g in row.get("flag_group_info", ()))
    )
    session = FakeSession(scene_id=row["scene_i_d"])
    flag_state.unlock_flags(session, [row["i_d"]], notify=False)
    return session, row


def flag_start(session, packet_id=20):
    return invoke(
        flag_battle,
        session,
        FlagBattleStateUpdateReq(battle_id=110300, battle_state=BattleState.Start),
        packet_id,
    )


def flag_finish(session, packet_id=21):
    return invoke(
        flag_battle,
        session,
        FlagBattleStateUpdateReq(battle_id=110300, battle_state=BattleState.Finish),
        packet_id,
    )


def encounter_start(session=None, packet_id=30):
    session = session or FakeSession()
    return invoke(
        encounter,
        session,
        BattleEncounterStateUpdateReq(encounter_id=1073, battle_state=BattleState.Start),
        packet_id,
    )


def encounter_finish(session=None, packet_id=31):
    session = session or FakeSession()
    invoke(
        encounter,
        session,
        BattleEncounterStateUpdateReq(encounter_id=1073, battle_state=BattleState.Finish),
        packet_id,
    )
    return session


def sandbag_drop(session=None, packet_id=40):
    session = session or FakeSession()
    invoke(
        sandbag_hit,
        session,
        ActivitySandbagHitReq(activity_id=10001, npc_id=10301, hit_count=1),
        packet_id,
    )
    return session


def sandbox_claim(session=None, seq=1, packet_id=41, activity_id=10001, reward_type=ActivitySandbagRewardType.RewardType_Box):
    session = session or FakeSession()
    invoke(
        sandbag_reward,
        session,
        ActivitySandbagRewardReq(
            activity_id=activity_id, reward_type=reward_type, seq_list=[seq]
        ),
        packet_id,
    )
    return session


def concurrently(calls):
    barrier = threading.Barrier(len(calls))

    def run(call):
        barrier.wait()
        return call()

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(calls)) as executor:
        return list(executor.map(run, calls))


def test_riddle_first_clear_commits_qualification_claim_and_reward():
    session = riddle_finish(riddle_start(), 11)
    assert session.response(MsgId.RiddleStateUpdateRsp).status == OK
    assert item_num() == REWARD_COUNT
    assert db.get_kv(PLAYER, "riddle", 100001) == 1
    assert len(db.get_claimed(PLAYER, "riddle_first")) == 1
    assert len(db.get_claimed(PLAYER, "riddle_finish")) == 1


def test_flag_first_clear_commits_experience_flag_and_box_once():
    session, row = flag_case()
    flag_start(session)
    flag_finish(session)
    rsp = session.response(MsgId.FlagBattleStateUpdateRsp)
    assert rsp.status == OK
    assert rsp.dynamic_treasure_box_base_info.box_id
    assert db.get_kv(PLAYER, "test_exp", "flag:110300") == 1
    assert row["i_d"] not in flag_state.visible_ids(PLAYER)
    assert db.get_kv(PLAYER, "dyn_box", rsp.dynamic_treasure_box_base_info.index)


def test_encounter_first_clear_commits_state_and_experience_once():
    session = encounter_finish(encounter_start())
    assert session.response(MsgId.BattleEncounterStateUpdateRsp).status == OK
    assert db.get_encounter_states(PLAYER)[1073][0] == BattleState.Finish
    assert db.get_kv(PLAYER, "test_exp", "encounter:1073") == 1


def test_sandbag_box_reward_commits_used_claim_and_items_once():
    sandbag_drop()
    session = sandbox_claim()
    assert session.response(MsgId.ActivitySandbagRewardRsp).status == OK
    assert item_num() == REWARD_COUNT
    with sandbag_hit.lock_sandbag:
        state = sandbag_hit.player_state(PLAYER)
    assert state["boxes"][0]["used"] is True
    assert db.has_claimed(PLAYER, "sandbag_box", "10001:1")


def test_sandbag_server_tier_without_threshold_is_claimable_once():
    session = sandbox_claim(
        reward_type=ActivitySandbagRewardType.RewardType_Server,
        seq=1,
    )
    assert session.response(MsgId.ActivitySandbagRewardRsp).status == OK
    assert session.response(MsgId.ActivitySandbagRewardRsp).rewards
    assert session.messages(MsgId.PackNotice)
    assert db.has_claimed(PLAYER, "sandbag_server", "10001:1")


@pytest.mark.parametrize("kind", ["riddle", "flag", "encounter", "sandbag-hit", "sandbox"])
def test_same_packet_fingerprint_replays_response_without_side_effects(kind):
    if kind == "riddle":
        riddle_start()
        first = riddle_finish(packet_id=101)
        request = RiddleStateUpdateReq(riddle_id=100001, battle_state=BattleState.Finish, is_win=True)
        handler, mid = riddle, MsgId.RiddleStateUpdateRsp
        replay = lambda: riddle_finish(packet_id=101)
    elif kind == "flag":
        starter, _ = flag_case()
        flag_start(starter)
        first = flag_finish(starter, 101)
        request = FlagBattleStateUpdateReq(battle_id=110300, battle_state=BattleState.Finish)
        handler, mid = flag_battle, MsgId.FlagBattleStateUpdateRsp
        replay = lambda: flag_finish(starter, 101)
    elif kind == "encounter":
        encounter_start()
        first = encounter_finish(packet_id=101)
        request = BattleEncounterStateUpdateReq(encounter_id=1073, battle_state=BattleState.Finish)
        handler, mid = encounter, MsgId.BattleEncounterStateUpdateRsp
        replay = lambda: encounter_finish(packet_id=101)
    elif kind == "sandbag-hit":
        first = sandbag_drop(packet_id=101)
        request = ActivitySandbagHitReq(activity_id=10001, npc_id=10301, hit_count=1)
        handler, mid = sandbag_hit, MsgId.ActivitySandbagHitRsp
        replay = lambda: sandbag_drop(packet_id=101)
    else:
        sandbag_drop()
        first = sandbox_claim(packet_id=101)
        request = ActivitySandbagRewardReq(activity_id=10001, seq_list=[1])
        handler, mid = sandbag_reward, MsgId.ActivitySandbagRewardRsp
        replay = lambda: sandbox_claim(packet_id=101)
    before = database_snapshot()
    original = first.response(mid).SerializeToString()
    original_packs = [m.SerializeToString() for m in first.messages(MsgId.PackNotice)]
    original_drops = [m.SerializeToString() for m in first.messages(MsgId.ActivitySandbagDropNotice)]
    replayed = replay()
    assert replayed.response(mid).SerializeToString() == original
    assert [m.SerializeToString() for m in replayed.messages(MsgId.PackNotice)] == original_packs
    assert [m.SerializeToString() for m in replayed.messages(MsgId.ActivitySandbagDropNotice)] == original_drops
    assert database_snapshot() == before

    # Same packet ID with a different payload must not mutate state either.
    changed = type(request)()
    changed.CopyFrom(request)
    if kind in ("riddle", "flag", "encounter"):
        if hasattr(changed, "riddle_id"):
            changed.riddle_id = 100002
        elif hasattr(changed, "battle_id"):
            changed.battle_id = 110220
        else:
            changed.battle_state = BattleState.Fail
    elif kind == "sandbag-hit":
        changed.hit_count = 2
    else:
        changed.seq_list.append(2)
    before = database_snapshot()
    conflict = FakeSession()
    invoke(handler, conflict, changed, 101)
    assert conflict.response(mid).status == BAD_REQ
    assert database_snapshot() == before


def test_concurrent_riddle_finish_with_distinct_packets_grants_first_clear_once():
    riddle_start()
    sessions = [FakeSession() for _ in range(8)]
    concurrently([
        (lambda session=session, packet=200 + index: riddle_finish(session, packet))
        for index, session in enumerate(sessions)
    ])
    assert sum(s.response(MsgId.RiddleStateUpdateRsp).status == OK for s in sessions) == 1
    assert item_num() == REWARD_COUNT
    assert len(db.get_claimed(PLAYER, "riddle_finish")) == 1


def test_concurrent_same_riddle_packet_returns_one_committed_response():
    riddle_start()
    sessions = [FakeSession() for _ in range(8)]
    concurrently([lambda session=session: riddle_finish(session, 300) for session in sessions])
    responses = [s.response(MsgId.RiddleStateUpdateRsp) for s in sessions]
    assert all(rsp.status == OK for rsp in responses)
    assert len({rsp.SerializeToString() for rsp in responses}) == 1
    assert item_num() == REWARD_COUNT


def test_concurrent_sandbag_claims_with_distinct_packets_grant_once():
    sandbag_drop()
    sessions = [FakeSession() for _ in range(8)]
    concurrently([
        (lambda session=session, packet=400 + index: sandbox_claim(session, packet_id=packet))
        for index, session in enumerate(sessions)
    ])
    assert sum(s.response(MsgId.ActivitySandbagRewardRsp).status == OK for s in sessions) == 1
    assert item_num() == REWARD_COUNT


@pytest.mark.parametrize("kind", ["riddle", "flag", "encounter"])
def test_client_finish_without_server_start_fails_closed(kind):
    if kind == "riddle":
        session = riddle_finish()
        mid = MsgId.RiddleStateUpdateRsp
    elif kind == "flag":
        session, _ = flag_case()
        session = flag_finish(session)
        mid = MsgId.FlagBattleStateUpdateRsp
    else:
        session = encounter_finish()
        mid = MsgId.BattleEncounterStateUpdateRsp
    assert session.response(mid).status != OK
    assert item_num() == 0
    assert db.get_kv(PLAYER, "test_exp", f"{kind}:{'100001' if kind == 'riddle' else '110300' if kind == 'flag' else '1073'}") is None


def test_inconsistent_riddle_win_and_unknown_battles_fail_closed():
    session = FakeSession()
    invoke(riddle, session, RiddleStateUpdateReq(riddle_id=100001, battle_state=BattleState.Finish), 1)
    assert session.response(MsgId.RiddleStateUpdateRsp).status == BAD_REQ
    invoke(encounter, session, BattleEncounterStateUpdateReq(encounter_id=999999, battle_state=BattleState.Finish), 2)
    assert session.response(MsgId.BattleEncounterStateUpdateRsp).status == StatusCode.StatusCode_CFG_NOT_FOUND
    assert item_num() == 0


def test_sandbag_nonexistent_box_cross_activity_and_unproven_server_tier_fail_closed():
    assert sandbox_claim(seq=999).response(MsgId.ActivitySandbagRewardRsp).status == REPEAT
    sandbag_drop()
    assert sandbox_claim(activity_id=10002).response(MsgId.ActivitySandbagRewardRsp).status == StatusCode.StatusCode_CFG_NOT_FOUND
    assert sandbox_claim(seq=2, reward_type=ActivitySandbagRewardType.RewardType_Server).response(MsgId.ActivitySandbagRewardRsp).status == StatusCode.StatusCode_NotFinishAllTask
    assert item_num() == 0
    with sandbag_hit.lock_sandbag:
        assert sandbag_hit.player_state(PLAYER)["boxes"][0]["used"] is False


def test_unknown_sandbag_activity_does_not_fall_back_to_first_config():
    session = sandbag_drop()
    invoke(sandbag_hit, session, ActivitySandbagHitReq(activity_id=999, npc_id=10301, hit_count=1), 2)
    assert session.response(MsgId.ActivitySandbagHitRsp).status == StatusCode.StatusCode_CFG_NOT_FOUND


@pytest.mark.parametrize("kind", ["riddle", "sandbox"])
def test_grant_failure_rolls_back_state_claim_reward_and_response(kind, monkeypatch):
    if kind == "riddle":
        riddle_start()
        call = lambda: riddle_finish()
        mid = MsgId.RiddleStateUpdateRsp
    else:
        sandbag_drop()
        call = lambda: sandbox_claim()
        mid = MsgId.ActivitySandbagRewardRsp
    before = database_snapshot()
    original = item_mgr.grant_items

    def grant_then_fail(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("synthetic grant failure")

    session = FakeSession()
    monkeypatch.setattr(item_mgr, "grant_items", grant_then_fail)
    with pytest.raises(RuntimeError, match="synthetic grant failure"):
        if kind == "riddle":
            riddle_finish(session)
        else:
            sandbox_claim(session)
    assert database_snapshot() == before
    assert session.sent == []


@pytest.mark.parametrize("kind", ["riddle", "flag", "encounter", "sandbox"])
def test_state_save_failure_rolls_back_rewards_and_claims(kind, monkeypatch):
    if kind == "riddle":
        riddle_start()
        call = lambda session: riddle_finish(session)
        mid = MsgId.RiddleStateUpdateRsp
    elif kind == "flag":
        starter, _ = flag_case()
        flag_start(starter)
        call = lambda session: flag_finish(session)
        mid = MsgId.FlagBattleStateUpdateRsp
    elif kind == "encounter":
        encounter_start()
        call = lambda session: encounter_finish(session)
        mid = MsgId.BattleEncounterStateUpdateRsp
    else:
        sandbag_drop()
        call = lambda session: sandbox_claim(session)
        mid = MsgId.ActivitySandbagRewardRsp
    before = database_snapshot()
    original = db.set_kv

    def fail_operation_save(player_id, ns, key, value):
        result = original(player_id, ns, key, value)
        if ns == "activity_operations":
            raise RuntimeError("synthetic state save failure")
        return result

    monkeypatch.setattr(db, "set_kv", fail_operation_save)
    session = FakeSession()
    with pytest.raises(RuntimeError, match="synthetic state save failure"):
        call(session)
    assert database_snapshot() == before
    assert session.sent == []


def test_flag_state_failure_after_experience_rolls_back_box_and_flag_removal(monkeypatch):
    starter, row = flag_case()
    flag_start(starter)
    before = database_snapshot()
    original = db.set_kv

    def fail_box_save(player_id, ns, key, value):
        result = original(player_id, ns, key, value)
        if ns == "dyn_box" and key != "seq":
            raise RuntimeError("synthetic box save failure")
        return result

    monkeypatch.setattr(db, "set_kv", fail_box_save)
    session = FakeSession(scene_id=starter.scene_id)
    with pytest.raises(RuntimeError, match="synthetic box save failure"):
        flag_finish(session)
    assert database_snapshot() == before
    assert row["i_d"] in flag_state.visible_ids(PLAYER)
    assert session.sent == []


def test_encounter_state_failure_after_experience_rolls_back_everything(monkeypatch):
    encounter_start()
    before = database_snapshot()
    original = db.set_encounter_state

    def fail_finish_save(*args, **kwargs):
        result = original(*args, **kwargs)
        if args[2] == BattleState.Finish:
            raise RuntimeError("synthetic encounter save failure")
        return result

    monkeypatch.setattr(db, "set_encounter_state", fail_finish_save)
    session = FakeSession()
    with pytest.raises(RuntimeError, match="synthetic encounter save failure"):
        encounter_finish(session)
    assert database_snapshot() == before
    assert session.sent == []


def test_sandbag_state_save_failure_rolls_back_hit_and_allows_retry(monkeypatch):
    before = database_snapshot()
    original = sandbag_hit.sb_save

    def fail_save(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("synthetic sandbag save failure")

    monkeypatch.setattr(sandbag_hit, "sb_save", fail_save)
    # Reward imported sb_save directly, so patch both references for this test.
    monkeypatch.setattr(sandbag_reward, "sb_save", fail_save)
    session = FakeSession()
    with pytest.raises(RuntimeError, match="synthetic sandbag save failure"):
        sandbag_drop(session)
    assert database_snapshot() == before
    assert session.sent == []
    monkeypatch.setattr(sandbag_hit, "sb_save", original)
    assert sandbag_drop().response(MsgId.ActivitySandbagHitRsp).status == OK


def test_restart_replays_sandbag_packet_and_new_packet_cannot_claim_twice():
    sandbag_drop()
    first = sandbox_claim(packet_id=501)
    assert first.response(MsgId.ActivitySandbagRewardRsp).status == OK
    assert item_num() == REWARD_COUNT
    Config.DB_PATH = str(Config.DB_PATH)
    db.exit()
    db.init()
    replay = sandbox_claim(packet_id=501)
    assert replay.response(MsgId.ActivitySandbagRewardRsp).status == OK
    assert item_num() == REWARD_COUNT
    duplicate = sandbox_claim(packet_id=502)
    assert duplicate.response(MsgId.ActivitySandbagRewardRsp).status == REPEAT
    assert item_num() == REWARD_COUNT
    assert not duplicate.messages(MsgId.PackNotice)


def test_restart_preserves_unclaimed_sandbag_box_and_hit_idempotency():
    first_hit = sandbag_drop(packet_id=601)
    db.exit()
    db.init()
    replay_hit = sandbag_drop(packet_id=601)
    assert replay_hit.response(MsgId.ActivitySandbagHitRsp).SerializeToString() == first_hit.response(MsgId.ActivitySandbagHitRsp).SerializeToString()
    assert len(replay_hit.messages(MsgId.ActivitySandbagDropNotice)) == 1
    claim = sandbox_claim(packet_id=602)
    assert claim.response(MsgId.ActivitySandbagRewardRsp).status == OK
    assert item_num() == REWARD_COUNT
