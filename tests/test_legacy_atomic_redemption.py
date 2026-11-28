"""Atomicity and packet-replay coverage for legacy tutorial/recall handlers."""

import concurrent.futures
import pickle
import threading

import pytest

import utils.db as db
from config import Config
from network.msg_id import MsgId
from proto.net_pb2 import (
    ActivityRecallRewardClaimReq,
    InheritCodeUseReq,
    ItemDetail,
    PackNotice,
    RecallCodeUseReq,
    StatusCode,
    TutorialReq,
)
import handlers.ActivityRecallRewardClaim as recall_claim
import handlers.InheritCodeUse as inherit_use
import handlers.RecallCodeUse as recall_use
import handlers.Tutorial as tutorial
from server import redemption
from utils import achieve_track, item_mgr, res_loader

PLAYER = 1
OK = StatusCode.StatusCode_OK
BAD_REQ = StatusCode.StatusCode_BAD_REQ
SYS_ERR = StatusCode.StatusCode_SYS_ERR
KINDS = ("tutorial", "recall-claim", "recall-code", "inherit-code")


class FakeSession:
    def __init__(self, player_id=PLAYER):
        self.player_id = player_id
        self.sent = []
        self.quests = {}
        self.temp_pack = []

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
    Config.DB_PATH = str(tmp_path / "legacy-atomic.db")
    Config.IN_MEMORY = False
    db.init()
    for player_id in range(1, 7):
        db.conn.execute(
            "INSERT INTO players(player_id,player_name,level,unlock_functions) "
            "VALUES(?,?,10,?)",
            (player_id, f"p{player_id}", pickle.dumps([])),
        )
    monkeypatch.setattr(Config, "SKIP_QUESTS", False)
    monkeypatch.setattr(Config, "INITIAL_UNLOCK_ITEMS", ())
    monkeypatch.setattr(Config, "TUTORIAL_UNLOCK_FUNCTIONS", ())
    monkeypatch.setattr(
        Config, "ACTIVITY_RECALL_REWARDS", {(1, 2): ((102, 4),)}
    )
    reports = []
    monkeypatch.setattr(
        achieve_track,
        "report",
        lambda session, event, **kwargs: reports.append(event),
    )
    yield reports
    db.exit()
    Config.DB_PATH, Config.IN_MEMORY = old_path, old_memory


def add_code(code, code_type, rewards="101:3", max_uses=0, expire_time=0):
    db.add_redeem_code(
        code, rewards, max_uses=max_uses, expire_time=expire_time, code_type=code_type
    )


def prepare(kind):
    if kind == "recall-code":
        add_code("RECALL-123", "recall")
    elif kind == "inherit-code":
        add_code("INHERIT-123", "inherit")


def request_for(kind, *, code=None, tutorial_id=9, activity_id=1, condition_id=2):
    if kind == "tutorial":
        return tutorial, MsgId.TutorialRsp, TutorialReq(
            tutorial_id=tutorial_id, tutorial_name="client text is not a reward"
        )
    if kind == "recall-claim":
        return (
            recall_claim,
            MsgId.ActivityRecallRewardClaimRsp,
            ActivityRecallRewardClaimReq(
                activity_id=activity_id, condition_id=condition_id
            ),
        )
    if kind == "recall-code":
        return (
            recall_use,
            MsgId.RecallCodeUseRsp,
            RecallCodeUseReq(code=code or "RECALL-123"),
        )
    return (
        inherit_use,
        MsgId.InheritCodeUseRsp,
        InheritCodeUseReq(code=code or "INHERIT-123"),
    )


def invoke(kind, packet_id, session=None, **request_kwargs):
    handler, response_id, request = request_for(kind, **request_kwargs)
    session = session or FakeSession()
    handler.Handler().handle(session, request.SerializeToString(), packet_id)
    return session, session.response(response_id)


def altered_request(kind):
    if kind == "tutorial":
        return {"tutorial_id": 8}
    if kind == "recall-claim":
        return {"activity_id": 1, "condition_id": 3}
    code = "OTHER-123"
    add_code(code, "recall" if kind == "recall-code" else "inherit")
    return {"code": code}


def item_num(player_id, item_id):
    raw = db.get_item_detail(player_id, item_id)
    if not raw:
        return 0
    detail = ItemDetail.FromString(raw)
    if detail.main_item.WhichOneof("item") != "base_item":
        return 1
    return detail.main_item.base_item.num


def snapshot():
    return tuple(db.conn.iterdump())


def sent_snapshot(session):
    return [
        (message_id, message.SerializeToString(), packet_id)
        for message_id, message, packet_id in session.sent
    ]


def concurrently(calls):
    barrier = threading.Barrier(len(calls))

    def run(call):
        barrier.wait()
        return call()

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(calls)) as executor:
        return list(executor.map(run, calls))


@pytest.mark.parametrize("kind", KINDS)
def test_success_and_same_packet_replay_are_identical_without_duplicate_grant(
    kind, monkeypatch, game_db
):
    reports = game_db
    prepare(kind)
    monkeypatch.setattr(tutorial, "resolve_reward", lambda *args: [(101, 3)])
    first_session, first = invoke(kind, 101)
    assert first.status == OK
    assert item_num(PLAYER, 101 if kind != "recall-claim" else 102) == (
        4 if kind == "recall-claim" else 3
    )
    committed = snapshot()

    replay_session, replay = invoke(kind, 101)
    assert replay.status == OK
    assert replay.SerializeToString() == first.SerializeToString()
    assert sent_snapshot(replay_session) == sent_snapshot(first_session)
    assert snapshot() == committed
    assert len(db.get_kv_keys(PLAYER, "activity_operations")) == 1
    if kind == "recall-claim":
        assert reports.count("recall_player") == 1
    if kind == "tutorial":
        assert db.get_kv(PLAYER, "tutorial", "9") == 1
    elif kind == "recall-claim":
        assert db.has_claimed(PLAYER, "activity_recall", "1:2")
    else:
        code = "RECALL-123" if kind == "recall-code" else "INHERIT-123"
        assert db.conn.execute(
            "SELECT used FROM redeem_codes WHERE code=?", (code,)
        ).fetchone()[0] == 1
        assert db.conn.execute("SELECT COUNT(*) FROM redeem_code_uses").fetchone()[0] == 1


@pytest.mark.parametrize("kind", KINDS)
def test_same_packet_with_different_request_is_rejected_without_side_effects(kind, monkeypatch):
    prepare(kind)
    altered = altered_request(kind)
    monkeypatch.setattr(tutorial, "resolve_reward", lambda *args: [(101, 3)])
    assert invoke(kind, 102)[1].status == OK
    committed = snapshot()
    session, response = invoke(kind, 102, **altered)
    assert response.status == BAD_REQ
    assert not session.messages(MsgId.PackNotice)
    assert snapshot() == committed


@pytest.mark.parametrize("kind", KINDS)
def test_concurrent_same_packet_grants_exactly_once(kind, monkeypatch):
    prepare(kind)
    monkeypatch.setattr(tutorial, "resolve_reward", lambda *args: [(101, 3)])
    results = concurrently(
        [lambda: invoke(kind, 103) for _ in range(6)]
    )
    assert [response.status for _, response in results] == [OK] * 6
    assert len({response.SerializeToString() for _, response in results}) == 1
    assert item_num(PLAYER, 101 if kind != "recall-claim" else 102) == (
        4 if kind == "recall-claim" else 3
    )
    assert len(db.get_kv_keys(PLAYER, "activity_operations")) == 1


@pytest.mark.parametrize("kind", KINDS)
def test_grant_failure_after_item_write_rolls_back_claim_counter_and_operation(
    kind, monkeypatch
):
    prepare(kind)
    monkeypatch.setattr(tutorial, "resolve_reward", lambda *args: [(101, 3)])
    before = snapshot()
    original = item_mgr.grant_items if kind == "tutorial" else db.grant_rewards

    def grant_then_fail(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("synthetic grant failure")

    if kind == "tutorial":
        monkeypatch.setattr(item_mgr, "grant_items", grant_then_fail)
    else:
        monkeypatch.setattr(db, "grant_rewards", grant_then_fail)
    session, response = invoke(kind, 104)
    assert response.status == SYS_ERR
    assert not session.messages(MsgId.PackNotice)
    assert snapshot() == before
    assert db.get_kv_keys(PLAYER, "activity_operations") == []


@pytest.mark.parametrize("kind", KINDS)
def test_operation_record_failure_rolls_back_item_write_and_claim(kind, monkeypatch):
    prepare(kind)
    monkeypatch.setattr(tutorial, "resolve_reward", lambda *args: [(101, 3)])
    before = snapshot()
    original = redemption._save_operation

    def save_then_fail(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("synthetic operation save failure")

    monkeypatch.setattr(redemption, "_save_operation", save_then_fail)
    session, response = invoke(kind, 105)
    assert response.status == SYS_ERR
    assert not session.messages(MsgId.PackNotice)
    assert snapshot() == before


@pytest.mark.parametrize("kind", ("recall-code", "inherit-code"))
@pytest.mark.parametrize(
    "case,expected",
    (
        ("missing", StatusCode.StatusCode_REDEMPTION_CODE_NOT_EXIST),
        ("malformed", StatusCode.StatusCode_REDEMPTION_CODE_FAIL),
        ("wrong-type", StatusCode.StatusCode_REDEMPTION_CODE_NOT_EXIST),
        ("expired", StatusCode.StatusCode_REDEMPTION_CODE_NOT_EXIST),
        ("exhausted", StatusCode.StatusCode_REDEMPTION_CODE_NUM_IS_MAX),
        ("used", StatusCode.StatusCode_REDEMPTION_CODE_USED),
    ),
)
def test_invalid_expired_exhausted_and_used_codes_never_grant(kind, case, expected):
    prepare(kind)
    code_type = "recall" if kind == "recall-code" else "inherit"
    if case == "missing":
        code = "MISSING-123"
    elif case == "malformed":
        code = "bad"
    elif case == "wrong-type":
        code = "WRONG-123"
        add_code(code, "redeem" if code_type == "recall" else "recall")
    elif case == "expired":
        code = "EXPIRED-123"
        add_code(code, code_type, expire_time=1)
    elif case == "exhausted":
        code = "EXHAUSTED-123"
        add_code(code, code_type, max_uses=1)
        db.conn.execute(
            "UPDATE redeem_codes SET used=1 WHERE code=?", (code,)
        )
    else:
        assert invoke(kind, 106)[1].status == OK
        code = "RECALL-123" if kind == "recall-code" else "INHERIT-123"
    before_items = item_num(PLAYER, 101)
    before_uses = db.conn.execute("SELECT COUNT(*) FROM redeem_code_uses").fetchone()[0]
    session, response = invoke(kind, 107, code=code)
    assert response.status == expected
    assert not session.messages(MsgId.PackNotice)
    assert item_num(PLAYER, 101) == before_items
    assert db.conn.execute("SELECT COUNT(*) FROM redeem_code_uses").fetchone()[0] == before_uses


@pytest.mark.parametrize("kind", ("recall-code", "inherit-code"))
def test_malformed_server_rewards_do_not_consume_code(kind):
    code_type = "recall" if kind == "recall-code" else "inherit"
    code = "BADCFG-123"
    add_code(code, code_type, rewards="101:0")
    _, response = invoke(kind, 108, code=code)
    assert response.status == StatusCode.StatusCode_REDEMPTION_CODE_FAIL
    assert db.conn.execute(
        "SELECT used FROM redeem_codes WHERE code=?", (code,)
    ).fetchone()[0] == 0
    assert db.conn.execute("SELECT COUNT(*) FROM redeem_code_uses").fetchone()[0] == 0
    # The only committed difference is the rejection audit event.
    assert item_num(PLAYER, 101) == 0


def test_recall_claim_missing_config_has_no_claim_or_operation(monkeypatch):
    monkeypatch.setattr(Config, "ACTIVITY_RECALL_REWARDS", {})
    before = snapshot()
    session, response = invoke("recall-claim", 109)
    assert response.status == StatusCode.StatusCode_CFG_NOT_FOUND
    assert not db.has_claimed(PLAYER, "activity_recall", "1:2")
    assert db.get_kv_keys(PLAYER, "activity_operations") == []
    assert snapshot() == before
    assert not session.messages(MsgId.PackNotice)


def test_recall_code_global_limit_is_atomic_across_players():
    add_code("LIMIT-123", "recall", max_uses=1)
    results = concurrently(
        [
            lambda player_id=player_id: invoke(
                "recall-code", 110, FakeSession(player_id), code="LIMIT-123"
            )
            for player_id in (1, 2)
        ]
    )
    assert sorted(response.status for _, response in results) == [
        OK,
        StatusCode.StatusCode_REDEMPTION_CODE_NUM_IS_MAX,
    ]
    assert item_num(1, 101) + item_num(2, 101) == 3
    assert db.conn.execute(
        "SELECT used FROM redeem_codes WHERE code='LIMIT-123'"
    ).fetchone()[0] == 1


def test_invite_relation_write_failure_rolls_back_and_allows_same_packet_retry(monkeypatch):
    redemption.register_invite_code("OWNER-123", 1)
    before = snapshot()
    original = db.set_kv

    def fail_invitees(player_id, ns, key, value):
        result = original(player_id, ns, key, value)
        if (player_id, ns, key) == (1, "invite", "invitees"):
            raise RuntimeError("synthetic relation write failure")
        return result

    monkeypatch.setattr(db, "set_kv", fail_invitees)
    with pytest.raises(RuntimeError, match="synthetic relation write failure"):
        redemption.use_invite_code(
            2, "OWNER-123", ((101, 3),), None, None, packet_id=111
        )
    assert snapshot() == before
    assert redemption.invite_count(1) == 0
    assert db.get_kv(2, "invite", "inviter") is None
    assert db.get_kv(0, "invite", "relation:2") is None
    assert item_num(2, 101) == 0

    monkeypatch.setattr(db, "set_kv", original)
    first_pack = PackNotice(status=OK)
    assert redemption.use_invite_code(
        2, "OWNER-123", ((101, 3),), None, first_pack, packet_id=111
    )[0] == "ok"
    committed = snapshot()
    replay_pack = PackNotice()
    assert redemption.use_invite_code(
        2, "OWNER-123", ((101, 3),), None, replay_pack, packet_id=111
    ) == ("ok", 1)
    assert replay_pack.SerializeToString() == first_pack.SerializeToString()
    assert snapshot() == committed
    assert redemption.invite_count(1) == 1
    assert item_num(2, 101) == 3
    assert redemption.use_invite_code(
        2, "UNKNOWN-123", ((101, 3),), None, PackNotice(), packet_id=111
    )[0] == "conflict"
    assert snapshot() == committed


def test_tutorial_reward_uses_server_config_not_client_name(monkeypatch):
    monkeypatch.setattr(tutorial, "resolve_reward", lambda *args: [(101, 3)])
    session, response = invoke(
        "tutorial", 112, tutorial_id=9, activity_id=0, condition_id=0
    )
    assert response.status == OK
    assert item_num(PLAYER, 101) == 3
    assert item_num(PLAYER, 4050) == 1
    assert item_num(PLAYER, 999999) == 0
    assert len(session.messages(MsgId.QuestNotice)) == 1
