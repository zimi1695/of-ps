import concurrent.futures
import pickle
import threading

import pytest

# Keep the repository's established db -> protobuf import order.
import utils.db as db
from config import Config
from network.msg_id import MsgId
from proto.net_pb2 import (
    EBagItemTag,
    ItemDetail,
    ItemSweepReq,
    MailBriefData,
    MailState,
    OperateMailsRsp,
    PackNotice,
    StatusCode,
)
import handlers.ItemSweep as item_sweep
from server import mail_rewards
from utils import item_mgr, res_loader


PLAYER = 1
REWARD = 102
WEAPON = 1101104
INSTANCE_ID = 9001
OK = StatusCode.StatusCode_OK
BAD_REQ = StatusCode.StatusCode_BAD_REQ
CFG_NOT_FOUND = StatusCode.StatusCode_CFG_NOT_FOUND
ITEM_NOT_ENOUGH = StatusCode.StatusCode_ITEM_NOT_ENOUGH


class FakeSession:
    def __init__(self):
        self.player_id = PLAYER
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
    Config.DB_PATH = str(tmp_path / "sweep-mail.db")
    Config.IN_MEMORY = False
    db.init()
    db.db.execute(
        "INSERT INTO players(player_id,player_name,level,exp,world_level,unlock_functions) "
        "VALUES(?,?,10,0,1,?)",
        (PLAYER, "sweep-mail", pickle.dumps([])),
    )
    monkeypatch.setattr(item_mgr.achieve_track, "report", lambda *args, **kwargs: None)
    yield
    db.exit()
    Config.DB_PATH, Config.IN_MEMORY = old_path, old_memory


@pytest.fixture(autouse=True)
def quick_config(monkeypatch):
    monkeypatch.setitem(
        res_loader.res,
        "QuickObtaining",
        {
            "quick_obtaining": {
                "datas": [
                    {
                        "i_d": REWARD,
                        "quick_obtaining_item": [
                            {"player_level": 1, "num": 1, "need_vitality": 2}
                        ],
                    }
                ]
            }
        },
    )


def grant(item_id, count):
    item_mgr.grant_items(PLAYER, [(item_id, count)])


def amount(item_id):
    return item_mgr.get_num(PLAYER, item_id)


def operation_keys():
    return sorted(db.get_kv_keys(PLAYER, "activity_operations"))


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


def assert_locked_transaction():
    assert db.conn.in_transaction
    assert item_mgr.player_lock(PLAYER)._is_owned()


def concurrently(calls):
    barrier = threading.Barrier(len(calls))

    def invoke(call):
        barrier.wait()
        return call()

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(calls)) as executor:
        return list(executor.map(invoke, calls))


def sweep(packet_id=1, count=1, item_id=REWARD, is_sweep=True, session=None):
    session = session or FakeSession()
    request = ItemSweepReq(item_id=item_id, count=count, is_sweep=is_sweep)
    item_sweep.Handler().handle(session, request.SerializeToString(), packet_id)
    packs = session.messages(MsgId.PackNotice)
    return session.response(MsgId.ItemSweepRsp), packs[-1] if packs else None


def instance_reward(instance_id=INSTANCE_ID, item_id=WEAPON):
    detail = ItemDetail(
        pack_type=ItemDetail.PackType.PackType_TempStorageArea,
        extra_quality=7,
    )
    detail.main_item.item_id = item_id
    detail.main_item.item_tag = EBagItemTag.EBagItemTag_Weapon
    detail.main_item.is_new = True
    detail.main_item.temp_pack_index = 9
    weapon = detail.main_item.weapon
    weapon.weapon_id = item_id
    weapon.instance_id = instance_id
    weapon.attack = 321
    weapon.damage_balance = 456
    weapon.critical_ratio = 789
    weapon.wearer_id = 101001
    weapon.level = 17
    weapon.star = 3
    return detail


def make_mail(mail_id=1, items=((REWARD, 3),), rewards=()):
    mail = MailBriefData(
        mail_id=mail_id,
        mail_state=MailState.MailState_UNCLAIM,
        title="atomic-mail",
        content="unchanged",
    )
    for item_id, count in items:
        mail.items.add(item_id=item_id, num=count)
    for reward in rewards:
        mail.reward.add().CopyFrom(reward)
    db.set_mail(PLAYER, mail_id, mail.SerializeToString())
    return mail


def claim(packet_id=1, mail_id=1, claim_all=False):
    response = OperateMailsRsp()
    pack = PackNotice(status=OK)
    result = mail_rewards.claim_attachments(
        PLAYER,
        mail_id=mail_id,
        claim_all=claim_all,
        claim_result=response.claim_result,
        pack_notice=pack,
        packet_id=packet_id,
    )
    return result, response, pack


def claim_signature(value):
    result, response, pack = value
    return (
        tuple(mail.SerializeToString() for mail in result.mails),
        tuple(result.gains),
        result.already_claimed,
        response.SerializeToString(),
        pack.SerializeToString(),
    )


def test_item_sweep_success_uses_one_lock_and_transaction(monkeypatch):
    grant(Config.VITALITY_ITEM_ID, 2)
    original = item_sweep._quick_cfg

    def checked_config(*args):
        assert_locked_transaction()
        return original(*args)

    monkeypatch.setattr(item_sweep, "_quick_cfg", checked_config)
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        response, pack = sweep(packet_id=41)
    finally:
        db.conn.set_trace_callback(None)

    assert response.status == OK
    assert [(item.main_item.item_id, item.main_item.base_item.num) for item in response.rewards] == [
        (REWARD, 1),
        (1, 2 * Config.EXP_PER_VITALITY),
    ]
    assert amount(Config.VITALITY_ITEM_ID) == 0
    assert amount(REWARD) == 1
    assert db.get_players_info(PLAYER, "exp") == [2 * Config.EXP_PER_VITALITY]
    assert pack.items
    assert operation_keys() == ["item-sweep:0:41"]
    assert statements.count("BEGIN IMMEDIATE") == 1
    assert not db.conn.in_transaction


def test_item_sweep_same_packet_replays_response_and_pack_without_effects():
    grant(Config.VITALITY_ITEM_ID, 2)
    first = sweep(packet_id=42)
    before = database_snapshot()

    replay = sweep(packet_id=42, session=FakeSession())

    assert replay[0].SerializeToString() == first[0].SerializeToString()
    assert replay[1].SerializeToString() == first[1].SerializeToString()
    assert database_snapshot() == before


def test_item_sweep_same_packet_with_different_fingerprint_conflicts():
    grant(Config.VITALITY_ITEM_ID, 2)
    first = sweep(packet_id=43)
    before = database_snapshot()

    response, pack = sweep(packet_id=43, count=2)

    assert response.status == BAD_REQ
    assert not response.rewards
    assert pack is None
    assert database_snapshot() == before
    assert first[0].status == OK


def test_item_sweep_packet_zero_remains_usable_without_packet_dedup():
    grant(Config.VITALITY_ITEM_ID, 4)

    first = sweep(packet_id=0)
    second = sweep(packet_id=0, session=FakeSession())

    assert first[0].status == second[0].status == OK
    assert amount(Config.VITALITY_ITEM_ID) == 0
    assert amount(REWARD) == 2
    assert db.get_players_info(PLAYER, "exp") == [4 * Config.EXP_PER_VITALITY]
    assert operation_keys() == []


def test_item_sweep_concurrent_same_packet_grants_once():
    grant(Config.VITALITY_ITEM_ID, 2)

    responses = concurrently(
        [lambda: sweep(packet_id=44, session=FakeSession()) for _ in range(8)]
    )

    assert all(response.status == OK for response, _ in responses)
    assert all(
        response.SerializeToString() == responses[0][0].SerializeToString()
        and pack.SerializeToString() == responses[0][1].SerializeToString()
        for response, pack in responses
    )
    assert amount(Config.VITALITY_ITEM_ID) == 0
    assert amount(REWARD) == 1
    assert operation_keys() == ["item-sweep:0:44"]


def test_item_sweep_concurrent_distinct_packets_cannot_overspend():
    grant(Config.VITALITY_ITEM_ID, 2)

    responses = concurrently(
        [
            lambda packet_id=packet_id: sweep(
                packet_id=packet_id, session=FakeSession()
            )[0].status
            for packet_id in range(50, 58)
        ]
    )

    assert responses.count(OK) == 1
    assert responses.count(ITEM_NOT_ENOUGH) == 7
    assert amount(Config.VITALITY_ITEM_ID) == 0
    assert amount(REWARD) == 1
    assert len(operation_keys()) == 1


def test_item_sweep_insufficient_material_has_no_side_effects():
    grant(Config.VITALITY_ITEM_ID, 1)
    before = database_snapshot()

    response, pack = sweep(packet_id=60, count=2)

    assert response.status == ITEM_NOT_ENOUGH
    assert not response.rewards
    assert pack is None
    assert database_snapshot() == before


@pytest.mark.parametrize(
    "datas,item_id,count,expected",
    [
        ([], REWARD, 1, CFG_NOT_FOUND),
        ([{"i_d": REWARD, "quick_obtaining_item": [{"player_level": 1, "num": 0, "need_vitality": 2}]}], REWARD, 1, CFG_NOT_FOUND),
        ([{"i_d": REWARD, "quick_obtaining_item": [{"player_level": 1, "num": 1, "need_vitality": -1}]}], REWARD, 1, CFG_NOT_FOUND),
        ([{"i_d": REWARD, "quick_obtaining_item": [{"player_level": 1, "num": 2, "need_vitality": 2}]}], REWARD, 1, BAD_REQ),
        ([{"i_d": 999999, "quick_obtaining_item": [{"player_level": 1, "num": 1, "need_vitality": 2}]}], 999999, 1, CFG_NOT_FOUND),
    ],
)
def test_item_sweep_invalid_config_or_request_has_no_side_effects(
    monkeypatch, datas, item_id, count, expected
):
    grant(Config.VITALITY_ITEM_ID, 2)
    monkeypatch.setitem(
        res_loader.res,
        "QuickObtaining",
        {"quick_obtaining": {"datas": datas}},
    )
    before = database_snapshot()

    response, pack = sweep(packet_id=61, item_id=item_id, count=count)

    assert response.status == expected
    assert not response.rewards
    assert pack is None
    assert database_snapshot() == before


def test_item_sweep_grant_failure_rolls_back_debit_and_allows_retry(monkeypatch):
    grant(Config.VITALITY_ITEM_ID, 2)
    before = database_snapshot()
    original_grant = item_mgr.grant_items

    def fail_grant(*args, **kwargs):
        raise RuntimeError("grant failed")

    monkeypatch.setattr(item_mgr, "grant_items", fail_grant)
    with pytest.raises(RuntimeError, match="grant failed"):
        sweep(packet_id=62)

    assert database_snapshot() == before
    monkeypatch.setattr(item_mgr, "grant_items", original_grant)
    assert sweep(packet_id=62)[0].status == OK


def test_item_sweep_operation_write_failure_rolls_back_everything(monkeypatch):
    grant(Config.VITALITY_ITEM_ID, 2)
    before = database_snapshot()
    original = item_sweep.activities._save_operation

    def save_then_fail(*args, **kwargs):
        original(*args, **kwargs)
        raise OSError("operation write failed")

    monkeypatch.setattr(item_sweep.activities, "_save_operation", save_then_fail)
    with pytest.raises(OSError, match="operation write failed"):
        sweep(packet_id=63)

    assert database_snapshot() == before
    assert not db.conn.in_transaction


def test_mail_claim_grants_stackable_and_exact_instance_atomically(monkeypatch):
    source = instance_reward()
    make_mail(rewards=(source,))
    events = []
    original_prepare = mail_rewards._prepare_rewards
    original_claim = db.try_claim
    original_grant = item_mgr.grant_items
    original_set_item = db.set_item_detail
    original_set_mail = db.set_mail

    def prepare(*args):
        assert_locked_transaction()
        events.append("validate")
        return original_prepare(*args)

    def try_claim(player_id, system, key):
        if system == mail_rewards.CLAIM_SYSTEM:
            assert_locked_transaction()
            events.append("claim")
        return original_claim(player_id, system, key)

    def grant_items(*args, **kwargs):
        assert_locked_transaction()
        events.append("grant")
        return original_grant(*args, **kwargs)

    def set_item(player_id, blob, item_id=None, instance_id=None):
        assert_locked_transaction()
        if instance_id == INSTANCE_ID:
            events.append("instance")
        return original_set_item(player_id, blob, item_id, instance_id)

    def set_mail(player_id, mail_id, blob):
        assert_locked_transaction()
        mail = MailBriefData.FromString(blob)
        assert mail.mail_state == MailState.MailState_CLAIMED
        assert not mail.items and not mail.reward
        events.append("clear")
        return original_set_mail(player_id, mail_id, blob)

    monkeypatch.setattr(mail_rewards, "_prepare_rewards", prepare)
    monkeypatch.setattr(db, "try_claim", try_claim)
    monkeypatch.setattr(item_mgr, "grant_items", grant_items)
    monkeypatch.setattr(db, "set_item_detail", set_item)
    monkeypatch.setattr(db, "set_mail", set_mail)
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        result, response, pack = claim(packet_id=70)
    finally:
        db.conn.set_trace_callback(None)

    assert result.gains == [(REWARD, 3), (WEAPON, 1)]
    assert not result.already_claimed
    assert len(response.claim_result) == 2
    assert len(pack.items) == 2
    assert events == ["validate", "claim", "grant", "instance", "clear"]
    assert statements.count("BEGIN IMMEDIATE") == 1
    assert amount(REWARD) == 3
    stored = ItemDetail.FromString(
        db.get_item_detail(PLAYER, None, INSTANCE_ID)
    )
    assert stored.main_item.weapon.weapon_id == WEAPON
    assert stored.main_item.weapon.instance_id == INSTANCE_ID
    assert stored.main_item.weapon.attack == 321
    assert stored.main_item.weapon.damage_balance == 456
    assert stored.main_item.weapon.critical_ratio == 789
    assert stored.main_item.weapon.level == 17
    assert stored.main_item.weapon.star == 3
    assert stored.main_item.weapon.wearer_id == 0
    assert stored.pack_type == ItemDetail.PackType.PackType_Inventory
    assert stored.main_item.temp_pack_index == 0
    assert not stored.main_item.is_new
    assert db.db.execute(
        "SELECT instance_id FROM instance WHERE player_id=?", (PLAYER,)
    ).fetchone() is None
    cleaned = MailBriefData.FromString(db.get_mail(PLAYER, 1))
    assert cleaned.mail_state == MailState.MailState_CLAIMED
    assert not cleaned.items and not cleaned.reward
    assert cleaned.title == "atomic-mail" and cleaned.content == "unchanged"
    assert db.get_claimed(PLAYER, mail_rewards.CLAIM_SYSTEM) == ["1"]
    assert operation_keys() == ["mail-attachment:0:70"]
    assert not db.conn.in_transaction


def test_mail_same_packet_replay_is_byte_stable_and_write_free():
    make_mail(rewards=(instance_reward(),))
    first = claim(packet_id=71)
    db.del_mail(PLAYER, 1)
    before = database_snapshot()

    replay = claim(packet_id=71)

    assert claim_signature(replay) == claim_signature(first)
    assert database_snapshot() == before


def test_mail_same_packet_with_different_selection_conflicts_without_buffer_changes():
    make_mail()
    claim(packet_id=72)
    before = database_snapshot()
    response = OperateMailsRsp()
    pack = PackNotice(status=OK)

    with pytest.raises(mail_rewards.MailRewardError) as caught:
        mail_rewards.claim_attachments(
            PLAYER,
            claim_all=True,
            claim_result=response.claim_result,
            pack_notice=pack,
            packet_id=72,
        )

    assert caught.value.status == BAD_REQ
    assert not response.claim_result and not pack.items
    assert database_snapshot() == before


def test_mail_packet_zero_uses_claim_ledger_and_remains_usable():
    make_mail()

    first = claim(packet_id=0)
    second = claim(packet_id=0)

    assert first[0].gains == [(REWARD, 3)]
    assert not first[0].already_claimed
    assert second[0].gains == []
    assert second[0].already_claimed
    assert not second[1].claim_result and not second[2].items
    assert amount(REWARD) == 3
    assert operation_keys() == []


@pytest.mark.parametrize("same_packet", [True, False])
def test_mail_concurrent_claims_grant_once(same_packet):
    make_mail(rewards=(instance_reward(),))

    results = concurrently(
        [
            lambda packet_id=70 if same_packet else 80 + index: claim(
                packet_id=packet_id
            )
            for index in range(8)
        ]
    )

    assert sum(bool(result.gains) for result, _, _ in results) == (
        8 if same_packet else 1
    )
    assert amount(REWARD) == 3
    assert len(db.get_item_detail(PLAYER, table="items_s")) == 1
    assert db.get_claimed(PLAYER, mail_rewards.CLAIM_SYSTEM) == ["1"]
    assert len(operation_keys()) == (1 if same_packet else 8)
    if same_packet:
        assert all(claim_signature(value) == claim_signature(results[0]) for value in results)


@pytest.mark.parametrize(
    "items,reward_kind,expected",
    [
        (((999999, 1),), None, CFG_NOT_FOUND),
        (((REWARD, 0),), None, CFG_NOT_FOUND),
        ((), "empty", CFG_NOT_FOUND),
        ((), "mismatch", BAD_REQ),
    ],
)
def test_mail_invalid_config_or_payload_has_no_side_effects(
    items, reward_kind, expected
):
    rewards = ()
    if reward_kind:
        reward = ItemDetail()
        reward.main_item.item_id = REWARD
        if reward_kind == "mismatch":
            reward.main_item.base_item.item_id = REWARD + 1
            reward.main_item.base_item.num = 1
        rewards = (reward,)
    make_mail(items=items, rewards=rewards)
    before = database_snapshot()
    response = OperateMailsRsp()
    pack = PackNotice(status=OK)

    with pytest.raises(mail_rewards.MailRewardError) as caught:
        mail_rewards.claim_attachments(
            PLAYER,
            mail_id=1,
            claim_result=response.claim_result,
            pack_notice=pack,
            packet_id=90,
        )

    assert caught.value.status == expected
    assert not response.claim_result and not pack.items
    assert database_snapshot() == before


@pytest.mark.parametrize("case", ["malformed", "zero-id", "missing"])
def test_mail_invalid_blob_or_selection_has_no_side_effects(case):
    if case == "malformed":
        db.set_mail(PLAYER, 1, b"\xff")
        mail_id = 1
    elif case == "zero-id":
        db.set_mail(PLAYER, 1, MailBriefData().SerializeToString())
        mail_id = 1
    else:
        mail_id = 2
    before = database_snapshot()

    with pytest.raises(mail_rewards.MailRewardError) as caught:
        claim(packet_id=91, mail_id=mail_id)

    assert caught.value.status == BAD_REQ
    assert database_snapshot() == before


def test_mail_duplicate_instance_across_claim_all_is_rejected_before_writes():
    reward = instance_reward()
    make_mail(mail_id=1, rewards=(reward,))
    make_mail(mail_id=2, rewards=(reward,))
    before = database_snapshot()

    with pytest.raises(mail_rewards.MailRewardError) as caught:
        claim(packet_id=92, claim_all=True)

    assert caught.value.status == BAD_REQ
    assert database_snapshot() == before


@pytest.mark.parametrize("case", ["mismatch", "existing"])
def test_mail_invalid_instance_is_rejected_without_side_effects(case):
    reward = instance_reward()
    if case == "mismatch":
        reward.main_item.weapon.weapon_id = WEAPON + 1
    else:
        db.set_item_detail(
            PLAYER, reward.SerializeToString(), None, INSTANCE_ID
        )
    make_mail(rewards=(reward,))
    before = database_snapshot()

    with pytest.raises(mail_rewards.MailRewardError) as caught:
        claim(packet_id=93)

    assert caught.value.status == BAD_REQ
    assert database_snapshot() == before


def test_mail_claim_all_validates_every_mail_before_any_grant():
    make_mail(mail_id=1)
    make_mail(mail_id=2, items=((999999, 1),))
    before = database_snapshot()

    with pytest.raises(mail_rewards.MailRewardError) as caught:
        claim(packet_id=94, claim_all=True)

    assert caught.value.status == CFG_NOT_FOUND
    assert database_snapshot() == before


def test_mail_instance_grant_failure_rolls_back_stackable_claim_and_mail(monkeypatch):
    make_mail(rewards=(instance_reward(),))
    before = database_snapshot()
    original = db.set_item_detail

    def fail_instance(player_id, blob, item_id=None, instance_id=None):
        if instance_id == INSTANCE_ID:
            assert_locked_transaction()
            raise OSError("instance grant failed")
        return original(player_id, blob, item_id, instance_id)

    monkeypatch.setattr(db, "set_item_detail", fail_instance)
    response = OperateMailsRsp()
    pack = PackNotice(status=OK)
    with pytest.raises(OSError, match="instance grant failed"):
        mail_rewards.claim_attachments(
            PLAYER,
            mail_id=1,
            claim_result=response.claim_result,
            pack_notice=pack,
            packet_id=95,
        )

    assert database_snapshot() == before
    assert not response.claim_result and not pack.items
    assert not db.conn.in_transaction


def test_mail_attachment_clear_failure_rolls_back_grant_and_claim(monkeypatch):
    make_mail(rewards=(instance_reward(),))
    before = database_snapshot()
    original = db.set_mail

    def fail_clear(player_id, mail_id, blob):
        assert_locked_transaction()
        raise OSError("mail clear failed")

    monkeypatch.setattr(db, "set_mail", fail_clear)
    with pytest.raises(OSError, match="mail clear failed"):
        claim(packet_id=96)

    assert database_snapshot() == before
    assert not db.conn.in_transaction


def test_mail_operation_write_failure_rolls_back_grant_and_claim(monkeypatch):
    make_mail(rewards=(instance_reward(),))
    before = database_snapshot()
    original = mail_rewards._save_operation

    def save_then_fail(*args, **kwargs):
        original(*args, **kwargs)
        raise OSError("operation write failed")

    monkeypatch.setattr(mail_rewards, "_save_operation", save_then_fail)
    with pytest.raises(OSError, match="operation write failed"):
        claim(packet_id=97)

    assert database_snapshot() == before
    assert not db.conn.in_transaction
