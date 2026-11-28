import concurrent.futures
import json
import pickle
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from config import Config
from network.msg_id import MsgId
from proto.net_pb2 import (
    ActivityKoiUseItemReq,
    ActivityKoiUseItemRsp,
    BattleState,
    ChallengeData,
    ChallengeStateUpdateReq,
    ChallengeStateUpdateRsp,
    GamePlayRewardReq,
    GamePlayRewardRsp,
    StatusCode,
)

# Keep the repository's established db -> protobuf import order.
import utils.db as db
import utils.res_loader as res_loader
from utils import item_mgr
from server import minigames
import handlers.ActivityKoiUseItem as koi
import handlers.ChallengeStateUpdate as challenge
import handlers.GamePlayReward as gameplay


OK = StatusCode.StatusCode_OK
BAD_REQ = StatusCode.StatusCode_BAD_REQ
CFG_NOT_FOUND = StatusCode.StatusCode_CFG_NOT_FOUND
FAIL = StatusCode.StatusCode_FAIL
REPEAT = StatusCode.StatusCode_REPEAT_GET_REWARD
NOT_FINISHED = StatusCode.StatusCode_NotFinishAllTask
LIMIT = StatusCode.StatusCode_GamePlayRewardLimit
PLAYER = 1


class FakeSession:
    def __init__(self, player_id=PLAYER, scene_id=1):
        self.player_id = player_id
        self.player_name = "economy-p0"
        self.scene_id = scene_id
        self.sent = []

    def send(self, message_id, message, packet_id=0):
        self.sent.append((message_id, message, packet_id))

    def response(self, message_id):
        return [message for mid, message, _ in self.sent if mid == message_id][-1]


class EconomyP0FixTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        res_loader.init()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_path = Config.DB_PATH
        self.old_memory = Config.IN_MEMORY
        db.exit()
        Config.DB_PATH = str(Path(self.tmp.name) / "economy-p0.db")
        Config.IN_MEMORY = False
        db.init()
        db.db.execute(
            "INSERT INTO players(player_id, player_name, level, world_level, unlock_functions) "
            "VALUES (?, ?, 10, 1, ?)",
            (PLAYER, "economy-p0", pickle.dumps([])),
        )
        self.team_seq = 100
        self.challenge_resources = {
            "Challenge": {"challenge": {"datas": (
                {
                    "i_d": 100001,
                    "scene_i_d": 1,
                    "asset_full_name": "TimeSpeed/ChallengeTimeSpeed_1001",
                    "star_timer1": 90,
                    "star_timer2": 40,
                    "star_timer3": 35,
                },
                {
                    "i_d": 100002,
                    "scene_i_d": 1,
                    "asset_full_name": "World/Challenge_1002",
                    "star_timer1": 80,
                    "star_timer2": 50,
                    "star_timer3": 30,
                },
            )}},
        }
        self.koi_resources = {
            "ActivityJinli": {"jinli": {"datas": ({
                "i_d": 260326180,
                "params1": 20,
                "params2": 6044,
                "params3": 1,
            },)}},
        }

    def tearDown(self):
        db.exit()
        Config.DB_PATH = self.old_path
        Config.IN_MEMORY = self.old_memory
        self.tmp.cleanup()

    def grant(self, item_id, count):
        item_mgr.grant_items(PLAYER, [(item_id, count)])

    def num(self, item_id):
        return item_mgr.get_num(PLAYER, item_id)

    def concurrently(self, calls):
        barrier = threading.Barrier(len(calls))

        def invoke(call):
            barrier.wait()
            return call()

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(calls)) as executor:
            return [
                future.result()
                for future in [executor.submit(invoke, call) for call in calls]
            ]

    def start_challenge(self, session, packet_id=10, challenge_id=100001):
        request = ChallengeStateUpdateReq(
            challenge_id=challenge_id, battle_state=BattleState.Start
        )
        challenge.Handler().handle(session, request.SerializeToString(), packet_id)
        return session.response(MsgId.ChallengeStateUpdateRsp)

    def finish_challenge(self, session, use_time=30, packet_id=1, challenge_id=100001):
        request = ChallengeStateUpdateReq(
            challenge_id=challenge_id,
            battle_state=BattleState.Finish,
            use_time=use_time,
        )
        challenge.Handler().handle(session, request.SerializeToString(), packet_id)
        return session.response(MsgId.ChallengeStateUpdateRsp)

    def make_run(self, *, mode_id=1001, sub_mode_id=100101, gameplay_id=10010101,
                 members=None, settle=True, rewards=None):
        self.team_seq += 1
        team = {
            "team_id": self.team_seq,
            "mode_id": mode_id,
            "sub_mode_id": sub_mode_id,
            "gameplay_id": gameplay_id,
            "game_mode": 1,
            "room": {"game_start_time": 1234},
            "members": members or {str(PLAYER): {"character_id": 1}},
        }
        table = {} if rewards is None else {
            (mode_id, sub_mode_id, gameplay_id, 3): {"items": rewards}
        }
        with patch.object(Config, "MP_ROOM_REWARDS", table, create=True):
            minigames.mark_started(team, 1000)
            if settle:
                run, changed = minigames.settle(team, 3, 2000)
                self.assertTrue(changed)
                return team, run
            return team, minigames.load_run(team["room"]["run_id"])

    def claim_gameplay(self, session, reward_id=1001, num=1, packet_id=1):
        request = GamePlayRewardReq(reward_id=reward_id, num=num)
        gameplay.Handler().handle(session, request.SerializeToString(), packet_id)
        return session.response(MsgId.GamePlayRewardRsp)

    def use_koi(self, session, num=1, activity_id=260326180, packet_id=1):
        request = ActivityKoiUseItemReq(num=num, activity_id=activity_id)
        koi.Handler().handle(session, request.SerializeToString(), packet_id)
        return session.response(MsgId.ActivityKoiUseItemRsp)

    def box_indexes(self):
        return sorted(
            int(key)
            for key in db.get_kv_keys(PLAYER, "dyn_box")
            if key != "seq"
        )

    def test_challenge_finish_pays_once_per_run_and_replays_original_result(self):
        self.grant(101, 7)
        session = FakeSession()
        with patch.dict(res_loader.res, self.challenge_resources), \
             patch.object(challenge._at, "report") as report:
            self.start_challenge(FakeSession(), packet_id=10)
            first = self.finish_challenge(session, packet_id=11)
            replay = self.finish_challenge(session, use_time=99, packet_id=12)
            duplicate = self.finish_challenge(session, use_time=1, packet_id=13)
            self.assertEqual((first.status, first.challenge_data.star), (OK, 3))
            self.assertEqual(first.SerializeToString(), replay.SerializeToString())
            self.assertEqual(first.SerializeToString(), duplicate.SerializeToString())
            self.assertEqual(self.num(101), 1007)

            start = ChallengeStateUpdateReq(
                challenge_id=100001, battle_state=BattleState.Start
            )
            challenge.Handler().handle(session, start.SerializeToString(), 14)
            # 旧 packet_id 在新 run 中仍重放旧响应，不能领取新 run 的奖励。
            old_packet = self.finish_challenge(session, use_time=30, packet_id=11)
            self.assertEqual(first.SerializeToString(), old_packet.SerializeToString())
            self.assertEqual(self.num(101), 1007)
            second = self.finish_challenge(session, use_time=20, packet_id=15)
            self.assertEqual(second.status, OK)
            self.assertEqual(self.num(101), 2007)

        self.assertEqual(session.sent[0][2], 11)
        responses = [entry for entry in session.sent if entry[0] == MsgId.ChallengeStateUpdateRsp]
        self.assertEqual(responses[-1][2], 15)
        self.assertEqual(sum(call.args[1] == "race_done" for call in report.call_args_list), 2)
        stored = ChallengeData.FromString(db.get_challenge(PLAYER, 1, 100001))
        self.assertEqual((stored.state, stored.use_time, stored.star), (BattleState.Finish, 20, 3))
        self.assertEqual(len(db.get_claimed(PLAYER, "challenge_finish")), 2)
        run = json.loads(db.get_kv(PLAYER, "challenge_runs", "1:100001"))
        self.assertEqual(run["state"], "settled")

    def test_challenge_start_is_persisted_and_finish_settles_it(self):
        session = FakeSession()
        request = ChallengeStateUpdateReq(
            challenge_id=100001, battle_state=BattleState.Start
        )
        with patch.dict(res_loader.res, self.challenge_resources), \
             patch.object(challenge._at, "report"):
            challenge.Handler().handle(session, request.SerializeToString(), 21)
            started = session.response(MsgId.ChallengeStateUpdateRsp)
            self.assertEqual(started.challenge_data.state, BattleState.Start)
            self.assertEqual(
                ChallengeData.FromString(db.get_challenge(PLAYER, 1, 100001)).state,
                BattleState.Start,
            )
            self.assertEqual(self.finish_challenge(session, packet_id=22).status, OK)
        self.assertEqual(self.num(101), 1000)

    def test_challenge_fail_is_persisted_without_reward(self):
        session = FakeSession()
        request = ChallengeStateUpdateReq(
            challenge_id=100001, battle_state=BattleState.Fail, use_time=10
        )
        with patch.dict(res_loader.res, self.challenge_resources):
            challenge.Handler().handle(session, request.SerializeToString(), 31)
        self.assertEqual(session.response(MsgId.ChallengeStateUpdateRsp).challenge_data.state, BattleState.Fail)
        self.assertEqual(self.num(101), 0)
        self.assertFalse(db.get_claimed(PLAYER, "challenge_finish"))
        self.assertEqual(self.finish_challenge(session, packet_id=32).status, StatusCode.StatusCode_ChallengeNotStart)
        self.assertEqual(self.num(101), 0)

    def test_challenge_concurrent_finish_grants_once(self):
        sessions = [FakeSession() for _ in range(8)]
        with patch.dict(res_loader.res, self.challenge_resources), \
             patch.object(challenge._at, "report") as report:
            self.start_challenge(FakeSession(), packet_id=10)
            responses = self.concurrently([
                (lambda session=session, packet_id=packet_id: self.finish_challenge(
                    session, packet_id=packet_id
                ))
                for packet_id, session in enumerate(sessions, 1)
            ])
        self.assertEqual([rsp.status for rsp in responses], [OK] * 8)
        self.assertEqual(len({rsp.SerializeToString() for rsp in responses}), 1)
        self.assertEqual(self.num(101), 1000)
        self.assertEqual(len(db.get_claimed(PLAYER, "challenge_finish")), 1)
        self.assertEqual(sum(call.args[1] == "race_done" for call in report.call_args_list), 1)

    def test_challenge_invalid_finish_has_no_side_effects(self):
        session = FakeSession()
        with patch.dict(res_loader.res, self.challenge_resources), \
             patch.object(challenge.item_mgr, "grant_items") as grant:
            self.assertEqual(
                self.finish_challenge(session, use_time=0).status,
                StatusCode.StatusCode_ChallengeNotStart,
            )
            self.start_challenge(session, packet_id=2)
            self.assertEqual(self.finish_challenge(session, use_time=0, packet_id=3).status, BAD_REQ)
            self.assertEqual(self.finish_challenge(session, challenge_id=999999, packet_id=4).status, CFG_NOT_FOUND)
        grant.assert_not_called()
        self.assertEqual(self.num(101), 0)
        self.assertFalse(db.get_claimed(PLAYER, "challenge_finish"))
        self.assertEqual(
            ChallengeData.FromString(db.get_challenge(PLAYER, 1, 100001)).state,
            BattleState.Start,
        )

    def test_challenge_finish_without_start_cannot_create_eligibility(self):
        with patch.dict(res_loader.res, self.challenge_resources):
            response = self.finish_challenge(FakeSession(), packet_id=1)
        self.assertEqual(response.status, StatusCode.StatusCode_ChallengeNotStart)
        self.assertEqual(self.num(101), 0)
        self.assertFalse(db.get_claimed(PLAYER, "challenge_finish"))
        self.assertIsNone(db.get_kv(PLAYER, "challenge_runs", "1:100001"))

    def test_challenge_grant_failure_rolls_back_claim_and_state(self):
        session = FakeSession()
        with patch.dict(res_loader.res, self.challenge_resources), \
             patch.object(challenge._at, "report"), \
             patch.object(challenge.item_mgr, "grant_items", side_effect=RuntimeError("synthetic")):
            self.start_challenge(session, packet_id=1)
            with self.assertRaisesRegex(RuntimeError, "synthetic"):
                self.finish_challenge(session, packet_id=2)
        self.assertEqual(self.num(101), 0)
        self.assertFalse(db.get_claimed(PLAYER, "challenge_finish"))
        self.assertEqual(
            ChallengeData.FromString(db.get_challenge(PLAYER, 1, 100001)).state,
            BattleState.Start,
        )
        run = json.loads(db.get_kv(PLAYER, "challenge_runs", "1:100001"))
        self.assertEqual(run["state"], "active")
        with patch.dict(res_loader.res, self.challenge_resources), \
             patch.object(challenge._at, "report"):
            self.assertEqual(self.finish_challenge(session, packet_id=3).status, OK)
        self.assertEqual(self.num(101), 1000)

    def test_gameplay_without_settled_run_fails_closed(self):
        session = FakeSession()
        rsp = self.claim_gameplay(session)
        self.assertEqual(rsp.status, NOT_FINISHED)
        self.assertEqual(rsp.dynamic_treasure_box_info.index, 0)
        self.assertEqual(self.box_indexes(), [])
        self.assertFalse(db.get_claimed(PLAYER, "gameplay_reward"))

    def test_gameplay_settled_run_claims_once_and_packet_replay_survives_restart(self):
        _team, run = self.make_run()
        session = FakeSession()
        first = self.claim_gameplay(session, packet_id=41)
        self.assertEqual(first.status, OK)
        self.assertEqual(first.dynamic_treasure_box_info.box_id, 3001)
        self.assertEqual(first.dynamic_treasure_box_info.index, 1)
        self.assertEqual(session.sent[0][2], 41)
        self.assertEqual(db.get_kv(PLAYER, "dyn_box", 1), 3001)
        self.assertTrue(db.has_claimed(PLAYER, "gameplay_reward", run["run_id"]))

        db.exit()
        db.init()
        replay = self.claim_gameplay(FakeSession(), packet_id=41)
        duplicate = self.claim_gameplay(FakeSession(), packet_id=42)
        self.assertEqual(first.SerializeToString(), replay.SerializeToString())
        self.assertEqual(duplicate.status, LIMIT)
        self.assertEqual(self.box_indexes(), [1])
        self.assertEqual(db.get_kv(PLAYER, "dyn_box", "seq"), 1)

    def test_gameplay_concurrent_claim_creates_one_box(self):
        _team, run = self.make_run()
        sessions = [FakeSession() for _ in range(8)]
        responses = self.concurrently([
            (lambda session=session, packet_id=packet_id: self.claim_gameplay(
                session, packet_id=packet_id
            ))
            for packet_id, session in enumerate(sessions, 1)
        ])
        self.assertEqual([rsp.status for rsp in responses].count(OK), 1)
        self.assertEqual([rsp.status for rsp in responses].count(LIMIT), 7)
        self.assertEqual(self.box_indexes(), [1])
        self.assertEqual(db.get_claimed(PLAYER, "gameplay_reward"), [run["run_id"]])

    def test_gameplay_can_claim_distinct_settled_runs(self):
        self.make_run()
        self.make_run()
        first = self.claim_gameplay(FakeSession(), packet_id=1)
        second = self.claim_gameplay(FakeSession(), packet_id=2)
        self.assertEqual((first.status, second.status), (OK, OK))
        self.assertEqual((first.dynamic_treasure_box_info.index, second.dynamic_treasure_box_info.index), (1, 2))
        self.assertEqual(self.box_indexes(), [1, 2])

    def test_gameplay_rejects_unqualified_or_already_paid_runs(self):
        active_team, _active = self.make_run(settle=False)
        self.assertEqual(self.claim_gameplay(FakeSession()).status, NOT_FINISHED)
        self.make_run(gameplay_id=99999999)
        self.assertEqual(self.claim_gameplay(FakeSession(), packet_id=2).status, NOT_FINISHED)
        self.make_run(members={"2": {"character_id": 2}})
        self.assertEqual(self.claim_gameplay(FakeSession(), packet_id=3).status, NOT_FINISHED)
        self.make_run(rewards=((101, 2),))
        self.assertEqual(self.claim_gameplay(FakeSession(), packet_id=4).status, NOT_FINISHED)
        self.assertEqual(self.num(101), 2)
        self.assertEqual(self.box_indexes(), [])
        self.assertTrue(active_team["room"]["run_id"])

    def test_gameplay_bad_quantity_and_packet_conflict_create_no_box(self):
        self.make_run()
        self.assertEqual(self.claim_gameplay(FakeSession(), num=2, packet_id=1).status, BAD_REQ)
        first = self.claim_gameplay(FakeSession(), packet_id=2)
        self.assertEqual(first.status, OK)
        conflict = self.claim_gameplay(FakeSession(), reward_id=1002, packet_id=2)
        self.assertEqual(conflict.status, BAD_REQ)
        self.assertEqual(self.box_indexes(), [1])

    def test_gameplay_box_write_failure_rolls_back_claim_and_sequence(self):
        _team, run = self.make_run()
        real_set_kv = db.set_kv

        def fail_box(player_id, namespace, key, value):
            if namespace == "dyn_box":
                raise RuntimeError("synthetic box write")
            return real_set_kv(player_id, namespace, key, value)

        with patch.object(gameplay.db, "set_kv", side_effect=fail_box):
            with self.assertRaisesRegex(RuntimeError, "synthetic box write"):
                self.claim_gameplay(FakeSession(), packet_id=1)
        self.assertFalse(db.has_claimed(PLAYER, "gameplay_reward", run["run_id"]))
        self.assertIsNone(db.get_kv(PLAYER, "dyn_box", "seq"))
        self.assertEqual(self.box_indexes(), [])
        self.assertEqual(self.claim_gameplay(FakeSession(), packet_id=1).status, OK)
        self.assertEqual(self.box_indexes(), [1])

    def test_koi_cap_failure_does_not_consume_item(self):
        self.grant(6044, 5)
        db.set_kv(PLAYER, "koi_codes", "260326180", pickle.dumps(["%08d" % i for i in range(19)]))
        session = FakeSession()
        with patch.dict(res_loader.res, self.koi_resources):
            rsp = self.use_koi(session, num=2, packet_id=51)
        self.assertEqual(rsp.status, FAIL)
        self.assertEqual(self.num(6044), 5)
        self.assertEqual(len(pickle.loads(db.get_kv(PLAYER, "koi_codes", "260326180"))), 19)
        self.assertEqual([mid for mid, _msg, _pid in session.sent], [MsgId.ActivityKoiUseItemRsp])
        self.assertEqual(session.sent[0][2], 51)

    def test_koi_success_debits_and_persists_codes(self):
        self.grant(6044, 5)
        session = FakeSession()
        with patch.dict(res_loader.res, self.koi_resources), \
             patch.object(koi.random, "randint", side_effect=[12345678, 12345679]):
            rsp = self.use_koi(session, num=2)
        self.assertEqual((rsp.status, rsp.num, list(rsp.codes)), (OK, 2, ["12345678", "12345679"]))
        self.assertEqual(self.num(6044), 3)
        self.assertEqual(pickle.loads(db.get_kv(PLAYER, "koi_codes", "260326180")), ["12345678", "12345679"])
        self.assertEqual([mid for mid, _msg, _pid in session.sent], [MsgId.ActivityKoiUseItemRsp, MsgId.PackNotice])

    def test_koi_concurrent_use_respects_cap_and_debits_exact_items(self):
        self.grant(6044, 5)
        resources = {"ActivityJinli": {"jinli": {"datas": ({
            "i_d": 260326180, "params1": 3, "params2": 6044, "params3": 1,
        },)}}}
        sessions = [FakeSession() for _ in range(5)]
        with patch.dict(res_loader.res, resources):
            responses = self.concurrently([
                (lambda session=session: self.use_koi(session)) for session in sessions
            ])
        self.assertEqual([rsp.status for rsp in responses].count(OK), 3)
        self.assertEqual([rsp.status for rsp in responses].count(FAIL), 2)
        self.assertEqual(self.num(6044), 2)
        self.assertEqual(len(pickle.loads(db.get_kv(PLAYER, "koi_codes", "260326180"))), 3)

    def test_koi_code_save_failure_rolls_back_consumption(self):
        self.grant(6044, 2)
        real_set_kv = db.set_kv

        def fail_codes(player_id, namespace, key, value):
            if namespace == "koi_codes":
                raise RuntimeError("synthetic code save")
            return real_set_kv(player_id, namespace, key, value)

        with patch.dict(res_loader.res, self.koi_resources), \
             patch.object(koi.db, "set_kv", side_effect=fail_codes):
            with self.assertRaisesRegex(RuntimeError, "synthetic code save"):
                self.use_koi(FakeSession())
        self.assertEqual(self.num(6044), 2)
        self.assertIsNone(db.get_kv(PLAYER, "koi_codes", "260326180"))

    def test_koi_unknown_activity_zero_count_and_corrupt_state_fail_closed(self):
        self.grant(6044, 2)
        with patch.dict(res_loader.res, self.koi_resources):
            self.assertEqual(self.use_koi(FakeSession(), activity_id=999).status, CFG_NOT_FOUND)
            self.assertEqual(self.use_koi(FakeSession(), num=0, packet_id=2).status, BAD_REQ)
            db.set_kv(PLAYER, "koi_codes", "260326180", b"not-a-code-list")
            self.assertEqual(self.use_koi(FakeSession(), packet_id=3).status, FAIL)
        self.assertEqual(self.num(6044), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
