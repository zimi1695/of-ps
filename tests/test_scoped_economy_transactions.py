import concurrent.futures
import pickle
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from config import Config
import utils.db as db
import utils.res_loader as res_loader
from proto.net_pb2 import (
    CollectCoinReq,
    DungeonData,
    HandicraftReq,
    LifeBaseInfo,
    LifeType,
    MakePropReq,
    RestaurantGameOperateReq,
    RestaurantGameOperateType,
    RestaurantGameQuestRewardReq,
    SewingReq,
    StatusCode,
)
from network.msg_id import MsgId
from utils import achieve_track, dungeon_state, item_mgr, restaurant_game_store as store
import handlers.Handicraft as handicraft
import handlers.MakeProp as makeprop
import handlers.Sewing as sewing
import handlers.RestaurantGameOperate as restaurant_operate
import handlers.RestaurantGameQuestReward as restaurant_quest
import handlers.CollectCoin as collect_coin

OK = StatusCode.StatusCode_OK
BAD_REQ = StatusCode.StatusCode_BAD_REQ
CFG_NOT_FOUND = StatusCode.StatusCode_CFG_NOT_FOUND
ITEM_NOT_ENOUGH = StatusCode.StatusCode_ITEM_NOT_ENOUGH
REPEAT = StatusCode.StatusCode_REPEAT_GET_REWARD


class FakeSession:
    def __init__(self, player_id=1, dungeon_id=0):
        self.player_id = player_id
        self.player_name = "ScopedEconomyTest"
        self.scene_id = 1
        self.channel_id = 1
        self.running = True
        self.dungeon = [dungeon_id, 0, 0, -1]
        self.sent = []

    def send(self, message_id, message, packet_id=0):
        self.sent.append((message_id, message, packet_id))

    def response(self, message_id):
        return [message for mid, message, _ in self.sent if mid == message_id][-1]


class ScopedEconomyTransactionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        res_loader.init()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_path = Config.DB_PATH
        self.old_memory = Config.IN_MEMORY
        db.exit()
        Config.DB_PATH = str(Path(self.tmp.name) / "scoped.db")
        Config.IN_MEMORY = False
        db.init()
        db.db.execute(
            "INSERT INTO players(player_id, player_name, level, world_level, unlock_functions) "
            "VALUES (1, 'ScopedEconomyTest', 10, 1, ?)",
            (pickle.dumps([]),),
        )
        db.db.execute(
            "INSERT INTO life_info(player_id, life_blob) VALUES (1, ?)",
            (pickle.dumps({}),),
        )
        for life_type in range(1, 7):
            db.set_life(1, life_type, LifeBaseInfo(life_type=life_type, level=1).SerializeToString())
        self.report_patch = patch.object(achieve_track, "report")
        self.report_patch.start()

    def tearDown(self):
        self.report_patch.stop()
        db.exit()
        Config.DB_PATH = self.old_path
        Config.IN_MEMORY = self.old_memory
        self.tmp.cleanup()

    def grant(self, item_id, count):
        item_mgr.grant_items(1, [(item_id, count)])

    def num(self, item_id):
        return item_mgr.get_num(1, item_id)

    def proficiency(self, life_type):
        return LifeBaseInfo.FromString(db.get_life(1, life_type)).proficiency_value

    def concurrently(self, calls):
        barrier = threading.Barrier(len(calls))

        def invoke(call):
            barrier.wait()
            return call()

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(calls)) as executor:
            return [future.result() for future in [executor.submit(invoke, call) for call in calls]]

    def craft_cases(self):
        return (
            (
                handicraft,
                HandicraftReq,
                MsgId.HandicraftRsp,
                "Handicraft",
                "handicraft_item",
                "handicraft_items",
                LifeType.LIFE_TYPE_Handicraft,
                4,
            ),
            (
                makeprop,
                MakePropReq,
                MsgId.MakePropRsp,
                "Make",
                "make_item",
                "make_items",
                LifeType.LIFE_TYPE_Making,
                3,
            ),
            (
                sewing,
                SewingReq,
                MsgId.SewingRsp,
                "Spin",
                "spin_item",
                "spin_items",
                LifeType.LIFE_TYPE_Sewing,
                5,
            ),
        )

    def craft_recipe(self, material_field, **changes):
        recipe = {
            "i_d": 1,
            "need_item_i_d": 11,
            "need_item_count": 2,
            material_field: [{"item_i_d": 104, "item_count": 3}],
            "successful_probability": 1,
            "get_item_i_d": 106,
        }
        recipe.update(changes)
        return recipe

    def craft_request(self, case, request_id=1):
        module, request_type, _, _, _, _, _, _ = case
        field = {
            HandicraftReq: "handicraft_id",
            MakePropReq: "make_id",
            SewingReq: "sewing_id",
        }[request_type]
        return request_type(**{field: request_id})

    def invoke_craft(self, case, packet_id, request_id=1, session=None, recipe_changes=None):
        module, request_type, response_id, sheet, table, _, _, _ = case
        level_key = {
            "Handicraft": "handicraft_level",
            "Make": "make_level",
            "Spin": "spin_level",
        }[sheet]
        recipe = self.craft_recipe(
            {"Handicraft": "handicraft_items", "Make": "make_items", "Spin": "spin_items"}[sheet],
            i_d=request_id,
            **{level_key: 1},
        )
        recipe.update(recipe_changes or {})
        session = session or FakeSession()
        with patch.dict(res_loader.res, {sheet: {table: {"datas": [recipe]}}}), patch.object(
            module.random, "random", return_value=0
        ):
            module.Handler().handle(
                session, self.craft_request(case, request_id).SerializeToString(), packet_id
            )
        return session.response(response_id)

    def test_craft_success_replay_conflict_and_no_duplicate(self):
        for case in self.craft_cases():
            with self.subTest(handler=case[0].__name__):
                self.grant(11, 2)
                self.grant(104, 3)
                first = self.invoke_craft(case, 1)
                self.assertEqual(first.status, OK)
                self.assertTrue(first.life_common_result.is_success)
                self.assertEqual((self.num(11), self.num(104), self.num(106)), (0, 0, 1))
                self.assertEqual(self.proficiency(case[6]), 10)
                replay = self.invoke_craft(case, 1, session=FakeSession())
                self.assertEqual(replay.SerializeToString(), first.SerializeToString())
                self.assertEqual((self.num(11), self.num(104), self.num(106)), (0, 0, 1))
                self.assertEqual(self.proficiency(case[6]), 10)
                conflict = self.invoke_craft(case, 1, request_id=2)
                self.assertEqual(conflict.status, BAD_REQ)
                self.assertEqual((self.num(11), self.num(104), self.num(106)), (0, 0, 1))
                item_mgr.consume_items(1, [(11, self.num(11)), (104, self.num(104)), (106, self.num(106))])
                life = LifeBaseInfo.FromString(db.get_life(1, case[6]))
                life.proficiency_value = 0
                db.set_life(1, case[6], life.SerializeToString())
                db.del_kv(1, "life_book")

    def test_craft_insufficient_materials_do_not_write(self):
        for case in self.craft_cases():
            with self.subTest(handler=case[0].__name__):
                self.grant(11, 2)
                self.grant(104, 2)
                before = (db.get_item_detail(1, 11), db.get_item_detail(1, 104))
                result = self.invoke_craft(case, 1, recipe_changes={"need_item_count": 0})
                self.assertEqual(result.status, ITEM_NOT_ENOUGH)
                self.assertEqual((db.get_item_detail(1, 11), db.get_item_detail(1, 104)), before)
                self.assertEqual((self.num(104), self.num(106), self.proficiency(case[6])), (2, 0, 0))
                item_mgr.consume_items(1, [(11, 2), (104, 2)])

    def test_craft_configuration_error_precedes_payment(self):
        for case in self.craft_cases():
            with self.subTest(handler=case[0].__name__):
                self.grant(11, 2)
                self.grant(104, 3)
                module, _, response_id, sheet, table, material_field, life_type, _ = case
                recipe = self.craft_recipe(material_field, get_item_i_d=999999999)
                session = FakeSession()
                with patch.dict(res_loader.res, {sheet: {table: {"datas": [recipe]}}}):
                    module.Handler().handle(session, self.craft_request(case).SerializeToString(), 1)
                self.assertEqual(session.response(response_id).status, CFG_NOT_FOUND)
                self.assertEqual((self.num(11), self.num(104), self.num(106)), (2, 3, 0))
                self.assertEqual(self.proficiency(life_type), 0)
                item_mgr.consume_items(1, [(11, 2), (104, 3)])

    def test_craft_grant_or_state_failure_rolls_back_everything(self):
        for case in self.craft_cases():
            for failure in ("grant", "state"):
                with self.subTest(handler=case[0].__name__, failure=failure):
                    self.grant(11, 2)
                    self.grant(104, 3)
                    target = (
                        (item_mgr, "grant_items")
                        if failure == "grant"
                        else (db, "set_life")
                    )
                    with patch.object(*target, side_effect=RuntimeError("synthetic failure")):
                        with self.assertRaisesRegex(RuntimeError, "synthetic"):
                            self.invoke_craft(case, 1)
                    self.assertEqual((self.num(11), self.num(104), self.num(106)), (2, 3, 0))
                    self.assertEqual(self.proficiency(case[6]), 0)
                    self.assertIsNone(db.get_kv(1, "activity_operations", f"{case[0]._SYSTEM}:0:1"))
                    self.assertEqual(self.invoke_craft(case, 1).status, OK)
                    item_mgr.consume_items(1, [(11, self.num(11)), (104, self.num(104)), (106, self.num(106))])
                    life = LifeBaseInfo.FromString(db.get_life(1, case[6]))
                    life.proficiency_value = 0
                    db.set_life(1, case[6], life.SerializeToString())
                    db.del_kv(1, "activity_operations")
                    db.del_kv(1, "life_book")

    def test_craft_concurrent_same_packet_and_distinct_packets(self):
        for case in self.craft_cases():
            for same_packet in (True, False):
                with self.subTest(handler=case[0].__name__, same_packet=same_packet):
                    self.grant(11, 2)
                    self.grant(104, 3)
                    statuses = self.concurrently([
                        (lambda i=i: self.invoke_craft(case, 1 if same_packet else i + 1, session=FakeSession()).status)
                        for i in range(6)
                    ])
                    self.assertEqual(statuses.count(OK), 6 if same_packet else 1)
                    self.assertEqual((self.num(11), self.num(104), self.num(106)), (0, 0, 1))
                    self.assertEqual(self.proficiency(case[6]), 10)
                    item_mgr.consume_items(1, [(11, self.num(11)), (104, self.num(104)), (106, self.num(106))])
                    life = LifeBaseInfo.FromString(db.get_life(1, case[6]))
                    life.proficiency_value = 0
                    db.set_life(1, case[6], life.SerializeToString())
                    db.del_kv(1, "activity_operations")
                    db.del_kv(1, "life_book")

    def restaurant_resources(self):
        return {
            "RestaurantGame": {
                "restaurant_desk_bag": {"datas": [{
                    "i_d": 12000,
                    "restaurant_desk_bag_group_info": [
                        {"bag_i_d": 1}, {"bag_i_d": 2}, {"bag_i_d": 3},
                    ],
                }]},
                "restaurant_dungeon": {"datas": [{"i_d": 10, "activity_i_d": 12000}]},
                "restaurant_game_food": {"datas": [{
                    "i_d": 12000,
                    "restaurant_game_food_group_info": [
                        {"food_i_d": 1, "synthetic_target": 2, "recycling_price_min": 2, "recycling_price_max": 2},
                        {"food_i_d": 2, "synthetic_target": 0, "recycling_price_min": 3, "recycling_price_max": 3},
                    ],
                }]},
                "restaurant_random_pool": {"datas": [{
                    "i_d": 1, "activity_i_d": 12000, "rate_min": 3, "rate_max": 3,
                    "restaurant_random_pool_group_info": [{"food_id": 1}],
                }]},
                "restaurant_game_quest": {"datas": [{
                    "i_d": 12000,
                    "restaurant_game_quest_group_info": [
                        {"game_quest_i_d": 1, "param": 10, "game_coin_i_d": 1601, "game_coin": 5},
                        {"game_quest_i_d": 2, "is_refresh": True, "new_restaurant_game_quest_type": 1, "game_coin_i_d": 106, "game_coin": 2},
                    ],
                }]},
            }
        }

    def set_restaurant_grids(self, grids):
        state = store.get_state(1, 12000)
        state["grids"] = grids
        store.save_state(1, 12000, state)
        return state

    def operate(self, packet_id, operate_type, grids=(), pool_id=1, session=None):
        session = session or FakeSession()
        req = RestaurantGameOperateReq(
            activity_id=12000, type=operate_type, grid=grids, buy_pool_id=pool_id
        )
        with patch.dict(res_loader.res, self.restaurant_resources()):
            restaurant_operate.Handler().handle(session, req.SerializeToString(), packet_id)
        return session.response(MsgId.RestaurantGameOperateRsp)

    def claim_quest(self, packet_id, quest_id=1, session=None):
        session = session or FakeSession()
        req = RestaurantGameQuestRewardReq(activity_id=12000, quest_id=quest_id)
        with patch.dict(res_loader.res, self.restaurant_resources()):
            restaurant_quest.Handler().handle(session, req.SerializeToString(), packet_id)
        return session.response(MsgId.RestaurantGameQuestRewardRsp)

    def test_restaurant_buy_success_replay_conflict_and_insufficient(self):
        self.grant(1601, 10)
        first = self.operate(1, RestaurantGameOperateType.RestaurantGameOperateType_Buy)
        self.assertEqual(first.status, OK)
        self.assertEqual((self.num(1601), store.get_state(1, 12000)["grids"][1]), (7, 1))
        replay = self.operate(1, RestaurantGameOperateType.RestaurantGameOperateType_Buy)
        self.assertEqual(replay.SerializeToString(), first.SerializeToString())
        self.assertEqual(self.num(1601), 7)
        self.assertEqual(self.operate(1, RestaurantGameOperateType.RestaurantGameOperateType_Buy, pool_id=999).status, BAD_REQ)
        item_mgr.consume_items(1, [(1601, 7)])
        self.grant(1601, 2)
        before = store.get_state(1, 12000)
        result = self.operate(2, RestaurantGameOperateType.RestaurantGameOperateType_Buy)
        self.assertEqual(result.status, ITEM_NOT_ENOUGH)
        self.assertEqual(store.get_state(1, 12000), before)
        self.assertEqual(self.num(1601), 2)

    def test_restaurant_mix_uses_exactly_two_authoritative_grids(self):
        self.set_restaurant_grids({1: 1, 2: 1, 3: 1})
        result = self.operate(1, RestaurantGameOperateType.RestaurantGameOperateType_Mix, grids=[1, 2])
        self.assertEqual(result.status, OK)
        self.assertEqual(store.get_state(1, 12000)["grids"], {1: 0, 2: 2, 3: 1})
        self.operate(1, RestaurantGameOperateType.RestaurantGameOperateType_Mix, grids=[1, 2])
        self.assertEqual(store.get_state(1, 12000)["grids"], {1: 0, 2: 2, 3: 1})
        before = store.get_state(1, 12000)
        rejected = self.operate(2, RestaurantGameOperateType.RestaurantGameOperateType_Mix, grids=[3, 3, 3])
        self.assertEqual(rejected.status, BAD_REQ)
        self.assertEqual(store.get_state(1, 12000), before)

    def test_restaurant_recycle_replay_and_rollback(self):
        self.set_restaurant_grids({1: 1, 2: 2, 3: 0})
        first = self.operate(1, RestaurantGameOperateType.RestaurantGameOperateType_Recycle, grids=[1, 2])
        self.assertEqual(first.status, OK)
        self.assertEqual(self.num(1601), 5)
        self.operate(1, RestaurantGameOperateType.RestaurantGameOperateType_Recycle, grids=[1, 2])
        self.assertEqual(self.num(1601), 5)

        self.set_restaurant_grids({1: 1, 2: 2, 3: 0})
        before = store.get_state(1, 12000)
        with patch.object(item_mgr, "grant_items", side_effect=RuntimeError("grant failure")):
            with self.assertRaisesRegex(RuntimeError, "grant failure"):
                self.operate(2, RestaurantGameOperateType.RestaurantGameOperateType_Recycle, grids=[1, 2])
        self.assertEqual(store.get_state(1, 12000), before)
        self.assertEqual(self.num(1601), 5)

    def test_restaurant_buy_state_failure_rolls_back_debit_and_grid(self):
        self.grant(1601, 10)
        before = store.get_state(1, 12000)
        with patch.object(store, "save_state", side_effect=RuntimeError("save failure")):
            with self.assertRaisesRegex(RuntimeError, "save failure"):
                self.operate(1, RestaurantGameOperateType.RestaurantGameOperateType_Buy)
        self.assertEqual(self.num(1601), 10)
        self.assertEqual(store.get_state(1, 12000), before)
        self.assertIsNone(db.get_kv(1, "activity_operations", "restaurant-operate:0:1"))

    def test_restaurant_config_error_and_concurrent_requests(self):
        self.grant(1601, 10)
        resources = self.restaurant_resources()
        resources["RestaurantGame"]["restaurant_random_pool"]["datas"][0]["rate_min"] = -1
        session = FakeSession()
        req = RestaurantGameOperateReq(activity_id=12000, type=RestaurantGameOperateType.RestaurantGameOperateType_Buy, buy_pool_id=1)
        with patch.dict(res_loader.res, resources):
            restaurant_operate.Handler().handle(session, req.SerializeToString(), 1)
        self.assertEqual(session.response(MsgId.RestaurantGameOperateRsp).status, CFG_NOT_FOUND)
        self.assertEqual(self.num(1601), 10)

        statuses = self.concurrently([
            (lambda: self.operate(2, RestaurantGameOperateType.RestaurantGameOperateType_Buy, session=FakeSession()).status)
            for _ in range(6)
        ])
        self.assertEqual(statuses, [OK] * 6)
        self.assertEqual(self.num(1601), 7)
        self.assertEqual(list(store.get_state(1, 12000)["grids"].values()).count(1), 1)

    def test_restaurant_quest_success_replay_and_daily_reset(self):
        state = store.get_state(1, 12000)
        state["completed"].add(10)
        store.save_state(1, 12000, state)
        first = self.claim_quest(1)
        self.assertEqual(first.status, OK)
        self.assertEqual(self.num(1601), 5)
        replay = self.claim_quest(1)
        self.assertEqual(replay.SerializeToString(), first.SerializeToString())
        self.assertEqual(self.num(1601), 5)
        self.assertEqual(self.claim_quest(2).status, REPEAT)
        self.assertEqual(self.num(1601), 5)

        with patch("utils.refresh.day_index", return_value=100):
            self.assertEqual(self.claim_quest(3, quest_id=2).status, OK)
            self.assertEqual(self.num(106), 2)
            self.assertEqual(self.claim_quest(4, quest_id=2).status, REPEAT)
        with patch("utils.refresh.day_index", return_value=101):
            self.assertEqual(self.claim_quest(3, quest_id=2).status, OK)
            self.assertEqual(self.num(106), 2)
            self.assertEqual(self.claim_quest(5, quest_id=2).status, OK)
            self.assertEqual(self.num(106), 4)

    def test_restaurant_quest_rejections_and_rollback(self):
        before = store.get_state(1, 12000)
        self.assertEqual(self.claim_quest(1).status, StatusCode.StatusCode_FAIL)
        self.assertEqual(store.get_state(1, 12000), before)
        state = store.get_state(1, 12000)
        state["completed"].add(10)
        store.save_state(1, 12000, state)
        before = store.get_state(1, 12000)
        with patch.object(item_mgr, "grant_items", side_effect=RuntimeError("grant failure")):
            with self.assertRaisesRegex(RuntimeError, "grant failure"):
                self.claim_quest(1)
        self.assertEqual(store.get_state(1, 12000), before)
        self.assertEqual(self.num(1601), 0)
        with patch.object(store, "save_state", side_effect=RuntimeError("save failure")):
            with self.assertRaisesRegex(RuntimeError, "save failure"):
                self.claim_quest(1)
        self.assertEqual(store.get_state(1, 12000), before)
        self.assertEqual(self.num(1601), 0)
        self.assertIsNone(db.get_kv(1, "activity_operations", "restaurant-quest-reward:0:1"))

        resources = self.restaurant_resources()
        resources["RestaurantGame"]["restaurant_game_quest"]["datas"][0]["restaurant_game_quest_group_info"][0]["game_coin"] = -1
        session = FakeSession()
        req = RestaurantGameQuestRewardReq(activity_id=12000, quest_id=1)
        with patch.dict(res_loader.res, resources):
            restaurant_quest.Handler().handle(session, req.SerializeToString(), 2)
        self.assertEqual(session.response(MsgId.RestaurantGameQuestRewardRsp).status, CFG_NOT_FOUND)
        self.assertEqual(store.get_state(1, 12000), before)

    def test_restaurant_quest_concurrent_claim_pays_once(self):
        state = store.get_state(1, 12000)
        state["completed"].add(10)
        store.save_state(1, 12000, state)
        statuses = self.concurrently([
            (lambda i=i: self.claim_quest(i + 1, session=FakeSession()).status)
            for i in range(6)
        ])
        self.assertEqual(statuses.count(OK), 1)
        self.assertEqual(statuses.count(REPEAT), 5)
        self.assertEqual(self.num(1601), 5)

    def test_craft_zero_packet_id_remains_supported(self):
        for case in self.craft_cases():
            with self.subTest(handler=case[0].__name__):
                self.grant(11, 2)
                self.grant(104, 3)
                result = self.invoke_craft(case, 0)
                self.assertEqual(result.status, OK)
                self.assertEqual((self.num(11), self.num(104), self.num(106)), (0, 0, 1))
                self.assertIsNone(db.get_kv(1, "activity_operations", f"{case[0]._SYSTEM}:0:0"))
                item_mgr.consume_items(1, [(11, self.num(11)), (104, self.num(104)), (106, self.num(106))])
                life = LifeBaseInfo.FromString(db.get_life(1, case[6]))
                life.proficiency_value = 0
                db.set_life(1, case[6], life.SerializeToString())
                db.del_kv(1, "life_book")

    def test_restaurant_zero_packet_id_remains_supported(self):
        self.grant(1601, 10)
        result = self.operate(0, RestaurantGameOperateType.RestaurantGameOperateType_Buy)
        self.assertEqual(result.status, OK)
        self.assertEqual((self.num(1601), store.get_state(1, 12000)["grids"][1]), (7, 1))
        self.assertIsNone(db.get_kv(1, "activity_operations", "restaurant-operate:0:0"))

    def test_collect_coin_zero_packet_id_remains_supported(self):
        with patch.object(Config, "DUNGEON_COIN_REWARD", (106, 7)):
            result = self.collect(0)
        self.assertEqual(result.status, OK)
        self.assertEqual(self.num(106), 7)
        self.assertEqual(DungeonData.FromString(db.get_dungeon(1, 10)).coins, [1])
        self.assertIsNone(db.get_kv(1, "activity_operations", "dungeon-collect-coin:0:0"))

    def test_economy_and_dungeon_locks_are_acquired_in_stable_order(self):
        from contextlib import contextmanager
        import handlers.DungeonSweep as sweep
        import handlers.DungeonTaskFinishReward as task_reward

        for module in (sweep, task_reward):
            with self.subTest(handler=module.__name__):
                events = []

                @contextmanager
                def track(name):
                    events.append(("enter", name))
                    try:
                        yield
                    finally:
                        events.append(("exit", name))

                session = FakeSession()
                economy = module.items_util if module is sweep else module.item_mgr
                with patch.object(economy, "player_lock", side_effect=lambda _pid: track("economy")), \
                     patch.object(module.dungeon_state, "player_lock", side_effect=lambda _pid: track("dungeon")), \
                     patch.object(module.Handler, "_handle"):
                    module.Handler().handle(session, b"", 1)
                self.assertEqual(events, [
                    ("enter", "economy"), ("enter", "dungeon"),
                    ("exit", "dungeon"), ("exit", "economy"),
                ])

    def dungeon_resources(self):
        return {
            "ScenesConfigAsset": {"Dungeons": [{"ID": 10, "CoinInfos": [{"Index": 1}]}]},
            "Dungeon": {"dungeon": {"datas": []}},
        }

    def collect(self, packet_id, coin_index=1, session=None):
        session = session or FakeSession(dungeon_id=10)
        with patch.dict(res_loader.res, self.dungeon_resources()):
            collect_coin.Handler().handle(
                session, CollectCoinReq(coin_index=coin_index).SerializeToString(), packet_id
            )
        return session.response(MsgId.CollectCoinRsp)

    def test_collect_coin_success_replay_conflict_and_authoritative_reward(self):
        with patch.object(Config, "DUNGEON_COIN_REWARD", (106, 7)):
            first = self.collect(1)
            self.assertEqual(first.status, OK)
            self.assertEqual(self.num(106), 7)
            self.assertEqual(DungeonData.FromString(db.get_dungeon(1, 10)).coins, [1])
            replay = self.collect(1, session=FakeSession(dungeon_id=10))
            self.assertEqual(replay.SerializeToString(), first.SerializeToString())
            self.assertEqual(self.num(106), 7)
            self.assertEqual(self.collect(1, coin_index=2).status, BAD_REQ)
            duplicate = self.collect(2)
            self.assertEqual(duplicate.status, OK)
            self.assertEqual(len(duplicate.rewards), 0)
            self.assertEqual(self.num(106), 7)

    def test_collect_coin_rejects_invalid_request_and_config(self):
        self.assertEqual(self.collect(1, coin_index=2).status, BAD_REQ)
        session = FakeSession(dungeon_id=0)
        with patch.dict(res_loader.res, self.dungeon_resources()):
            collect_coin.Handler().handle(session, CollectCoinReq(coin_index=1).SerializeToString(), 2)
        self.assertEqual(session.response(MsgId.CollectCoinRsp).status, StatusCode.StatusCode_PlayerNotInDungeon)
        with patch.object(Config, "DUNGEON_COIN_REWARD", (999999999, 1)):
            self.assertEqual(self.collect(3).status, CFG_NOT_FOUND)
        self.assertEqual(self.num(106), 0)
        self.assertIsNone(db.get_dungeon(1, 10))

    def test_collect_coin_grant_or_state_failure_rolls_back(self):
        for failure in ("grant", "state"):
            with self.subTest(failure=failure):
                target = (
                    (item_mgr, "grant_items")
                    if failure == "grant"
                    else (dungeon_state, "save")
                )
                with patch.object(*target, side_effect=RuntimeError("synthetic failure")):
                    with self.assertRaisesRegex(RuntimeError, "synthetic"):
                        self.collect(1)
                self.assertEqual(self.num(106), 0)
                self.assertIsNone(db.get_dungeon(1, 10))
                self.assertIsNone(db.get_kv(1, "activity_operations", "dungeon-collect-coin:0:1"))

    def test_collect_coin_concurrent_same_and_distinct_packets(self):
        for same_packet in (True, False):
            with self.subTest(same_packet=same_packet):
                statuses = self.concurrently([
                    (lambda i=i: self.collect(1 if same_packet else i + 1, session=FakeSession(dungeon_id=10)).status)
                    for i in range(6)
                ])
                self.assertEqual(statuses, [OK] * 6)
                self.assertEqual(self.num(106), 1)
                self.assertEqual(DungeonData.FromString(db.get_dungeon(1, 10)).coins, [1])
                item_mgr.consume_items(1, [(106, 1)])
                db.db.execute("DELETE FROM dungeons WHERE player_id=1")
                db.del_kv(1, "activity_operations")


if __name__ == "__main__":
    unittest.main()
