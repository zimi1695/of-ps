import pickle
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from config import Config
from network.msg_id import MsgId
from proto.net_pb2 import (
    MPPlayRoomEvent,
    MPPlayRoomEventNotice,
    MPPlayRoomEventReq,
    MPPlayRoomEventRsp,
    MPPlayRoomEventType,
    StatusCode,
)
import handlers.MPPlayRoomEvent as room_event
from server import minigames
import utils.db as db
from utils import item_mgr, res_loader


class FakeSession:
    def __init__(self, player_id):
        self.player_id = player_id
        self.sent = []

    def send(self, message_id, message, packet_id=0):
        self.sent.append((message_id, message, packet_id))

    def response(self, message_id):
        return [message for mid, message, _ in self.sent if mid == message_id][-1]


class MPRoomEventAuthorityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        res_loader.init()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_path = Config.DB_PATH
        self.old_memory = Config.IN_MEMORY
        db.exit()
        Config.DB_PATH = str(Path(self.tmp.name) / "mp-room.db")
        Config.IN_MEMORY = False
        db.init()
        for player_id in (1, 2):
            db.db.execute(
                "INSERT INTO players(player_id,player_name,level,world_level,unlock_functions) "
                "VALUES (?,?,10,1,?)",
                (player_id, f"mp-{player_id}", pickle.dumps([])),
            )
        self.team = {
            "team_id": 1,
            "leader": 1,
            "mode_id": 1001,
            "sub_mode_id": 100101,
            "gameplay_id": 10010101,
            "game_mode": 1,
            "members": {1: {"character_id": 1}, 2: {"character_id": 2}},
            "beacon": None,
            "room": {
                "game_start_time": 1000,
                "ready": {1, 2},
                "exited": set(),
                "fg_bytes": {},
            },
        }
        self.broadcasts = []
        self.settlements = []

    def tearDown(self):
        db.exit()
        Config.DB_PATH = self.old_path
        Config.IN_MEMORY = self.old_memory
        self.tmp.cleanup()

    def invoke(self, player_id, events, packet_id=1):
        session = FakeSession(player_id)
        request = MPPlayRoomEventReq(events=events)
        with patch.object(room_event.mp_team, "get_team_by_player", return_value=self.team), \
             patch.object(room_event.mp_team, "broadcast", side_effect=lambda *args, **kwargs: self.broadcasts.append(args)), \
             patch.object(room_event.mp_team, "send_to_players", side_effect=lambda *args: self.settlements.append(args)), \
             patch.object(room_event.mp_team, "_persist"):
            room_event.Handler().handle(session, request.SerializeToString(), packet_id)
        return session.response(MsgId.MPPlayRoomEventRsp)

    def start(self, now_ms=1000):
        return minigames.mark_started(self.team, now_ms)

    def test_client_star_cannot_select_reward_without_server_timers(self):
        self.start()
        item_mgr.grant_items(1, [(101, 0)])
        with patch.object(Config, "MP_ROOM_REWARDS", {
            (1001, 100101, 10010101, 0): {"items": ((101, 999),)},
            (1001, 100101, 10010101, 3): {"items": ((101, 50),)}
        }, create=True), patch.object(Config, "MP_ROOM_STAR_TIMERS_MS", {}, create=True), \
             patch.object(minigames.time, "time", return_value=2.0):
            response = self.invoke(1, [MPPlayRoomEvent(
                event_type=MPPlayRoomEventType.MPPlayRoomEvent_GameEnd,
                player_id=999,
                star=3,
                count=999,
            )])
        self.assertEqual(response.status, StatusCode.StatusCode_OK)
        run = minigames.load_run(self.team["room"]["run_id"])
        self.assertEqual((run["star"], run["reported_star"]), (0, 3))
        self.assertEqual(item_mgr.get_num(1, 101), 0)
        self.assertEqual(item_mgr.get_num(2, 101), 0)
        notice = self.broadcasts[0][2]
        self.assertEqual(notice.events[0].star, 0)
        self.assertEqual(notice.events[0].player_id, 1)
        self.assertEqual(notice.events[0].count, 0)

    def test_server_timer_selects_star_and_pays_each_member_once(self):
        self.start()
        with patch.object(Config, "MP_ROOM_REWARDS", {
            (1001, 100101, 10010101, 2): {"items": ((101, 5),)}
        }, create=True), patch.object(Config, "MP_ROOM_STAR_TIMERS_MS", {
            (1001, 100101, 10010101): (500, 1500, 2500)
        }, create=True), patch.object(minigames.time, "time", return_value=2.0):
            first = self.invoke(1, [MPPlayRoomEvent(
                event_type=MPPlayRoomEventType.MPPlayRoomEvent_GameEnd,
                star=3,
            )], packet_id=1)
            replay = self.invoke(1, [MPPlayRoomEvent(
                event_type=MPPlayRoomEventType.MPPlayRoomEvent_GameEnd,
                star=3,
            )], packet_id=2)
        self.assertEqual((first.status, replay.status), (StatusCode.StatusCode_OK, StatusCode.StatusCode_OK))
        run = minigames.load_run(self.team["room"]["run_id"])
        self.assertEqual((run["star"], run["reported_star"], run["duration_ms"]), (2, 3, 1000))
        self.assertEqual((item_mgr.get_num(1, 101), item_mgr.get_num(2, 101)), (5, 5))
        self.assertEqual(len(self.settlements), 2)
        self.assertEqual(len(self.broadcasts), 1)

    def test_non_leader_cannot_end_room(self):
        self.start()
        response = self.invoke(2, [MPPlayRoomEvent(
            event_type=MPPlayRoomEventType.MPPlayRoomEvent_GameEnd,
            star=3,
        )])
        self.assertEqual(response.status, StatusCode.StatusCode_BAD_REQ)
        run = minigames.load_run(self.team["room"]["run_id"])
        self.assertEqual(run["status"], "active")
        self.assertFalse(self.broadcasts)
        self.assertFalse(self.settlements)

    def test_social_event_identity_and_time_are_rewritten(self):
        self.start()
        response = self.invoke(2, [MPPlayRoomEvent(
            event_type=MPPlayRoomEventType.MPPlayRoomEvent_DestroyObject,
            player_id=999,
            time=1,
            game_start_time=2,
            play_mode=99,
            star=3,
            count=7,
        )])
        self.assertEqual(response.status, StatusCode.StatusCode_OK)
        event = self.broadcasts[0][2].events[0]
        self.assertEqual(event.player_id, 2)
        self.assertEqual(event.play_mode, 1)
        self.assertEqual(event.game_start_time, 1000)
        self.assertEqual(event.star, 0)
        self.assertEqual(event.count, 7)

    def test_invalid_reported_star_and_multiple_end_events_are_rejected(self):
        self.start()
        for events in (
            [MPPlayRoomEvent(event_type=MPPlayRoomEventType.MPPlayRoomEvent_GameEnd, star=9)],
            [
                MPPlayRoomEvent(event_type=MPPlayRoomEventType.MPPlayRoomEvent_GameEnd, star=1),
                MPPlayRoomEvent(event_type=MPPlayRoomEventType.MPPlayRoomEvent_GameEnd, star=2),
            ],
        ):
            response = self.invoke(1, events)
            self.assertEqual(response.status, StatusCode.StatusCode_BAD_REQ)
        run = minigames.load_run(self.team["room"]["run_id"])
        self.assertEqual(run["status"], "active")
        self.assertFalse(self.broadcasts)


if __name__ == "__main__":
    unittest.main()
