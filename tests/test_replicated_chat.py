import unittest
from types import SimpleNamespace
from unittest.mock import patch

from proto.net_pb2 import (
    ChatChannelType,
    SendChatMsgReq,
    SendChatMsgRsp,
    StatusCode,
)
from handlers.SendChatMsg import Handler
from server import replicated_events
from utils.metrics import metrics


class AllowingLimiter:
    def allow(self, name):
        return True


class ReplicatedChatTests(unittest.TestCase):
    def setUp(self):
        metrics.reset()
        self.sent = []
        self.session = SimpleNamespace(
            player_id=7,
            session_generation=8,
            avatar_id=1,
            badge_id=2,
            player_name="tester",
            scene_id=3,
            channel_id=4,
            chat_channel_id=5,
            rate_limiter=AllowingLimiter(),
            send=lambda msg_id, message, packet_id: self.sent.append(
                (msg_id, message, packet_id)
            ),
        )

    def tearDown(self):
        replicated_events.deactivate(self)
        metrics.reset()

    def run_handler(self, committed):
        response = SendChatMsgRsp(
            status=StatusCode.StatusCode_OK, text="hello"
        ).SerializeToString()
        log = SimpleNamespace(
            append_once=lambda *args: (response, committed)
        )
        replicated_events.activate(log)
        request = SendChatMsgReq(
            type=ChatChannelType.ChatChannel_Default, text="hello"
        )
        with patch("handlers.SendChatMsg.db.get_players_info", return_value=[0]), patch(
            "handlers.SendChatMsg.append_chat_history"
        ), patch("handlers.SendChatMsg.up_chat_msg") as broadcast, patch(
            "handlers.SendChatMsg.social.player_locks"
        ):
            Handler().handle(self.session, request.SerializeToString(), 9)
        return broadcast

    def test_committed_chat_is_broadcast_once(self):
        broadcast = self.run_handler(True)
        broadcast.assert_called_once()
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.sent[0][2], 9)

    def test_replayed_chat_returns_response_without_rebroadcast(self):
        broadcast = self.run_handler(False)
        broadcast.assert_not_called()
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.sent[0][1].text, "hello")
        self.assertEqual(
            metrics.snapshot()["counters"].get("ofps.events.replayed_total", 0),
            0,
        )


if __name__ == "__main__":
    unittest.main()
