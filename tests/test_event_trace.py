import threading
import unittest

from proto.net_pb2 import (
    ChatMsgData,
    EntityOwnershipTransfer,
    PacketHead,
    PopEmojiNotice,
    SendActionNotice,
)
from network.game_session import GameSession
from utils import event_trace
from utils.metrics import metrics


class FakeSocket:
    def setsockopt(self, *args):
        pass

    def shutdown(self, *args):
        pass

    def close(self, *args):
        pass


class EventTraceTests(unittest.TestCase):
    def setUp(self):
        metrics.reset()

    def tearDown(self):
        metrics.reset()

    def test_protocol_fields_roundtrip_without_changing_legacy_values(self):
        chat = ChatMsgData(
            player_id=7,
            text="hello",
            session_generation=11,
            event_seq=12,
            trace_id=13,
            produced_at_us=14,
        )
        restored = ChatMsgData.FromString(chat.SerializeToString())
        self.assertEqual(restored.player_id, 7)
        self.assertEqual(restored.text, "hello")
        self.assertEqual(restored.session_generation, 11)
        self.assertEqual(restored.event_seq, 12)
        self.assertEqual(restored.trace_id, 13)
        self.assertEqual(restored.produced_at_us, 14)
        self.assertEqual(PopEmojiNotice(event_seq=3).event_seq, 3)
        self.assertEqual(SendActionNotice(event_seq=4).event_seq, 4)
        transfer = EntityOwnershipTransfer(
            domain="scene:1:channel:2:item:3",
            authority_epoch=5,
            entity_id=6,
            prior_owner_generation=7,
            prior_owner_id="host-a",
            new_owner_generation=8,
            new_owner_id="host-b",
            transfer_seq=9,
            signature=b"signature",
            issued_at_us=10,
        )
        self.assertEqual(
            EntityOwnershipTransfer.FromString(transfer.SerializeToString()),
            transfer,
        )

    def test_trace_context_is_propagated_and_reset(self):
        trace = event_trace.new_trace(123, 456)
        token = event_trace.activate(trace)
        try:
            self.assertEqual(event_trace.current(), trace)
        finally:
            event_trace.reset(token)
        self.assertIsNone(event_trace.current())

    def test_receive_callback_records_elapsed_time(self):
        event_trace.record_receive(1, 1_000_000, callback_at_us=1_002_500)
        timer = metrics.snapshot()["timers"]["ofps.trace.produced_to_receive_seconds"]
        self.assertAlmostEqual(timer["total_seconds"], 0.0025)

    def test_timer_percentiles_are_bounded_and_reported(self):
        for value in range(1, 101):
            metrics.observe("latency", value / 1000)
        timer = metrics.snapshot()["timers"]["latency"]
        self.assertAlmostEqual(timer["p50_seconds"], 0.050)
        self.assertAlmostEqual(timer["p95_seconds"], 0.095)
        self.assertAlmostEqual(timer["p99_seconds"], 0.099)

    def test_session_event_sequence_is_unique_under_concurrency(self):
        session = GameSession(FakeSocket(), ("synthetic", 1))
        values = []
        lock = threading.Lock()

        def allocate():
            for _ in range(100):
                value = session.next_event_seq()
                with lock:
                    values.append(value)

        threads = [threading.Thread(target=allocate) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(values), list(range(1, 401)))
        self.assertGreater(session.session_generation, 0)


if __name__ == "__main__":
    unittest.main()
