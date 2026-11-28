import base64
import json
import logging

import utils.db as db

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId

from proto.net_pb2 import (
    PackNotice,
    QuestNotice,
    StatusCode,
    TutorialReq,
    TutorialRsp,
)

from config import Config
from utils import item_mgr, flag_state, flag_config, achieve_track
from utils.pb_create import make_Quest
from utils.reward_resolver import resolve_reward
from server import redemption

logger = logging.getLogger(__name__)


@packet_handler(MsgId.TutorialReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = TutorialReq()
        req.ParseFromString(data)

        rsp = TutorialRsp(status=StatusCode.StatusCode_OK)
        pack = PackNotice(status=StatusCode.StatusCode_OK)
        fingerprint = json.dumps([req.tutorial_id], separators=(",", ":"))
        try:
            # Item/achievement notifications are released only after commit.
            with item_mgr.deferred_notifications(), achieve_track.batch(session):
                status, payload = redemption.run_operation(
                    session.player_id,
                    "tutorial",
                    0,
                    packet_id,
                    fingerprint,
                    rsp,
                    pack,
                    lambda: self._apply_tutorial(session, req, rsp, pack),
                )
        except Exception:
            logger.exception("Tutorial fulfillment failed")
            rsp.Clear()
            rsp.status = StatusCode.StatusCode_SYS_ERR
            session.send(MsgId.TutorialRsp, rsp, packet_id)
            return

        if status == "conflict":
            rsp.status = StatusCode.StatusCode_BAD_REQ
        elif status != "ok":
            rsp.status = StatusCode.StatusCode_SYS_ERR
        session.send(MsgId.TutorialRsp, rsp, packet_id)
        if status != "ok":
            return

        if pack.items:
            session.send(MsgId.PackNotice, pack, 0)
        payload = payload or {}
        flag_state.notify_added(session, payload.get("flags", ()))
        quest_blob = payload.get("quest")
        if quest_blob:
            quest = make_Quest(session.player_id, 100011)
            quest.ParseFromString(base64.b64decode(quest_blob, validate=True))
            session.quests[quest.quest_id] = quest
            quest_notice = QuestNotice(status=StatusCode.StatusCode_OK)
            quest_notice.quests.add().CopyFrom(quest)
            session.send(MsgId.QuestNotice, quest_notice, 0)

    def _apply_tutorial(self, session, req, rsp, pack):
        player_id = session.player_id
        first_open_space_reward = (
            not Config.SKIP_QUESTS
            and req.tutorial_id == 9
            and db.try_claim(player_id, "tutorial_reward", "open_space")
        )

        # 教程完成记录(每次新 packet 计数; 同 packet 重放不进入此回调)
        if req.tutorial_id:
            db.incr_kv(player_id, "tutorial", str(req.tutorial_id), 1)

        # 引导完成留档(kv ns=tutorial), 供后续条件判定引用
        done = db.get_kv(player_id, "tutorial", "ids", b"")
        ids = {x for x in done.decode().split(",") if x}
        ids.add(str(req.tutorial_id))
        db.set_kv(player_id, "tutorial", "ids", ",".join(sorted(ids)).encode())

        if first_open_space_reward:
            level_row = db.get_players_info(player_id, "level")
            level = level_row[0] if level_row else 1
            reward_list = list(resolve_reward(600, level))
            if not reward_list:
                reward_list = [
                    (item_id, 1) for item_id in Config.INITIAL_UNLOCK_ITEMS
                ]
            reward_list.append((4050, 1))
            item_mgr.grant_items(
                player_id,
                reward_list,
                pack_notice=pack,
                session=session,
                unlock_function_ids=Config.TUTORIAL_UNLOCK_FUNCTIONS,
            )

        quest_blob = ""
        added_flags = []
        if (
            not Config.SKIP_QUESTS
            and req.tutorial_id == 9
            and not db.get_quest(player_id, 100011)
        ):
            # 初始空间离开后，正式主线的第一环由服务端主动下发。
            quest = make_Quest(player_id, 100011)
            db.set_quest(
                player_id, quest.quest_id, quest.SerializeToString()
            )
            condition_ids = [condition.condition_id for condition in quest.conditions]
            added_flags = flag_state.unlock_flags(
                session,
                flag_config.flag_ids_for_conditions(condition_ids),
                notify=False,
            )
            quest_blob = base64.b64encode(quest.SerializeToString()).decode("ascii")

        rsp.status = StatusCode.StatusCode_OK
        return "ok", {"quest": quest_blob, "flags": added_flags}
