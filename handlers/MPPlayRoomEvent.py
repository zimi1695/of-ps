import time

from network.msg_id import MsgId
from network.packet_handler import PacketHandler, packet_handler
from proto.net_pb2 import MPPlayRoomEventReq, MPPlayRoomEventRsp, MPPlayRoomEventNotice, MPPlayRoomEventType, MPBeaconStatus, StatusCode
import server.mp_team as mp_team
from server import minigames


@packet_handler(MsgId.MPPlayRoomEventReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = MPPlayRoomEventReq()
        req.ParseFromString(data)
        rsp = MPPlayRoomEventRsp()
        with mp_team.lock:
            team = mp_team.get_team_by_player(session.player_id)
            if team is None or team["room"] is None:
                rsp.status = StatusCode.StatusCode_PLAYER_NOT_IN_MP_ROOM
                session.send(MsgId.MPPlayRoomEventRsp, rsp, packet_id)
                return
            room = team["room"]
            if session.player_id in room["exited"]:
                rsp.status = StatusCode.StatusCode_BAD_REQ
                session.send(MsgId.MPPlayRoomEventRsp, rsp, packet_id)
                return
            run = minigames.ensure_run(team)
            if run["status"] == "settled":
                # 重复/重连上报只确认，不重复发放或广播结算。
                rsp.status = StatusCode.StatusCode_OK
                session.send(MsgId.MPPlayRoomEventRsp, rsp, packet_id)
                return
            if any(event.event_type == MPPlayRoomEventType.MPPlayRoomEvent_GameStart for event in req.events):
                rsp.status = StatusCode.StatusCode_BAD_REQ
                session.send(MsgId.MPPlayRoomEventRsp, rsp, packet_id)
                return
            end_events = [
                event
                for event in req.events
                if event.event_type == MPPlayRoomEventType.MPPlayRoomEvent_GameEnd
            ]
            if len(end_events) > 1 or (end_events and team["leader"] != session.player_id):
                rsp.status = StatusCode.StatusCode_BAD_REQ
                session.send(MsgId.MPPlayRoomEventRsp, rsp, packet_id)
                return

            if end_events:
                try:
                    run, changed = minigames.settle(
                        team, reported_star=int(end_events[0].star)
                    )
                except minigames.RoomRewardConfigError:
                    rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                    session.send(MsgId.MPPlayRoomEventRsp, rsp, packet_id)
                    return
                except minigames.RoomStateError:
                    rsp.status = StatusCode.StatusCode_BAD_REQ
                    session.send(MsgId.MPPlayRoomEventRsp, rsp, packet_id)
                    return
                if changed:
                    room["ended"] = True
                    for player_id in sorted(int(pid) for pid in run["members"]):
                        mp_team.send_to_players(
                            [player_id], MsgId.MPPlayRoomSettlementNotice,
                            minigames.settlement_notice(run, player_id),
                        )
                    if team["beacon"] is not None:
                        team["beacon"]["status"] = MPBeaconStatus.MPBeaconStatus_PLACED
                    mp_team._persist()

            if req.events:
                notice = MPPlayRoomEventNotice(status=StatusCode.StatusCode_OK)
                now_ms = int(time.time() * 1000)
                for event in req.events:
                    clean = notice.events.add()
                    clean.CopyFrom(event)
                    clean.player_id = session.player_id
                    clean.play_mode = team["game_mode"]
                    clean.time = now_ms
                    clean.game_start_time = int(run.get("start_ms") or room["game_start_time"])
                    if event.event_type == MPPlayRoomEventType.MPPlayRoomEvent_GameEnd:
                        clean.star = int(run["star"])
                        clean.count = 0
                        clean.time = int(run["end_ms"])
                    else:
                        clean.star = 0
                mp_team.broadcast(team, MsgId.MPPlayRoomEventNotice, notice, exclude=session.player_id)
            rsp.status = StatusCode.StatusCode_OK
        session.send(MsgId.MPPlayRoomEventRsp, rsp, packet_id)
