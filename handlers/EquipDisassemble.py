import hashlib
import math
import random

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId
from proto.net_pb2 import EquipDisassembleReq, EquipDisassembleRsp, StatusCode, PackNotice

from server import activities, character_growth as growth
from utils import achieve_track, item_mgr
import utils.db as db


_SYSTEM = "equip-disassemble"


def _rewards(kind, equip):
    try:
        row = growth.disassemble_row(kind, equip)
    except growth.GrowthError:
        raise
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND) from error
    entries = row.get(f"{kind}_disassemble_item", ())
    if not isinstance(entries, (list, tuple)) or not entries:
        raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND)
    parsed = []
    try:
        for entry in entries:
            if not isinstance(entry, dict):
                raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND)
            item_id = entry.get("item_i_d", 0)
            rate = entry.get("item_rate", 1)
            low = entry.get("item_min_count", 1)
            high = entry.get("item_max_count", low)
            if (
                type(item_id) is not int
                or type(low) is not int
                or type(high) is not int
                or isinstance(rate, bool)
                or not isinstance(rate, (int, float))
            ):
                raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND)
            item_mgr.validate_stackable_item_id(item_id)
            item_mgr.validate_count(low, label="disassemble minimum reward")
            item_mgr.validate_count(high, label="disassemble maximum reward")
            if not math.isfinite(rate) or not 0 <= rate <= 1 or high < low:
                raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND)
            parsed.append((item_id, rate, low, high))
    except item_mgr.EconomyConfigError as error:
        raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND) from error
    except (TypeError, ValueError, OverflowError) as error:
        raise growth.GrowthError(StatusCode.StatusCode_CFG_NOT_FOUND) from error
    return tuple(parsed)


@packet_handler(MsgId.EquipDisassembleReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = EquipDisassembleReq()
        req.ParseFromString(data)
        rsp = EquipDisassembleRsp(status=StatusCode.StatusCode_OK)
        notice = PackNotice(status=StatusCode.StatusCode_OK)
        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()
        replay = False
        try:
            with growth.transaction(session.player_id), item_mgr.deferred_notifications():
                saved = (
                    activities._load_operation(
                        session.player_id, _SYSTEM, 0, packet_id, fingerprint
                    )
                    if packet_id
                    else None
                )
                if saved == "conflict":
                    rsp.status = StatusCode.StatusCode_BAD_REQ
                elif saved is not None:
                    saved_response, saved_pack = saved
                    rsp.ParseFromString(saved_response)
                    notice.ParseFromString(saved_pack)
                    replay = True
                else:
                    seen = set()
                    pending = []
                    grant_list = []
                    for kind, ids, response_ids in (
                        ("weapon", req.weapon_ids, rsp.weapon_ids),
                        ("armor", req.armor_ids, rsp.armor_ids),
                    ):
                        for instance_id in ids:
                            if not instance_id or instance_id in seen:
                                continue
                            seen.add(instance_id)
                            try:
                                detail = growth.load_owned_instance(
                                    session.player_id, instance_id, kind
                                )
                            except growth.GrowthError as error:
                                # 批量分解保留跳过不存在、已归属错误或类型不符项的语义。
                                if error.status == StatusCode.StatusCode_BAD_REQ:
                                    continue
                                raise
                            equip = getattr(detail.main_item, kind)
                            if equip.is_lock or equip.wearer_id:
                                continue
                            if kind == "weapon" and equip.inscription1:
                                continue
                            rewards = _rewards(kind, equip)
                            for item_id, rate, low, high in rewards:
                                if random.random() < rate:
                                    grant_list.append(
                                        (item_id, random.randint(low, high))
                                    )
                            pending.append(instance_id)
                            response_ids.append(instance_id)

                    # 所有资格、奖励配置及随机结果均确定后才开始删除和发奖。
                    totals = {}
                    try:
                        for item_id, count in grant_list:
                            totals[item_id] = item_mgr.checked_add(
                                totals.get(item_id, 0), count
                            )
                    except item_mgr.EconomyConfigError as error:
                        raise growth.GrowthError(
                            StatusCode.StatusCode_CFG_NOT_FOUND
                        ) from error
                    for instance_id in pending:
                        db.del_item_detail(session.player_id, instance_id)
                    if grant_list:
                        item_mgr.grant_items(
                            session.player_id,
                            grant_list,
                            rsp_items=rsp.items,
                            session=None,
                        )
                    activities._save_operation(
                        session.player_id,
                        _SYSTEM,
                        0,
                        packet_id,
                        fingerprint,
                        rsp,
                        notice,
                    )
        except growth.GrowthError as error:
            rsp.Clear()
            rsp.status = error.status
            notice.Clear()
        except item_mgr.EconomyConfigError:
            rsp.Clear()
            rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
            notice.Clear()

        if not replay and (rsp.weapon_ids or rsp.armor_ids):
            achieve_track.report(
                session,
                "disassemble",
                armor_n=len(rsp.armor_ids),
                weapon_n=len(rsp.weapon_ids),
            )
        session.send(MsgId.EquipDisassembleRsp, rsp, packet_id)
