import hashlib

from network.packet_handler import PacketHandler, packet_handler
from network.msg_id import MsgId

from proto.net_pb2 import (
    BlessTreeUnlockReq,
    BlessTreeUnlockRsp,
    StatusCode,
    PackNotice,
)

from utils.res_loader import res
from server import activities

import utils.db as db
from utils import item_mgr


_OPERATION_SYSTEM = "bless-tree-unlock"


def _rows(sheet, table):
    section = res.get(sheet, {}).get(table, {})
    rows = section.get("datas", ()) if isinstance(section, dict) else ()
    if not isinstance(rows, (list, tuple)):
        return ()
    return tuple(row for row in rows if isinstance(row, dict))


def _validated_node(def_id, tree_id):
    trees = [row for row in _rows("BlessingTree", "blessing_tree_info") if row.get("i_d") == def_id]
    if len(trees) != 1:
        raise item_mgr.EconomyConfigError("missing or duplicate blessing tree config")
    raw_nodes = trees[0].get("blessing_tree_info_group_info", ())
    if not isinstance(raw_nodes, (list, tuple)):
        raise item_mgr.EconomyConfigError("invalid blessing tree nodes")
    nodes = tuple(row for row in raw_nodes if isinstance(row, dict))
    if len(nodes) != len(raw_nodes):
        raise item_mgr.EconomyConfigError("invalid blessing tree node")
    matches = [row for row in nodes if row.get("box_i_d") == tree_id]
    if len(matches) != 1:
        raise item_mgr.EconomyConfigError("missing or duplicate blessing node config")
    node = matches[0]
    node_ids = {row.get("box_i_d") for row in nodes}
    relate_id = node.get("relate_box_i_d", 0)
    if type(relate_id) is not int or relate_id < 0 or (relate_id and relate_id not in node_ids):
        raise item_mgr.EconomyConfigError("invalid blessing node prerequisite")

    cost_item = node.get("item_i_d", 0)
    cost_count = node.get("item_count", 0)
    item_mgr.validate_stackable_item_id(cost_item)
    item_mgr.validate_count(cost_count, label="blessing cost")
    reward_item = node.get("reward_item_i_d", 0)
    reward_count = node.get("reward_item_count", 0)
    item_mgr.validate_count(reward_count, label="blessing reward")
    item_mgr.validate_rewards(((reward_item, reward_count),))
    return node, relate_id


@packet_handler(MsgId.BlessTreeUnlockReq)
class Handler(PacketHandler):
    def handle(self, session, data: bytes, packet_id: int):
        req = BlessTreeUnlockReq()
        req.ParseFromString(data)

        rsp = BlessTreeUnlockRsp(def_id=req.def_id, tree_id=req.tree_id)

        fingerprint = hashlib.sha256(req.SerializeToString()).hexdigest()
        notice = PackNotice(status=StatusCode.StatusCode_OK)
        with item_mgr.deferred_notifications(), item_mgr.player_lock(session.player_id), db.transaction():
            operation = activities._load_operation(
                session.player_id, _OPERATION_SYSTEM, 0, packet_id, fingerprint
            )
            if operation == "conflict":
                rsp.status = StatusCode.StatusCode_BAD_REQ
            elif operation is not None:
                saved_response, saved_pack = operation
                rsp.ParseFromString(saved_response)
                notice.ParseFromString(saved_pack)
            else:
                try:
                    node, relate_id = _validated_node(req.def_id, req.tree_id)
                except (KeyError, TypeError, ValueError):
                    rsp.status = StatusCode.StatusCode_CFG_NOT_FOUND
                else:
                    stored_ids = db.get_bless_tree(session.player_id, req.def_id)
                    if not isinstance(stored_ids, (list, tuple)):
                        rsp.status = StatusCode.StatusCode_FAIL
                    else:
                        tree_ids = list(stored_ids)
                        if req.tree_id in tree_ids:
                            rsp.status = StatusCode.StatusCode_PLAYER_BLESS_TREE_EXIST
                        elif relate_id and relate_id not in tree_ids:
                            rsp.status = StatusCode.StatusCode_PLAYER_BLESS_NEED_NODE
                        elif not item_mgr.consume_items(
                            session.player_id,
                            [(node["item_i_d"], node["item_count"])],
                            pack_notice=notice,
                        ):
                            rsp.status = StatusCode.StatusCode_ITEM_NOT_ENOUGH
                        else:
                            tree_ids.append(req.tree_id)
                            db.set_bless_tree(session.player_id, req.def_id, tree_ids)
                            item_mgr.grant_items(
                                session.player_id,
                                [(node["reward_item_i_d"], node["reward_item_count"])],
                                rsp_items=rsp.rewards,
                                pack_notice=notice,
                            )
                            rsp.status = StatusCode.StatusCode_OK
                            activities._save_operation(
                                session.player_id,
                                _OPERATION_SYSTEM,
                                0,
                                packet_id,
                                fingerprint,
                                rsp,
                                notice,
                            )

        session.send(MsgId.BlessTreeUnlockRsp, rsp, packet_id)
        if rsp.status == StatusCode.StatusCode_OK and notice.items:
            session.send(MsgId.PackNotice, notice, 0)
