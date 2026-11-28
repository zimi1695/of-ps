import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from server import replicated_roster, shared_projection
from server.scene_data import _session_list as session_list


class FakeAuthority:
    def __init__(self, value=None):
        self.value = value
        self.commits = []
        self.credentials = []

    def load_roster(self, channel_id):
        return self.value

    def commit_roster(self, credential, value):
        self.credentials.append(credential)
        self.commits.append(value)
        self.value = value


class ReplicatedRosterTests(unittest.TestCase):
    def setUp(self):
        self.old_sessions = list(session_list)
        session_list.clear()
        shared_projection.store.clear()
        self.credential = SimpleNamespace(channel_id="public")

    def tearDown(self):
        session_list[:] = self.old_sessions
        shared_projection.store.clear()
        replicated_roster.deactivate(self)

    def add_member(self, player_id):
        session_list.append(
            SimpleNamespace(
                player_id=player_id,
                scene_id=1,
                channel_id=2,
                running=True,
                logged_in=True,
                remote=False,
                server_name="",
            )
        )

    def test_membership_change_increments_roster_version(self):
        authority = FakeAuthority()
        replicator = replicated_roster.RosterReplicator(authority, self.credential)
        self.add_member(7)
        first = json.loads(replicator.publish())
        self.assertEqual([item["player_id"] for item in first["members"]], [7])
        second = json.loads(replicator.publish())
        self.assertEqual(second["roster_seq"], first["roster_seq"])
        self.add_member(8)
        third = json.loads(replicator.publish())
        self.assertEqual(third["roster_seq"], first["roster_seq"] + 1)
        self.assertEqual(len(authority.commits), 3)

    def test_publish_uses_current_credential_provider(self):
        authority = FakeAuthority()
        current = [self.credential]
        replicator = replicated_roster.RosterReplicator(
            authority, lambda: current[0]
        )
        self.add_member(7)
        replicator.publish()
        renewed = SimpleNamespace(channel_id="public", key_revision=2)
        current[0] = renewed
        replicator.publish()
        self.assertIs(authority.credentials[0], self.credential)
        self.assertIs(authority.credentials[1], renewed)

    def test_stale_roster_cannot_replace_newer_version(self):
        authority = FakeAuthority(
            json.dumps(
                {
                    "config_generation": 1,
                    "authority_epoch": 1,
                    "roster_seq": 999,
                    "members": [],
                }
            ).encode()
        )
        replicator = replicated_roster.RosterReplicator(authority, self.credential)
        self.add_member(7)
        with self.assertRaises(ValueError):
            replicator.publish()
        self.assertEqual(authority.commits, [])

    def test_paused_replicator_does_not_overwrite_remote_roster(self):
        remote = json.dumps(
            {
                "config_generation": 1,
                "authority_epoch": 1,
                "roster_seq": 0,
                "members": [{"player_id": 99}],
            }
        ).encode()
        authority = FakeAuthority(remote)
        replicator = replicated_roster.RosterReplicator(
            authority, self.credential, paused=True
        )
        baseline = shared_projection.store.roster_metadata()

        self.assertIsNone(replicator.publish())
        self.assertEqual(authority.commits, [])
        self.assertEqual(authority.value, remote)
        self.assertEqual(shared_projection.store.roster_metadata(), baseline)

        self.add_member(7)
        published = json.loads(replicator.resume())
        self.assertEqual([item["player_id"] for item in published["members"]], [7])
        self.assertEqual(len(authority.commits), 1)

    def test_remote_link_initial_sync_resumes_pending_publish(self):
        from network import remote_link

        authority = FakeAuthority()
        replicator = replicated_roster.RosterReplicator(
            authority, self.credential, paused=True
        )
        replicated_roster.activate(replicator)
        self.add_member(7)
        self.assertIsNone(replicator.publish())

        with patch.object(remote_link, "_share_servers"), patch.object(
            remote_link, "_sync_players"
        ), patch.object(remote_link, "sync_subscriptions"):
            remote_link._on_new_server("peer", ("127.0.0.1", 11000))

        self.assertEqual(len(authority.commits), 1)
        self.assertEqual(
            [item["player_id"] for item in json.loads(authority.commits[0])["members"]],
            [7],
        )

    def test_active_replicator_lifecycle(self):
        authority = FakeAuthority()
        replicator = replicated_roster.RosterReplicator(authority, self.credential)
        replicated_roster.activate(replicator)
        self.assertIs(replicated_roster.active(), replicator)
        replicated_roster.deactivate(replicator)
        self.assertIsNone(replicated_roster.active())


if __name__ == "__main__":
    unittest.main()
