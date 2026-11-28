"""Accept channel authority recovery against three real local etcd processes."""

import argparse
from dataclasses import asdict
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime, timezone

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from server.entity_authority import EntityAuthority, OwnershipError, entity_domain
from server.entity_store import EntityOwnershipStore
from server.etcd_authority import (
    AuthorityCredential,
    AuthorityError,
    ChannelAuthority,
    EtcdClient,
)
from server.etcd_sidecar import EtcdSidecar
from server.replicated_events import ReplicatedEventLog
from tools.etcd_resource_baseline import _cluster_config

CHANNEL_ID = "1"
CONFIG_GENERATION = 1
SCOPE_NOTICE = (
    "单机三进程真实 etcd 验收；强杀频道主机子进程和一个 etcd 成员，"
    "不代表跨主机故障域、公网延迟或生产负载。"
)


def _public_bytes(private_key):
    return private_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)


def _host_child(endpoint, output_path, host_id, lease_seconds):
    authority = ChannelAuthority(EtcdClient(endpoint), lease_seconds=lease_seconds)
    credential = authority.acquire(CHANNEL_ID, host_id, CONFIG_GENERATION)
    credential = authority.commit_noop(credential)
    Path(output_path).write_text(json.dumps(asdict(credential)), encoding="utf-8")
    while True:
        time.sleep(1)


def _spawn_host_child(endpoint, output_path, host_id, lease_seconds):
    kwargs = {
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.PIPE,
        "text": True,
    }
    if sys.platform == "win32":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
    return subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--host-child",
            endpoint,
            str(output_path),
            host_id,
            str(lease_seconds),
        ],
        **kwargs,
    )


def _read_child_credential(process, output_path, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if output_path.is_file():
            try:
                return AuthorityCredential(**json.loads(output_path.read_text(encoding="utf-8")))
            except (ValueError, TypeError, json.JSONDecodeError):
                pass
        if process.poll() is not None:
            stderr = process.stderr.read() if process.stderr is not None else ""
            raise RuntimeError(f"channel host child exited early: {stderr.strip()}")
        time.sleep(0.05)
    raise TimeoutError("channel host child did not publish its credential")


def _expect_rejection(action):
    try:
        action()
    except (AuthorityError, OwnershipError) as error:
        return type(error).__name__
    raise AssertionError("stale authority operation was accepted")


def _wait_for_authority_expiry(client, key, timeout):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            if client.range(key) is None:
                return
        except AuthorityError as error:
            last_error = str(error)
        time.sleep(0.1)
    suffix = f"; last error: {last_error}" if last_error else ""
    raise TimeoutError(f"channel authority lease did not expire{suffix}")


def _etcd_version(executable):
    result = subprocess.run(
        [str(executable), "--version"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    output = (result.stdout + result.stderr).strip()
    if result.returncode != 0 or not output:
        raise RuntimeError("could not read etcd version")
    return output


def run_acceptance(args):
    report = {
        "schema_version": 1,
        "ok": False,
        "scope_notice": SCOPE_NOTICE,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "channel_id": int(CHANNEL_ID),
        "config_generation": CONFIG_GENERATION,
        "lease_seconds": args.lease_seconds,
        "checks": {},
    }
    sidecars = []
    started_sidecars = []
    host_child = None
    try:
        executable = Path(args.executable).expanduser().resolve()
        if not executable.is_file():
            raise FileNotFoundError(f"etcd executable not found: {executable}")
        if args.lease_seconds <= 0 or args.failover_timeout <= 0:
            raise ValueError("lease_seconds and failover_timeout must be positive")
        report["etcd_version"] = _etcd_version(executable)
        data_root = Path(args.data_dir).expanduser().resolve()
        data_root.mkdir(parents=True, exist_ok=True)
        configs = _cluster_config(
            "cluster", data_root, args.client_base_port, args.peer_base_port
        )
        client_ports = set(range(args.client_base_port, args.client_base_port + 3))
        peer_ports = set(range(args.peer_base_port, args.peer_base_port + 3))
        if client_ports & peer_ports:
            raise ValueError("cluster client and peer port ranges must not overlap")
        report["processes"] = [
            {
                "name": config["name"],
                "client_url": config["client_url"],
                "peer_url": config["peer_url"],
            }
            for config in configs
        ]
        for config in configs:
            sidecar = EtcdSidecar(
                executable,
                startup_timeout=args.startup_timeout,
                **config,
            )
            sidecars.append(sidecar)
            sidecar.start(wait_for_health=False)
            started_sidecars.append(sidecar)
        for sidecar in sidecars:
            sidecar.wait_until_healthy(timeout=args.startup_timeout)
        for process_report, sidecar in zip(report["processes"], sidecars):
            process_report["pid"] = sidecar.pid
        report["checks"]["three_members_healthy"] = True

        credential_path = data_root / "host-a-credential.json"
        host_child = _spawn_host_child(
            configs[0]["client_url"], credential_path, "host-a", args.lease_seconds
        )
        first = _read_child_credential(host_child, credential_path, args.startup_timeout)
        first_authority = ChannelAuthority(
            EtcdClient(configs[0]["client_url"]), lease_seconds=args.lease_seconds
        )
        first_authority.verify(
            CHANNEL_ID, first.host_id, first.app_term, first.config_generation
        )
        report["initial_authority"] = {
            "host_id": first.host_id,
            "app_term": first.app_term,
            "lease_id": first.lease_id,
            "key_version": first.key_version,
            "key_revision": first.key_revision,
        }
        report["checks"]["initial_credential_verified"] = True

        roster = json.dumps(
            {
                "channel_id": 1,
                "authority_epoch": first.app_term,
                "roster_seq": 7,
                "members": [101, 102, 103],
            },
            separators=(",", ":"),
        ).encode()
        snapshot = json.dumps(
            {
                "channel_id": 1,
                "authority_epoch": first.app_term,
                "players": [{"player_id": 101, "x": 30, "y": 22}],
            },
            separators=(",", ":"),
        ).encode()
        first_authority.commit_roster(first, roster)
        first_authority.commit_snapshot(first, snapshot)
        first_log = ReplicatedEventLog(first_authority, first)
        event_args = (101, 3, 1, "chat", 9)
        original_response = first_log.append(
            *event_args, b"hello", b"accepted"
        )
        report["checks"]["persistent_state_committed"] = True

        key_a = Ed25519PrivateKey.generate()
        store = EntityOwnershipStore(first_authority.client, CHANNEL_ID)
        entity_a = EntityAuthority("host-a", key_a)
        entity_a.configure_store(store, lambda: first)
        domain = entity_domain("gather", 1, 1)
        owned_before = entity_a.assert_owner(domain, 55, require_shared=True)
        report["entity_before_failover"] = {
            "owner_id": owned_before.owner_id,
            "authority_epoch": owned_before.authority_epoch,
            "owner_generation": owned_before.owner_generation,
            "transfer_seq": owned_before.sequence,
        }
        report["checks"]["entity_claim_committed"] = True

        killed_at = time.monotonic()
        host_child.kill()
        host_child.wait(timeout=5)
        report["killed_channel_host"] = {
            "host_id": "host-a",
            "pid": host_child.pid,
            "returncode": host_child.returncode,
        }
        killed_etcd = sidecars[0]
        killed_etcd.process.kill()
        killed_etcd.process.wait(timeout=5)
        killed_etcd.state = "FAILED"
        report["killed_etcd_member"] = {
            "name": killed_etcd.name,
            "pid": killed_etcd.pid,
            "returncode": killed_etcd.process.returncode,
        }
        report["checks"]["channel_host_and_etcd_member_killed"] = True

        surviving_client = EtcdClient(configs[1]["client_url"])
        surviving_store = EntityOwnershipStore(surviving_client, CHANNEL_ID)
        _wait_for_authority_expiry(surviving_client, first.key, args.failover_timeout)
        second_authority = ChannelAuthority(
            surviving_client, lease_seconds=args.lease_seconds
        )
        second = second_authority.acquire(CHANNEL_ID, "host-b", CONFIG_GENERATION)
        second = second_authority.commit_noop(second)
        failover_seconds = time.monotonic() - killed_at
        second_authority.verify(
            CHANNEL_ID, second.host_id, second.app_term, second.config_generation
        )
        if second.app_term <= first.app_term:
            raise AssertionError("new authority term did not increase")
        report["recovered_authority"] = {
            "host_id": second.host_id,
            "app_term": second.app_term,
            "lease_id": second.lease_id,
            "key_version": second.key_version,
            "key_revision": second.key_revision,
            "failover_seconds": failover_seconds,
        }
        report["checks"]["new_credential_verified"] = True

        if second_authority.load_roster(CHANNEL_ID) != roster:
            raise AssertionError("roster did not survive failover")
        if second_authority.load_snapshot(CHANNEL_ID) != snapshot:
            raise AssertionError("snapshot did not survive failover")
        second_log = ReplicatedEventLog(second_authority, second)
        if second_log.recover_response(*event_args) != original_response:
            raise AssertionError("event response did not survive failover")
        replay_response, committed = second_log.append_once(
            *event_args, b"different", b"different"
        )
        if committed or replay_response != original_response:
            raise AssertionError("event replay did not return the original response")
        report["checks"]["roster_snapshot_event_recovered"] = True

        stale_authority = ChannelAuthority(
            surviving_client, lease_seconds=args.lease_seconds
        )
        stale_log = ReplicatedEventLog(stale_authority, first)
        stale_entity_a = EntityAuthority("host-a", key_a)
        stale_entity_a.configure_store(surviving_store, lambda: first)
        report["stale_rejections"] = {
            "commit_state": _expect_rejection(
                lambda: stale_authority.commit_state(
                    first, "channels/1/stale-state", b"stale"
                )
            ),
            "commit_roster": _expect_rejection(
                lambda: stale_authority.commit_roster(first, b"stale")
            ),
            "commit_snapshot": _expect_rejection(
                lambda: stale_authority.commit_snapshot(first, b"stale")
            ),
            "commit_event": _expect_rejection(
                lambda: stale_log.append(*event_args, b"stale", b"stale")
            ),
            "entity_owner": _expect_rejection(
                lambda: stale_entity_a.assert_owner(domain, 55, require_shared=True)
            ),
        }
        report["checks"]["stale_credential_rejected"] = True

        key_b = Ed25519PrivateKey.generate()
        entity_b = EntityAuthority("host-b", key_b)
        entity_b.configure_store(surviving_store, lambda: second)
        owned_after = entity_b.assert_owner(domain, 55, require_shared=True)
        stored_after = surviving_store.load(domain, 55)
        if (
            owned_after.owner_id != "host-b"
            or owned_after.authority_epoch != second.app_term
            or owned_after.owner_generation <= owned_before.owner_generation
            or stored_after["transfer"].signer_public_key != _public_bytes(key_b)
        ):
            raise AssertionError("new leader did not take over entity ownership")
        report["entity_after_failover"] = {
            "owner_id": owned_after.owner_id,
            "authority_epoch": owned_after.authority_epoch,
            "owner_generation": owned_after.owner_generation,
            "transfer_seq": owned_after.sequence,
        }
        report["checks"]["entity_ownership_recovered"] = True

        second_authority.commit_roster(
            second, roster.replace(b'"roster_seq":7', b'"roster_seq":8')
        )
        report["checks"]["new_leader_can_publish"] = True
        report["ok"] = all(report["checks"].values())
    except Exception as error:
        report["error"] = {"type": type(error).__name__, "message": str(error)}
    finally:
        if host_child is not None and host_child.poll() is None:
            host_child.kill()
            host_child.wait(timeout=5)
        for sidecar in reversed(started_sidecars):
            try:
                sidecar.stop()
            except Exception as error:
                report["ok"] = False
                report.setdefault("cleanup_errors", []).append(
                    {
                        "name": sidecar.name,
                        "type": type(error).__name__,
                        "message": str(error),
                    }
                )
        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    return report


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--executable", required=True)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--client-base-port", type=int, default=24779)
    parser.add_argument("--peer-base-port", type=int, default=25780)
    parser.add_argument("--lease-seconds", type=int, default=3)
    parser.add_argument("--startup-timeout", type=float, default=30.0)
    parser.add_argument("--failover-timeout", type=float, default=20.0)
    parser.add_argument("--output", type=Path)
    return parser


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "--host-child":
        if len(argv) != 5:
            raise SystemExit("invalid host child arguments")
        _host_child(argv[1], argv[2], argv[3], int(argv[4]))
        return 0
    args = build_parser().parse_args(argv)
    report = run_acceptance(args)
    text = json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    print(text, end="")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
