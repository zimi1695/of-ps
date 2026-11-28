"""Measure a local one- or three-process etcd resource baseline as JSON.

Example:
    python tools/etcd_resource_baseline.py --executable C:/etcd/etcd.exe \
        --data-dir C:/tmp/ofps-etcd-baseline --mode single --duration 10

No etcd binary is downloaded.  Missing executables and sampling failures produce
an ``ok: false`` JSON document and a non-zero process exit status.
"""

import argparse
import ctypes
import json
import math
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from server.etcd_sidecar import EtcdSidecar

SCOPE_SINGLE = "单机单进程基线，不代表公网容量。"
SCOPE_CLUSTER = "单机3进程基线，不代表公网容量；未验证跨主机网络、故障域或生产负载。"


class ResourceSamplingError(RuntimeError):
    """A process resource sample could not be collected."""


class _FILETIME(ctypes.Structure):
    _fields_ = [("dwLowDateTime", ctypes.c_uint32), ("dwHighDateTime", ctypes.c_uint32)]


class _PROCESS_MEMORY_COUNTERS_EX(ctypes.Structure):
    _fields_ = [
        ("cb", ctypes.c_uint32),
        ("PageFaultCount", ctypes.c_uint32),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
        ("PrivateUsage", ctypes.c_size_t),
    ]


def _filetime_100ns(value):
    return (value.dwHighDateTime << 32) | value.dwLowDateTime


class WindowsProcessSampler:
    """Read process CPU and working set using Windows' native APIs.

    CPU is normalized by logical processor count.  The first sample establishes
    a CPU counter baseline and reports ``cpu_percent: null``; subsequent samples
    report usage since the previous sample.  Dependencies are injectable so the
    tests never need a real child process or a real etcd installation.
    """

    def __init__(self, *, clock=None, processor_count=None, kernel32=None, psapi=None):
        if sys.platform != "win32" and (kernel32 is None or psapi is None):
            raise ResourceSamplingError("process sampling currently requires Windows")
        self._clock = clock or time.monotonic
        self._processor_count = int(processor_count or (os.cpu_count() or 1))
        self._kernel32 = kernel32
        self._psapi = psapi
        self._previous = {}
        if kernel32 is None or psapi is None:
            self._configure_ctypes()

    def _configure_ctypes(self):
        if sys.platform != "win32":
            raise ResourceSamplingError("process sampling currently requires Windows")
        self._kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        self._psapi = ctypes.WinDLL("psapi", use_last_error=True)
        self._kernel32.OpenProcess.argtypes = [
            ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32
        ]
        self._kernel32.OpenProcess.restype = ctypes.c_void_p
        self._kernel32.GetProcessTimes.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(_FILETIME),
            ctypes.POINTER(_FILETIME),
            ctypes.POINTER(_FILETIME),
            ctypes.POINTER(_FILETIME),
        ]
        self._kernel32.GetProcessTimes.restype = ctypes.c_int
        self._kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        self._kernel32.CloseHandle.restype = ctypes.c_int
        self._psapi.GetProcessMemoryInfo.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(_PROCESS_MEMORY_COUNTERS_EX),
            ctypes.c_uint32,
        ]
        self._psapi.GetProcessMemoryInfo.restype = ctypes.c_int

    @property
    def now(self):
        return self._clock()

    def reset(self):
        self._previous.clear()

    def sample(self, pid):
        if sys.platform != "win32" and self._kernel32 is None:
            raise ResourceSamplingError("process sampling currently requires Windows")
        handle = self._kernel32.OpenProcess(0x0400 | 0x0010, False, int(pid))
        if not handle:
            raise ResourceSamplingError(
                f"OpenProcess failed for pid {pid} (Windows error {ctypes.get_last_error()})"
            )
        creation, exit_time, kernel_time, user_time = (
            _FILETIME(), _FILETIME(), _FILETIME(), _FILETIME()
        )
        memory = _PROCESS_MEMORY_COUNTERS_EX()
        memory.cb = ctypes.sizeof(memory)
        try:
            if not self._kernel32.GetProcessTimes(
                handle, ctypes.byref(creation), ctypes.byref(exit_time),
                ctypes.byref(kernel_time), ctypes.byref(user_time)
            ):
                raise ResourceSamplingError(
                    f"GetProcessTimes failed for pid {pid} "
                    f"(Windows error {ctypes.get_last_error()})"
                )
            if not self._psapi.GetProcessMemoryInfo(
                handle, ctypes.byref(memory), ctypes.sizeof(memory)
            ):
                raise ResourceSamplingError(
                    f"GetProcessMemoryInfo failed for pid {pid} "
                    f"(Windows error {ctypes.get_last_error()})"
                )
        finally:
            self._kernel32.CloseHandle(handle)

        now = self._clock()
        cpu_seconds = (_filetime_100ns(kernel_time) + _filetime_100ns(user_time)) / 10_000_000
        previous = self._previous.get(pid)
        cpu_percent = None
        if previous is not None:
            elapsed = now - previous[0]
            if elapsed <= 0:
                raise ResourceSamplingError("sampling clock did not advance")
            cpu_percent = max(0.0, (cpu_seconds - previous[1]) / elapsed * 100.0)
            cpu_percent /= self._processor_count
        self._previous[pid] = (now, cpu_seconds)
        return {
            "pid": int(pid),
            "cpu_percent": cpu_percent,
            "working_set_bytes": int(memory.WorkingSetSize),
        }


def measure_data_directory(data_dir):
    """Return etcd data-directory size plus host filesystem capacity."""
    data_dir = Path(data_dir)
    total_bytes = 0
    file_count = 0
    for root, dirs, files in os.walk(data_dir):
        dirs.sort()
        for filename in sorted(files):
            path = Path(root) / filename
            try:
                total_bytes += path.stat().st_size
                file_count += 1
            except FileNotFoundError:
                # WAL rotation can remove a file between walk and stat.
                continue
    usage = shutil.disk_usage(data_dir)
    return {
        "data_directory_bytes": total_bytes,
        "data_directory_files": file_count,
        "disk_total_bytes": int(usage.total),
        "disk_free_bytes": int(usage.free),
    }


def sample_sidecar(sidecar, sampler, *, started_at=None, data_dir=None):
    if not sidecar.is_running():
        raise ResourceSamplingError(f"etcd {sidecar.name!r} is not running")
    started_at = sidecar.started_at if started_at is None else started_at
    result = {"name": sidecar.name}
    result.update(sampler.sample(sidecar.pid))
    result["uptime_seconds"] = max(0.0, float(sampler.now - started_at))
    result.update(measure_data_directory(data_dir or sidecar.data_dir))
    return result


def _summary(samples):
    cpu_values = [row["cpu_percent"] for row in samples if row["cpu_percent"] is not None]
    working_sets = [row["working_set_bytes"] for row in samples]
    return {
        "samples": len(samples),
        "average_cpu_percent": sum(cpu_values) / len(cpu_values) if cpu_values else None,
        "max_cpu_percent": max(cpu_values) if cpu_values else None,
        "max_working_set_bytes": max(working_sets) if working_sets else None,
        "latest_working_set_bytes": working_sets[-1] if working_sets else None,
        "latest_data_directory_bytes": samples[-1]["data_directory_bytes"] if samples else None,
        "latest_uptime_seconds": samples[-1]["uptime_seconds"] if samples else None,
    }


def _cluster_config(mode, data_dir, client_base_port, peer_base_port):
    count = 1 if mode == "single" else 3
    names = [f"etcd{index}" for index in range(1, count + 1)]
    members = [
        f"{name}=http://127.0.0.1:{peer_base_port + index}"
        for index, name in enumerate(names)
    ]
    configs = []
    for index, name in enumerate(names):
        client_url = f"http://127.0.0.1:{client_base_port + index}"
        peer_url = f"http://127.0.0.1:{peer_base_port + index}"
        node_dir = data_dir / name
        configs.append({
            "name": name,
            "data_dir": node_dir,
            "client_url": client_url,
            "peer_url": peer_url,
            "initial_cluster": ",".join(members),
            "log_path": data_dir / "logs" / f"{name}.log",
        })
    return configs


def collect_baseline(args, *, sidecar_factory=EtcdSidecar, sampler_factory=WindowsProcessSampler,
                     clock=None, sleeper=None):
    """Run a baseline and return a JSON-serializable success or failure report."""
    clock = clock or time.monotonic
    sleeper = sleeper or time.sleep
    report = {
        "schema_version": 1,
        "ok": False,
        "mode": args.mode,
        "scope_notice": SCOPE_SINGLE if args.mode == "single" else SCOPE_CLUSTER,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": args.duration,
        "sample_interval_seconds": args.interval,
        "processes": [],
    }
    sidecars = []
    started_sidecars = []
    try:
        executable = Path(args.executable).expanduser().resolve()
        if not executable.is_file():
            raise FileNotFoundError(
                f"etcd executable not found: {executable}; configure --executable explicitly"
            )
        if (not all(math.isfinite(value) for value in
                    (args.duration, args.interval, args.startup_timeout))
                or args.duration < 0 or args.interval <= 0 or args.startup_timeout <= 0):
            raise ValueError(
                "duration must be finite and non-negative; interval and startup "
                "timeout must be finite and positive"
            )
        if not (1 <= args.client_base_port <= 65532
                and 1 <= args.peer_base_port <= 65532):
            raise ValueError("base ports must be between 1 and 65532")
        if args.mode == "cluster":
            client_ports = set(range(args.client_base_port, args.client_base_port + 3))
            peer_ports = set(range(args.peer_base_port, args.peer_base_port + 3))
            if client_ports & peer_ports:
                raise ValueError("cluster client and peer port ranges must not overlap")
        data_root = Path(args.data_dir).expanduser().resolve()
        data_root.mkdir(parents=True, exist_ok=True)
        configs = _cluster_config(
            args.mode, data_root, args.client_base_port, args.peer_base_port
        )
        report["processes"] = [
            {"name": config["name"], "client_url": config["client_url"], "samples": []}
            for config in configs
        ]
        sampler = sampler_factory()
        for config in configs:
            sidecar = sidecar_factory(
                executable,
                startup_timeout=args.startup_timeout,
                **config,
            )
            sidecars.append(sidecar)
            sidecar.start(wait_for_health=False)
            started_sidecars.append(sidecar)

        for sidecar in sidecars:
            sidecar.wait_until_healthy(timeout=args.startup_timeout)

        deadline = clock() + args.duration
        while True:
            for sidecar, process_report in zip(sidecars, report["processes"]):
                sample = sample_sidecar(sidecar, sampler)
                sample["uptime_seconds"] = max(0.0, clock() - sidecar.started_at)
                process_report["samples"].append(sample)
            remaining = deadline - clock()
            if remaining <= 0:
                break
            sleeper(min(args.interval, remaining))
        for process_report in report["processes"]:
            process_report["summary"] = _summary(process_report["samples"])
        report["ok"] = True
    except Exception as exc:
        report["error"] = {"type": type(exc).__name__, "message": str(exc)}
    finally:
        for sidecar in reversed(started_sidecars):
            try:
                sidecar.stop()
            except Exception as exc:
                report["ok"] = False
                report.setdefault("cleanup_errors", []).append(
                    {"name": sidecar.name, "type": type(exc).__name__, "message": str(exc)}
                )
    return report


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--executable", required=True, help="Explicit path to etcd executable")
    parser.add_argument("--data-dir", required=True, help="Empty baseline directory for node data")
    parser.add_argument("--mode", choices=("single", "cluster"), default="single")
    parser.add_argument("--client-base-port", type=int, default=2379)
    parser.add_argument("--peer-base-port", type=int, default=2480)
    parser.add_argument("--duration", type=float, default=10.0, help="Seconds to sample")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between samples")
    parser.add_argument("--startup-timeout", type=float, default=30.0)
    parser.add_argument("--output", type=Path, help="Write the JSON report as UTF-8")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    report = collect_baseline(args)
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
