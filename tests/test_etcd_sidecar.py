"""Lifecycle and resource-baseline tests using only fake processes and clocks."""

import ctypes
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from server.etcd_sidecar import (
    EtcdProcessExited,
    EtcdSidecar,
    EtcdStartError,
)
from tools import etcd_resource_baseline as baseline


class FakeClock:
    def __init__(self):
        self.value = 0.0
        self.sleeps = []

    def __call__(self):
        return self.value

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.value += seconds


class FakeProcess:
    def __init__(self, pid=4321, *, returncode=None, ignore_terminate=False,
                 terminate_error=None):
        self.pid = pid
        self.returncode = returncode
        self.ignore_terminate = ignore_terminate
        self.terminate_error = terminate_error
        self.terminate_calls = 0
        self.kill_calls = 0
        self.wait_calls = []

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminate_calls += 1
        if self.terminate_error is not None:
            raise self.terminate_error
        if not self.ignore_terminate:
            self.returncode = 0

    def kill(self):
        self.kill_calls += 1
        self.returncode = -9

    def wait(self, timeout=None):
        self.wait_calls.append(timeout)
        if self.returncode is None:
            raise subprocess.TimeoutExpired("fake-etcd", timeout)
        return self.returncode


class ProcessRecorder:
    def __init__(self, process=None, error=None):
        self.process = process
        self.error = error
        self.calls = []

    def __call__(self, command, **kwargs):
        self.calls.append((command, kwargs))
        if self.error is not None:
            raise self.error
        return self.process


def make_sidecar(tmp_path, process=None, **overrides):
    executable = tmp_path / "etcd.exe"
    executable.write_bytes(b"not executed")
    process = process or FakeProcess()
    recorder = ProcessRecorder(process, error=overrides.pop("error", None))
    clock = FakeClock()
    options = {
        "name": "etcd1",
        "data_dir": tmp_path / "data",
        "process_factory": recorder,
        "health_prober": lambda url, timeout: True,
        "clock": clock,
        "sleeper": clock.sleep,
    }
    options.update(overrides)
    return EtcdSidecar(executable, **options), process, recorder, clock


def test_single_node_start_builds_explicit_command(tmp_path):
    sidecar, process, recorder, _ = make_sidecar(tmp_path)
    assert sidecar.start() is sidecar
    command, kwargs = recorder.calls[0]
    assert command[0] == str((tmp_path / "etcd.exe").resolve())
    assert "--name" in command
    assert command[command.index("--name") + 1] == "etcd1"
    assert command[command.index("--initial-cluster") + 1] == (
        "etcd1=http://127.0.0.1:2380"
    )
    assert kwargs["stdin"] == -3
    assert kwargs["cwd"] == str(tmp_path.resolve())
    assert sidecar.state == "RUNNING"
    assert sidecar.pid == process.pid
    assert sidecar.data_dir.is_dir()
    sidecar.stop()


def test_three_node_cluster_arguments_are_preserved(tmp_path):
    cluster = (
        "etcd1=http://127.0.0.1:2380,etcd2=http://127.0.0.1:2381,"
        "etcd3=http://127.0.0.1:2382"
    )
    sidecar, _, recorder, _ = make_sidecar(
        tmp_path,
        name="etcd2",
        client_url="http://127.0.0.1:2381",
        peer_url="http://127.0.0.1:2381",
        initial_cluster=cluster,
        initial_cluster_token="test-token",
    )
    # Peer and client ports may coincide only in artificial test configurations.
    sidecar.start()
    command, _ = recorder.calls[0]
    assert command[command.index("--initial-cluster") + 1] == cluster
    assert command[command.index("--initial-cluster-token") + 1] == "test-token"
    sidecar.stop()


def test_missing_executable_fails_without_spawn_or_data_directory(tmp_path):
    sidecar, _, recorder, _ = make_sidecar(tmp_path)
    sidecar.executable_path = tmp_path / "missing-etcd.exe"
    with pytest.raises(FileNotFoundError, match="explicit executable path"):
        sidecar.start()
    assert recorder.calls == []
    assert not sidecar.data_dir.exists()
    assert sidecar.state == "FAILED"


def test_spawn_failure_is_reported_and_log_is_closed(tmp_path):
    log_path = tmp_path / "etcd.log"
    sidecar, _, recorder, _ = make_sidecar(
        tmp_path, log_path=log_path, error=OSError("access denied")
    )
    with pytest.raises(EtcdStartError, match="access denied"):
        sidecar.start()
    assert recorder.calls
    assert sidecar.process is None
    assert sidecar.state == "FAILED"
    assert sidecar._log_handle is None


def test_health_timeout_cleans_up_partial_start(tmp_path):
    clock = FakeClock()
    process = FakeProcess()
    sidecar, _, _, _ = make_sidecar(
        tmp_path,
        process=process,
        startup_timeout=0.25,
        poll_interval=0.1,
        health_prober=lambda url, timeout: False,
        clock=clock,
        sleeper=clock.sleep,
    )
    with pytest.raises(EtcdStartError, match="did not become healthy"):
        sidecar.start()
    assert process.terminate_calls == 1
    assert process.wait_calls == [10.0]
    assert sidecar.process is None
    assert sidecar.state == "FAILED"


def test_start_waits_for_health_with_fake_clock(tmp_path):
    clock = FakeClock()
    responses = iter((False, False, True))
    sidecar, _, _, _ = make_sidecar(
        tmp_path,
        health_prober=lambda url, timeout: next(responses),
        clock=clock,
        sleeper=clock.sleep,
        startup_timeout=1.0,
        poll_interval=0.1,
    )
    sidecar.start()
    assert clock.value == pytest.approx(0.2)
    assert sidecar.state == "RUNNING"
    sidecar.stop()


def test_start_can_spawn_without_waiting_for_health(tmp_path):
    health_calls = []
    sidecar, process, recorder, _ = make_sidecar(
        tmp_path,
        health_prober=lambda url, timeout: health_calls.append(url) or True,
    )
    assert sidecar.start(wait_for_health=False) is sidecar
    assert sidecar.state == "STARTING"
    assert sidecar.is_running()
    assert health_calls == []
    assert sidecar.wait_until_healthy(timeout=1.0)
    assert sidecar.state == "RUNNING"
    assert health_calls == [sidecar.health_url]
    assert len(recorder.calls) == 1
    sidecar.stop()
    assert process.terminate_calls == 1


def test_unexpected_exit_is_detected(tmp_path):
    process = FakeProcess()
    sidecar, _, _, _ = make_sidecar(tmp_path, process=process)
    sidecar.start()
    process.returncode = 7
    with pytest.raises(EtcdProcessExited) as caught:
        sidecar.check_health()
    assert caught.value.returncode == 7
    assert sidecar.state == "FAILED"
    sidecar.stop()
    assert process.wait_calls == [None]


def test_stop_is_idempotent_and_only_signals_owned_process(tmp_path):
    process = FakeProcess()
    sidecar, _, _, _ = make_sidecar(tmp_path, process=process)
    sidecar.start()
    sidecar.stop()
    sidecar.stop()
    assert process.terminate_calls == 1
    assert process.kill_calls == 0
    assert process.wait_calls == [10.0]
    assert sidecar.state == "STOPPED"


def test_stop_escalates_to_kill_after_timeout(tmp_path):
    process = FakeProcess(ignore_terminate=True)
    sidecar, _, _, _ = make_sidecar(tmp_path, process=process)
    sidecar.start()
    sidecar.stop()
    assert process.terminate_calls == 1
    assert process.kill_calls == 1
    assert process.wait_calls == [10.0, None]


def test_terminate_error_still_reaps_via_kill(tmp_path):
    process = FakeProcess(terminate_error=PermissionError("denied"))
    sidecar, _, _, _ = make_sidecar(tmp_path, process=process)
    sidecar.start()
    sidecar.stop()
    assert process.terminate_calls == 1
    assert process.kill_calls == 1
    assert process.wait_calls == [10.0, None]


def test_windows_uses_create_no_window(monkeypatch, tmp_path):
    sidecar, _, recorder, _ = make_sidecar(tmp_path)
    monkeypatch.setattr("server.etcd_sidecar.sys.platform", "win32")
    sidecar.start()
    _, kwargs = recorder.calls[0]
    assert kwargs["creationflags"] == 0x08000000
    sidecar.stop()


def test_context_manager_stops_process_on_exception(tmp_path):
    process = FakeProcess()
    sidecar, _, _, _ = make_sidecar(tmp_path, process=process)
    with pytest.raises(LookupError):
        with sidecar:
            raise LookupError("body failed")
    assert process.terminate_calls == 1
    assert sidecar.process is None


class FakeKernel32:
    def __init__(self):
        self.closed = []
        self.opened = []

    def OpenProcess(self, access, inherit, pid):
        self.opened.append((access, inherit, pid))
        return 99

    def CloseHandle(self, handle):
        self.closed.append(handle)
        return 1

    def GetProcessTimes(self, handle, creation, exit_time, kernel, user):
        for value in (creation, exit_time):
            value._obj.dwLowDateTime = 1
        kernel._obj.dwLowDateTime = 2_000_000
        user._obj.dwLowDateTime = 1_000_000
        return 1


class FakePsapi:
    def __init__(self):
        self.memory = _memory_info(12_000_000)

    def GetProcessMemoryInfo(self, handle, memory, size):
        ctypes.memmove(memory, ctypes.byref(self.memory), size)
        return 1


def _memory_info(working_set):
    memory = baseline._PROCESS_MEMORY_COUNTERS_EX()
    memory.cb = ctypes.sizeof(memory)
    memory.WorkingSetSize = working_set
    return memory


def test_windows_resource_sampling_uses_cpu_delta_and_working_set():
    clock = FakeClock()
    kernel32 = FakeKernel32()
    psapi = FakePsapi()
    sampler = baseline.WindowsProcessSampler(
        clock=clock, processor_count=4, kernel32=kernel32, psapi=psapi
    )
    assert sampler.now == 0.0
    first = sampler.sample(4321)
    clock.value = 1.0
    kernel32.GetProcessTimes = lambda *args: _set_times(args[-1], args[-2], 13_000_000)
    psapi.memory = _memory_info(20_000_000)
    second = sampler.sample(4321)
    assert first["cpu_percent"] is None
    assert first["working_set_bytes"] == 12_000_000
    assert second["cpu_percent"] == pytest.approx(25.0)
    assert second["working_set_bytes"] == 20_000_000
    assert kernel32.opened == [(0x0410, False, 4321), (0x0410, False, 4321)]
    assert kernel32.closed == [99, 99]


def _set_times(user, kernel, user_ticks):
    for value in (kernel, user):
        value._obj.dwLowDateTime = 0
        value._obj.dwHighDateTime = 0
    kernel._obj.dwLowDateTime = 0
    user._obj.dwLowDateTime = user_ticks
    return 1


def test_data_directory_and_sidecar_sampling(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "a.bin").write_bytes(b"1234")
    (data_dir / "b.bin").write_bytes(b"123")
    sidecar = SimpleNamespace(
        name="etcd1", pid=4321, started_at=2.0, data_dir=data_dir,
        is_running=lambda: True,
    )
    clock = FakeClock()
    clock.value = 7.0
    sampler = SimpleNamespace(
        now=clock.value,
        sample=lambda pid: {"pid": pid, "cpu_percent": 1.5, "working_set_bytes": 100},
    )
    result = baseline.sample_sidecar(sidecar, sampler)
    assert result["data_directory_bytes"] == 7
    assert result["data_directory_files"] == 2
    assert result["uptime_seconds"] == 5.0
    assert result["disk_total_bytes"] > 0


def test_summary_ignores_initial_null_cpu_sample():
    samples = [
        {"cpu_percent": None, "working_set_bytes": 10, "data_directory_bytes": 1,
         "uptime_seconds": 1},
        {"cpu_percent": 20.0, "working_set_bytes": 30, "data_directory_bytes": 2,
         "uptime_seconds": 2},
        {"cpu_percent": 40.0, "working_set_bytes": 20, "data_directory_bytes": 3,
         "uptime_seconds": 3},
    ]
    summary = baseline._summary(samples)
    assert summary["average_cpu_percent"] == 30.0
    assert summary["max_cpu_percent"] == 40.0
    assert summary["max_working_set_bytes"] == 30
    assert summary["latest_uptime_seconds"] == 3


class FakeSidecar:
    def __init__(self, config, clock):
        self.name = config["name"]
        self.data_dir = config["data_dir"]
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.started_at = clock.value
        self.stopped = False

    @property
    def pid(self):
        return 1000 + int(self.name[-1])

    def is_running(self):
        return not self.stopped

    def start(self, *, wait_for_health=True):
        self.wait_for_health = wait_for_health
        return self

    def wait_until_healthy(self, timeout=None):
        self.health_timeout = timeout
        return True

    def stop(self):
        self.stopped = True


class FakeSampler:
    def __init__(self, clock):
        self.clock = clock

    @property
    def now(self):
        return self.clock.value

    def sample(self, pid):
        return {"pid": pid, "cpu_percent": 2.0, "working_set_bytes": 1024}


def baseline_args(tmp_path, mode="single", duration=2.0):
    executable = tmp_path / "etcd.exe"
    executable.write_bytes(b"fake")
    return SimpleNamespace(
        executable=str(executable), data_dir=str(tmp_path / "run"), mode=mode,
        client_base_port=12379, peer_base_port=13380, duration=duration,
        interval=1.0, startup_timeout=1.0,
    )


def test_collect_single_baseline_and_cleanup(tmp_path):
    args = baseline_args(tmp_path)
    clock = FakeClock()
    created = []

    def factory(executable, **config):
        sidecar = FakeSidecar(config, clock)
        created.append(sidecar)
        return sidecar

    report = baseline.collect_baseline(
        args, sidecar_factory=factory,
        sampler_factory=lambda: FakeSampler(clock),
        clock=clock, sleeper=clock.sleep,
    )
    assert report["ok"] is True, report["error"]
    assert len(report["processes"]) == 1
    assert len(report["processes"][0]["samples"]) == 3
    assert report["scope_notice"] == baseline.SCOPE_SINGLE
    assert created[0].stopped is True


def test_collect_three_process_baseline_scope_and_cleanup(tmp_path):
    args = baseline_args(tmp_path, mode="cluster", duration=0.0)
    clock = FakeClock()
    created = []
    events = []

    class OrderedSidecar(FakeSidecar):
        def start(self, *, wait_for_health=True):
            events.append(("spawn", self.name, wait_for_health))
            return super().start(wait_for_health=wait_for_health)

        def wait_until_healthy(self, timeout=None):
            events.append(("health", self.name, timeout))
            return super().wait_until_healthy(timeout=timeout)

    def factory(executable, **config):
        sidecar = OrderedSidecar(config, clock)
        created.append(sidecar)
        return sidecar

    report = baseline.collect_baseline(
        args, sidecar_factory=factory,
        sampler_factory=lambda: FakeSampler(clock),
        clock=clock, sleeper=clock.sleep,
    )
    assert report["ok"] is True, report["error"]
    assert len(report["processes"]) == 3
    assert all(sidecar.stopped for sidecar in created)
    assert [event[0] for event in events] == ["spawn"] * 3 + ["health"] * 3
    assert all(event[2] is False for event in events[:3])
    assert "单机3进程" in report["scope_notice"]
    assert "不代表公网容量" in report["scope_notice"]


def test_cluster_rejects_overlapping_client_and_peer_ports(tmp_path):
    args = baseline_args(tmp_path, mode="cluster", duration=0.0)
    args.peer_base_port = args.client_base_port + 1
    created = []

    def factory(executable, **config):
        created.append(config)
        raise AssertionError("overlapping ports must fail before process creation")

    report = baseline.collect_baseline(
        args, sidecar_factory=factory,
        sampler_factory=lambda: FakeSampler(FakeClock()),
    )
    assert report["ok"] is False
    assert report["error"]["type"] == "ValueError"
    assert "must not overlap" in report["error"]["message"]
    assert created == []
    assert not Path(args.data_dir).exists()


def test_collect_start_failure_cleans_up_prior_nodes(tmp_path):
    args = baseline_args(tmp_path, mode="cluster", duration=0.0)
    clock = FakeClock()
    created = []

    class FailingSidecar(FakeSidecar):
        def start(self, *, wait_for_health=True):
            if self.name == "etcd2":
                raise EtcdStartError("boom")
            return super().start(wait_for_health=wait_for_health)

    def factory(executable, **config):
        sidecar = FailingSidecar(config, clock)
        created.append(sidecar)
        return sidecar

    report = baseline.collect_baseline(
        args, sidecar_factory=factory,
        sampler_factory=lambda: FakeSampler(clock),
        clock=clock, sleeper=clock.sleep,
    )
    assert report["ok"] is False
    assert report["error"]["message"] == "boom"
    assert created[0].stopped is True


def test_missing_etcd_cli_returns_failure_json(tmp_path, capsys):
    args = [
        "--executable", str(tmp_path / "missing-etcd.exe"),
        "--data-dir", str(tmp_path / "data"), "--duration", "0",
    ]
    assert baseline.main(args) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["ok"] is False
    assert report["error"]["type"] == "FileNotFoundError"
    assert "不代表公网容量" in report["scope_notice"]
