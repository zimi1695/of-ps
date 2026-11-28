"""Explicit, dependency-free lifecycle management for a local etcd process.

This module deliberately does not search PATH, download etcd, or fall back to an
in-memory implementation.  Callers must supply the executable and endpoint
configuration they intend to use.
"""

import os
import math
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path


class EtcdSidecarError(RuntimeError):
    """Base error for etcd sidecar failures."""


class EtcdStartError(EtcdSidecarError):
    """The configured etcd process could not be started or became healthy."""


class EtcdProcessExited(EtcdSidecarError):
    """An etcd process owned by this sidecar exited unexpectedly."""

    def __init__(self, name, returncode):
        self.name = name
        self.returncode = returncode
        super().__init__(f"etcd process {name!r} exited with code {returncode}")


def _http_health(url, timeout):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return 200 <= response.status < 300
    except (OSError, ValueError):
        return False


class EtcdSidecar:
    """Own one etcd subprocess and clean up only that subprocess.

    ``initial_cluster`` defaults to a single member.  For a three-node cluster,
    pass the same ``initial_cluster`` string and token to all three sidecars,
    with each sidecar's own ``name``, ``client_url``, and ``peer_url``.
    """

    def __init__(
        self,
        executable_path,
        *,
        name="etcd1",
        data_dir,
        client_url="http://127.0.0.1:2379",
        peer_url="http://127.0.0.1:2380",
        initial_cluster=None,
        initial_cluster_token="ofps-etcd-cluster",
        initial_cluster_state="new",
        health_url=None,
        log_path=None,
        startup_timeout=30.0,
        poll_interval=0.1,
        stop_timeout=10.0,
        process_factory=None,
        health_prober=None,
        clock=None,
        sleeper=None,
    ):
        if not executable_path:
            raise ValueError("executable_path is required; etcd is never auto-discovered")
        if (not all(math.isfinite(value) for value in
                    (startup_timeout, poll_interval, stop_timeout))
                or startup_timeout <= 0 or poll_interval <= 0 or stop_timeout <= 0):
            raise ValueError("timeouts and poll_interval must be finite and positive")
        self.executable_path = Path(executable_path).expanduser().resolve()
        self.name = name
        self.data_dir = Path(data_dir).expanduser().resolve()
        self.client_url = client_url.rstrip("/")
        self.peer_url = peer_url.rstrip("/")
        self.initial_cluster = initial_cluster or f"{name}={self.peer_url}"
        self.initial_cluster_token = initial_cluster_token
        self.initial_cluster_state = initial_cluster_state
        self.health_url = health_url or f"{self.client_url}/health"
        self.log_path = Path(log_path).expanduser().resolve() if log_path else None
        self.startup_timeout = float(startup_timeout)
        self.poll_interval = float(poll_interval)
        self.stop_timeout = float(stop_timeout)
        self._process_factory = process_factory or subprocess.Popen
        self._health_prober = health_prober or _http_health
        self._clock = clock or time.monotonic
        self._sleeper = sleeper or time.sleep
        self._lock = threading.RLock()
        self._process = None
        self._log_handle = None
        self._started_at = None
        self.state = "NEW"

    @property
    def process(self):
        return self._process

    @property
    def started_at(self):
        return self._started_at

    @property
    def pid(self):
        process = self._process
        return process.pid if process is not None else None

    def build_command(self):
        """Return the argv for either a single member or a configured cluster."""
        return [
            str(self.executable_path),
            "--name",
            self.name,
            "--data-dir",
            str(self.data_dir),
            "--listen-client-urls",
            self.client_url,
            "--advertise-client-urls",
            self.client_url,
            "--listen-peer-urls",
            self.peer_url,
            "--initial-advertise-peer-urls",
            self.peer_url,
            "--initial-cluster",
            self.initial_cluster,
            "--initial-cluster-token",
            self.initial_cluster_token,
            "--initial-cluster-state",
            self.initial_cluster_state,
        ]

    def start(self, timeout=None, *, wait_for_health=True):
        """Start and optionally wait for health, cleaning partial starts on failure.

        Cluster callers can spawn every member with ``wait_for_health=False``
        before waiting for health, allowing the initial cluster to form a quorum.
        """
        with self._lock:
            if self.is_running():
                return self
            timeout = self.startup_timeout if timeout is None else float(timeout)
            if not math.isfinite(timeout) or timeout <= 0:
                raise ValueError("timeout must be finite and positive")
            if not self.executable_path.is_file():
                self.state = "FAILED"
                raise FileNotFoundError(
                    f"etcd executable not found: {self.executable_path}; "
                    "configure an explicit executable path"
                )

            self.data_dir.mkdir(parents=True, exist_ok=True)
            if self.log_path is not None:
                self.log_path.parent.mkdir(parents=True, exist_ok=True)
                self._log_handle = open(self.log_path, "ab")
            kwargs = {
                "cwd": str(self.executable_path.parent),
                "stdin": subprocess.DEVNULL,
                "stdout": self._log_handle if self._log_handle is not None else subprocess.DEVNULL,
                "stderr": subprocess.STDOUT,
            }
            if sys.platform == "win32":
                kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)

            self.state = "STARTING"
            try:
                self._process = self._process_factory(self.build_command(), **kwargs)
                self._started_at = self._clock()
                if wait_for_health:
                    self.wait_until_healthy(timeout=timeout)
                    self.state = "RUNNING"
                return self
            except BaseException as exc:
                self.stop()
                if isinstance(exc, EtcdSidecarError):
                    self.state = "FAILED"
                    raise
                self.state = "FAILED"
                if isinstance(exc, FileNotFoundError):
                    raise
                raise EtcdStartError(
                    f"failed to start etcd {self.name!r}: {exc}"
                ) from exc

    def is_running(self):
        process = self._process
        return process is not None and process.poll() is None

    def check_health(self):
        """Return HTTP health; raise if the owned process has already exited."""
        with self._lock:
            if self._process is None:
                return False
            returncode = self._process.poll()
            if returncode is not None:
                self.state = "FAILED"
                raise EtcdProcessExited(self.name, returncode)
            return bool(self._health_prober(self.health_url, timeout=1.0))

    def wait_until_healthy(self, timeout=None):
        timeout = self.startup_timeout if timeout is None else float(timeout)
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        deadline = self._clock() + timeout
        while True:
            if self.check_health():
                self.state = "RUNNING"
                return True
            remaining = deadline - self._clock()
            if remaining <= 0:
                self.state = "FAILED"
                raise TimeoutError(
                    f"etcd {self.name!r} did not become healthy within {timeout:g}s"
                )
            self._sleeper(min(self.poll_interval, remaining))

    def stop(self, timeout=None):
        """Terminate, then kill if needed, and reap the owned process.

        Safe to call repeatedly, including after failed startup or an abnormal
        exit.  No attempt is made to discover or signal an unowned etcd process.
        """
        timeout = self.stop_timeout if timeout is None else float(timeout)
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        with self._lock:
            process, self._process = self._process, None
            handle, self._log_handle = self._log_handle, None
            try:
                if process is not None:
                    if process.poll() is None:
                        try:
                            process.terminate()
                        except ProcessLookupError:
                            pass
                        except OSError:
                            # Some processes deny terminate but still permit kill.
                            # The bounded wait below gives them time to exit first.
                            pass
                        try:
                            process.wait(timeout=timeout)
                        except subprocess.TimeoutExpired:
                            try:
                                process.kill()
                            except ProcessLookupError:
                                pass
                            process.wait()
                    else:
                        process.wait()
            finally:
                if handle is not None:
                    handle.close()
                self.state = "STOPPED"

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
        return False
