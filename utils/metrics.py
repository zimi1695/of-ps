"""Process-local multiplayer metrics with bounded, label-free cardinality."""

from collections import Counter, deque
import math
from threading import RLock


class MetricsRegistry:
    def __init__(self):
        self._lock = RLock()
        self._counters = Counter()
        self._gauges = {}
        self._timers = {}
        self._samples = {}

    def inc(self, name, value=1):
        with self._lock:
            self._counters[name] += value

    def set(self, name, value):
        with self._lock:
            self._gauges[name] = value

    def observe(self, name, seconds):
        value = max(0.0, float(seconds))
        with self._lock:
            state = self._timers.setdefault(name, [0, 0.0, 0.0])
            state[0] += 1
            state[1] += value
            state[2] = max(state[2], value)
            self._samples.setdefault(name, deque(maxlen=2048)).append(value)

    def _percentile(self, name, fraction):
        values = sorted(self._samples.get(name, ()))
        if not values:
            return 0.0
        index = max(0, math.ceil(len(values) * fraction) - 1)
        return values[min(len(values) - 1, index)]

    def snapshot(self):
        with self._lock:
            return {
                "counters": dict(sorted(self._counters.items())),
                "gauges": dict(sorted(self._gauges.items())),
                "timers": {
                    name: {
                        "count": state[0],
                        "total_seconds": state[1],
                        "max_seconds": state[2],
                        "avg_seconds": state[1] / state[0] if state[0] else 0.0,
                        "p50_seconds": self._percentile(name, 0.50),
                        "p95_seconds": self._percentile(name, 0.95),
                        "p99_seconds": self._percentile(name, 0.99),
                    }
                    for name, state in sorted(self._timers.items())
                },
            }

    def reset(self):
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._timers.clear()
            self._samples.clear()


metrics = MetricsRegistry()
