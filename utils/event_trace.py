"""Request-scoped timing metadata shared by packet and broadcast paths."""

from contextvars import ContextVar
from dataclasses import dataclass
import itertools
import os
import time

from utils import metrics as metrics_registry

_process_prefix = (int(time.time_ns()) ^ (os.getpid() << 32)) & ((1 << 64) - 1)
_counter = itertools.count(1)
_current = ContextVar("ofps_event_trace", default=None)


@dataclass(frozen=True)
class TraceContext:
    trace_id: int
    produced_at_us: int


def new_trace(trace_id=0, produced_at_us=0):
    return TraceContext(
        int(trace_id) or (_process_prefix ^ (next(_counter) << 16)),
        int(produced_at_us) or time.time_ns() // 1000,
    )


def current():
    return _current.get()


def next_event(session):
    next_seq = getattr(session, "next_event_seq", None)
    return (next_seq() if next_seq else 0, current() or new_trace())


def activate(trace):
    return _current.set(trace)


def reset(token):
    _current.reset(token)


def record_receive(trace_id, produced_at_us, callback_at_us=None):
    if not trace_id or not produced_at_us:
        return
    received_at_us = callback_at_us or time.time_ns() // 1000
    metrics_registry.metrics.observe(
        "ofps.trace.produced_to_receive_seconds",
        max(0, received_at_us - produced_at_us) / 1_000_000,
    )
    metrics_registry.metrics.inc("ofps.trace.receive_callbacks_total")
