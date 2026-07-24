"""Synchronous EventEngine specialization for deterministic backtesting."""

from __future__ import annotations

from collections import deque

from autotrade.engine.event_engine import (
    EVENT_ACCOUNT,
    EVENT_DATA,
    EVENT_LOG,
    EVENT_ORDER,
    EVENT_POSITION,
    EVENT_REQUEST,
    EVENT_REQUEST_STATUS,
    EVENT_SLICE,
    EVENT_TRADE,
    Event,
    EventEngine,
    Message,
    MessageKind,
)

class BacktestEventEngine(EventEngine):
    """Single-threaded, re-entry-safe event dispatcher.

    Handler registration and dispatch rules come from ``EventEngine``. Only
    lifecycle and queue progression differ: backtests process the queue on the
    caller's thread and ``put`` returns after all resulting events are drained.
    """

    def __init__(self) -> None:
        super().__init__()
        self._queue: deque[Event | Message] = deque()
        self._processing = False

    def start(self) -> None:
        """Compatibility no-op: synchronous dispatch is always available."""

    def stop(self) -> None:
        """Compatibility no-op: no worker threads are owned by this engine."""

    def put(self, event: Event | Message) -> None:
        self._queue.append(event)
        if self._processing:
            return

        self._processing = True
        try:
            while self._queue:
                self._process(self._queue.popleft())
        finally:
            self._processing = False

__all__ = [
    "EVENT_ACCOUNT",
    "EVENT_DATA",
    "EVENT_LOG",
    "EVENT_ORDER",
    "EVENT_POSITION",
    "EVENT_REQUEST",
    "EVENT_REQUEST_STATUS",
    "EVENT_SLICE",
    "EVENT_TRADE",
    "Event",
    "Message",
    "MessageKind",
    "BacktestEventEngine",
]
