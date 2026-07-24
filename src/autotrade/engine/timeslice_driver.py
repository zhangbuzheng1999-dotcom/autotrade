"""Ordered TimeSlice progression shared by live and backtest runtimes."""

from __future__ import annotations

from autotrade.coreutils.object import TimeSlice
from autotrade.engine.event_engine import (
    COMMAND_ACCOUNT_VALUATION,
    COMMAND_MARKET_AFTER,
    COMMAND_MARKET_BEFORE,
    EVENT_DATA,
    EVENT_SLICE,
    Event,
    EventEngine,
    Message,
    MessageKind,
    RouteNotFoundError,
)


class TimeSliceDriver:
    def __init__(
        self,
        event_engine: EventEngine,
        *,
        clock=None,
        simulated_broker: bool = False,
        source: str = "timeslice_driver",
    ) -> None:
        self.event_engine = event_engine
        self.clock = clock
        self.simulated_broker = simulated_broker
        self.source = source

    def process(self, time_slice: TimeSlice) -> None:
        if not isinstance(time_slice, TimeSlice):
            raise TypeError(
                f"TimeSliceDriver expects TimeSlice, got "
                f"{type(time_slice).__name__}"
            )
        if self.clock is not None and time_slice.time is not None:
            advance = getattr(self.clock, "advance", None)
            if advance is not None:
                advance(time_slice.time)

        for update in time_slice.security_updates:
            self.event_engine.put(Event(EVENT_DATA, update))

        if time_slice.is_bootstrap:
            return

        if self.simulated_broker:
            self._send(COMMAND_MARKET_BEFORE, time_slice)

        self.event_engine.put(Event(EVENT_SLICE, time_slice.slice))

        if self.simulated_broker:
            self._send(COMMAND_MARKET_AFTER, time_slice)
            if time_slice.valuation_updates:
                self._send(COMMAND_ACCOUNT_VALUATION, time_slice)

    def _send(self, name: str, time_slice: TimeSlice) -> None:
        self.event_engine.put(
            Message(
                MessageKind.COMMAND,
                name,
                time_slice,
                source=self.source,
                target="simulated_broker",
            )
        )


__all__ = ["TimeSliceDriver"]
