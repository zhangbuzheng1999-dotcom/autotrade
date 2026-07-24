"""Shared clock abstractions for live and deterministic backtest runtimes."""

from __future__ import annotations

from datetime import datetime


class LiveClock:
    def now(self) -> datetime:
        return datetime.now()


class BacktestClock:
    def __init__(self) -> None:
        self._current: datetime | None = None

    def advance(self, when: datetime) -> None:
        self._current = when

    def now(self) -> datetime:
        if self._current is None:
            raise RuntimeError("backtest clock has not been advanced")
        return self._current


__all__ = ["BacktestClock", "LiveClock"]
