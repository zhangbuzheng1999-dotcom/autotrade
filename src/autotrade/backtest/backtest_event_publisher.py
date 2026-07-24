"""Translate simulated-broker results into the shared trading event protocol."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass

from autotrade.backtest.backtest_event_engine import BacktestEventEngine
from autotrade.coreutils.object import (
    AccountData,
    LogData,
    OrderData,
    PositionData,
    RequestStatus,
    TradeData,
)
from autotrade.engine.event_engine import (
    EVENT_ACCOUNT,
    EVENT_LOG,
    EVENT_ORDER,
    EVENT_POSITION_SNAPSHOT,
    EVENT_REQUEST_STATUS,
    EVENT_TRADE,
    Event,
)


@dataclass(slots=True)
class BacktestEventPublisher:
    event_engine: BacktestEventEngine

    def order(self, data: OrderData) -> None:
        self.event_engine.put(Event(EVENT_ORDER, deepcopy(data)))

    def trade(self, data: TradeData) -> None:
        self.event_engine.put(Event(EVENT_TRADE, deepcopy(data)))

    def position_snapshot(self, data: PositionData) -> None:
        self.event_engine.put(Event(EVENT_POSITION_SNAPSHOT, deepcopy(data)))

    def account(self, data: AccountData) -> None:
        self.event_engine.put(Event(EVENT_ACCOUNT, deepcopy(data)))

    def request_status(self, data: RequestStatus) -> None:
        self.event_engine.put(Event(EVENT_REQUEST_STATUS, deepcopy(data)))

    def log(self, data: LogData) -> None:
        self.event_engine.put(Event(EVENT_LOG, data))


__all__ = ["BacktestEventPublisher"]
