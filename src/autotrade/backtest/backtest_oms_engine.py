"""Backtest account simulation built on the shared OMS state machine."""

from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

from autotrade.coreutils.constant import Direction
from autotrade.coreutils.object import AccountData, PositionData, TradeData
from autotrade.backtest.backtest_recorder import BacktestRecorder
from autotrade.engine.event_engine import EVENT_ACCOUNT, Event
from autotrade.engine.oms_engine import OmsBase

if TYPE_CHECKING:
    from autotrade.backtest.backtest_event_engine import BacktestEventEngine
    from autotrade.coreutils.object import TimeSlice
    from autotrade.engine.security_manager import SecurityManager


class BacktestOms(OmsBase):
    """Simulate broker accounting while reusing common OMS trade/position flow."""

    account_id = "BACKTEST"

    def __init__(
        self,
        event_engine: "BacktestEventEngine",
        *,
        security_manager: "SecurityManager",
        initial_cash: float = 1_000_000,
        recorder: BacktestRecorder | None = None,
    ) -> None:
        self.security_manager = security_manager
        self.recorder = recorder or BacktestRecorder()
        self.initial_cash = float(initial_cash)
        self.mark_prices: dict[str, float] = {}
        self.unrealized_pnl_by_symbol: dict[str, float] = {}
        self.realized_pnl_by_symbol: dict[str, float] = {}
        self.turnover_by_symbol: dict[str, float] = {}
        self.commission_by_symbol: dict[str, float] = {}
        super().__init__(event_engine)

        account = AccountData("BACKTEST", accountid=self.account_id)
        account.cash = self.initial_cash
        account.available = self.initial_cash
        account.equity = self.initial_cash
        self._account_key = account.vt_accountid
        self.accounts[self._account_key] = account

    @property
    def account(self) -> AccountData:
        return self.accounts[self._account_key]

    @property
    def trade_log(self) -> list[TradeData]:
        """Ordered compatibility view backed by the authoritative trade dict."""
        return list(self.trades.values())

    def _after_trade_applied(
        self,
        trade: TradeData,
        previous: PositionData | None,
        position: PositionData,
    ) -> None:
        security = self.security_manager.get(trade.symbol)
        multiplier = float(security.multiplier if security is not None else 1)
        commission_rate = self._commission_rate(security, trade.direction)
        turnover = abs(float(trade.volume)) * float(trade.price) * multiplier
        commission = turnover * commission_rate

        old_volume = self._signed_volume(previous)
        delta = (
            abs(float(trade.volume))
            if trade.direction == Direction.LONG
            else -abs(float(trade.volume))
        )
        close_quantity = (
            min(abs(old_volume), abs(delta))
            if old_volume * delta < 0
            else 0.0
        )
        realized_pnl = 0.0
        if close_quantity and previous is not None:
            if old_volume > 0:
                realized_pnl = (
                    float(trade.price) - float(previous.price)
                ) * close_quantity * multiplier
            else:
                realized_pnl = (
                    float(previous.price) - float(trade.price)
                ) * close_quantity * multiplier

        account = self.account
        account.cash += realized_pnl - commission
        account.realized_pnl += realized_pnl
        self.realized_pnl_by_symbol[trade.symbol] = (
            self.realized_pnl_by_symbol.get(trade.symbol, 0.0)
            + realized_pnl
        )
        self.turnover_by_symbol[trade.symbol] = (
            self.turnover_by_symbol.get(trade.symbol, 0.0)
            + turnover
        )
        self.commission_by_symbol[trade.symbol] = (
            self.commission_by_symbol.get(trade.symbol, 0.0)
            + commission
        )

        self.mark_prices[trade.symbol] = float(trade.price)
        if position.volume == 0:
            self.unrealized_pnl_by_symbol.pop(trade.symbol, None)
        self._refresh_position_margins()
        self._refresh_portfolio()

    def on_timeslice(self, time_slice: "TimeSlice") -> bool:
        """Settle and record only when the slice carries valuation data."""
        if not time_slice.valuation_updates:
            return False

        for update in time_slice.valuation_updates:
            self.mark_prices[update.symbol] = float(update.price)

        self._mark_to_market()
        self._refresh_position_margins()
        self._refresh_portfolio()
        self.publish_account()
        self.recorder.snapshot(
            time_slice.time,
            self,
            self.security_manager,
        )
        return True

    @property
    def account_daily(self):
        return self.recorder.account_daily

    @property
    def position_daily(self):
        return self.recorder.position_daily

    @property
    def contract_daily(self):
        return self.recorder.contract_daily

    def get_trade_log_df(self):
        return self.recorder.get_trade_log_df(self)

    def get_account_daily_df(self):
        return self.recorder.get_account_daily_df()

    def _mark_to_market(self) -> None:
        active_symbols = set(self.positions)
        for symbol in tuple(self.unrealized_pnl_by_symbol):
            if symbol not in active_symbols:
                del self.unrealized_pnl_by_symbol[symbol]

        for symbol, position in self.positions.items():
            security = self.security_manager.get(symbol)
            multiplier = float(security.multiplier if security is not None else 1)
            mark_price = self.mark_prices.get(symbol, float(position.price))
            self.unrealized_pnl_by_symbol[symbol] = (
                (mark_price - float(position.price))
                * float(position.volume)
                * multiplier
            )

    def _refresh_position_margins(self) -> None:
        for symbol, position in self.positions.items():
            security = self.security_manager.get(symbol)
            if security is None:
                position.margin = 0.0
                continue
            mark_price = self.mark_prices.get(symbol, float(position.price))
            position.margin = (
                abs(float(position.volume))
                * mark_price
                * float(security.multiplier)
                * float(security.margin_rate)
            )

    def _refresh_portfolio(self) -> None:
        account = self.account
        account.margin = sum(
            float(position.margin)
            for position in self.positions.values()
        )
        account.unrealized_pnl = sum(self.unrealized_pnl_by_symbol.values())
        account.equity = account.cash + account.unrealized_pnl
        account.available = account.equity - account.margin

    def publish_account(self) -> None:
        self.event_engine.put(Event(EVENT_ACCOUNT, deepcopy(self.account)))

    @staticmethod
    def _signed_volume(position: PositionData | None) -> float:
        if position is None:
            return 0.0
        volume = float(position.volume)
        if position.direction == Direction.SHORT and volume > 0:
            return -volume
        return volume

    @staticmethod
    def _commission_rate(security, direction: Direction) -> float:
        if security is None:
            return 0.0
        if direction == Direction.LONG:
            rate = security.long_commission_rate
        else:
            rate = security.short_commission_rate
        return float(security.commission_rate if rate is None else rate)


__all__ = ["BacktestOms"]
