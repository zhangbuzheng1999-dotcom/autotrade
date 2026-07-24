"""Accounting and mark-to-market manager for TimeSlice backtests."""

from __future__ import annotations

import math
import sys
from copy import deepcopy
from typing import TYPE_CHECKING

import pandas as pd

from autotrade.backtest.backtest_oms_engine import BacktestOms
from autotrade.coreutils.constant import Direction, Exchange
from autotrade.coreutils.object import PositionData, TradeData

from autotrade.backtest.backtest_event_engine import (
    EVENT_ORDER,
    EVENT_POSITION,
    EVENT_TRADE,
    Event,
    BacktestEventEngine,
)

if TYPE_CHECKING:
    from autotrade.backtest.security_manager import SecurityManager
    from autotrade.coreutils.object import TimeSlice


class AccountingManager:
    """Own account state and snapshots without owning backtest progression.

    Legacy accounting/statistics helpers are called explicitly during the
    migration. The legacy engine itself is not part of this type's public API.
    """

    def __init__(
        self,
        event_engine: BacktestEventEngine,
        *,
        security_manager: "SecurityManager",
        initial_cash: float = 1_000_000,
        risk_free: float = 0.02,
        annual_days: int = 240,
    ) -> None:
        self.gateway_name = "backtest"
        self.event_engine = event_engine
        self.security_manager = security_manager
        self.oms = BacktestOms(event_engine=BacktestEventEngine(), initial_cash=initial_cash)

        self.initial_cash = initial_cash
        self.risk_free = risk_free
        self.annual_days = annual_days
        self.account_daily: dict = {}
        self.contract_daily: dict = {}
        self.position_daily: dict = {}
        self.unrealized_pnl_by_symbol: dict[str, float] = {}

        self.register_event()

    def register_event(self) -> None:
        self.event_engine.register(EVENT_ORDER, self.oms.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)

    def process_trade_event(self, event: Event) -> None:
        """Commit a trade, then publish the authoritative position snapshot."""
        trade: TradeData = event.data
        self._sync_oms_contract(trade.symbol)
        self.oms.process_trade_event(event)

        position = self.get_position(trade.symbol)
        if position is None or position.volume == 0:
            self.unrealized_pnl_by_symbol.pop(trade.symbol, None)
            contract_info = self.oms.contracts_log.get(trade.symbol)
            if contract_info is not None:
                contract_info["unrealized_pnl"] = 0.0
        else:
            unrealized_pnl = self.calculate_position_unrealized_pnl(
                trade.symbol,
                float(trade.price),
            )
            self.unrealized_pnl_by_symbol[trade.symbol] = unrealized_pnl
            contract_info = self.oms.contracts_log.get(trade.symbol)
            if contract_info is not None:
                contract_info["unrealized_pnl"] = unrealized_pnl

        # Legacy OMS resets equity to cash on every trade. Restore portfolio
        # totals from the authoritative per-symbol valuation state.
        self.refresh_account_totals()

        position = self.get_position_snapshot(trade.symbol, exchange=trade.exchange)
        self.event_engine.put(Event(EVENT_POSITION, position))

    def get_position(self, symbol: str) -> PositionData | None:
        """Return the current authoritative OMS position, if it is active."""
        return self.oms.get_position(symbol)

    def get_position_snapshot(
        self,
        symbol: str,
        *,
        exchange: Exchange | None = None,
    ) -> PositionData:
        """Return an immutable-in-meaning snapshot, including flat positions."""
        position = self.get_position(symbol)
        if position is not None:
            return deepcopy(position)

        security = self.security_manager.get(symbol)
        resolved_exchange = exchange or (security.exchange if security is not None else None)
        return PositionData(
            symbol=symbol,
            exchange=resolved_exchange or Exchange.UNKNOWN,
            direction=Direction.NET,
            volume=0,
            price=0,
            margin=0,
            gateway_name=self.gateway_name,
        )

    def get_quantity(self, symbol: str) -> float:
        """Return signed net quantity; absent OMS positions are flat."""
        position = self.get_position(symbol)
        return 0.0 if position is None else float(position.volume)

    def calculate_statistics(self):
        df = pd.DataFrame.from_dict(self.account_daily, orient="index").sort_index()
        equity = df["equity"].astype(float)
        final_equity = float(equity.iloc[-1])
        initial_cash = float(self.initial_cash)
        total_return = final_equity / initial_cash - 1 if initial_cash > 0 else math.nan
        max_drawdown = self._calc_max_drawdown(equity)

        timestamps = pd.to_datetime(equity.index)
        elapsed_seconds = (timestamps[-1] - timestamps[0]).total_seconds()
        annual_return = self._calculate_annual_return(
            initial_cash,
            final_equity,
            elapsed_seconds,
        )

        timed_equity = pd.Series(equity.to_numpy(), index=timestamps).sort_index()
        daily_equity = timed_equity.resample("1D").last().dropna()
        daily_returns = daily_equity.pct_change().dropna()
        if len(daily_returns) >= 2 and daily_returns.std() > 0:
            sharpe = (
                (daily_returns.mean() - self.risk_free / self.annual_days)
                / daily_returns.std()
                * math.sqrt(self.annual_days)
            )
        else:
            sharpe = math.nan

        print("\n===== 回测绩效 =====")
        print(f"初始资金: {self.initial_cash:.2f}")
        print(f"结束资金: {final_equity:.2f}")
        print(f"总收益率: {total_return * 100:.2f}%")
        print(f"年化收益率: {annual_return * 100:.2f}%")
        print(f"最大回撤: {max_drawdown * 100:.2f}%")
        print(f"Sharpe Ratio: {sharpe:.2f}")
        return {
            "total_return": f"{total_return * 100:.2f}%",
            "annual_return": f"{annual_return * 100:.2f}%",
            "sharpe": sharpe,
            "max_drawdown": f"{max_drawdown * 100:.2f}%",
        }

    @staticmethod
    def _calculate_annual_return(
        initial_equity: float,
        final_equity: float,
        elapsed_seconds: float,
    ) -> float:
        """Annualize only periods of at least one day without numeric overflow."""
        if (
            elapsed_seconds < 24 * 60 * 60
            or initial_equity <= 0
            or final_equity <= 0
            or not math.isfinite(initial_equity)
            or not math.isfinite(final_equity)
        ):
            return math.nan

        years = elapsed_seconds / (365.25 * 24 * 60 * 60)
        annual_log_return = (
            math.log(final_equity) - math.log(initial_equity)
        ) / years
        if annual_log_return > math.log(sys.float_info.max):
            return math.inf
        return math.expm1(annual_log_return)

    def _calc_max_drawdown(self, equity_series):
        peak = equity_series.iloc[0]
        max_drawdown = 0.0
        for equity in equity_series:
            peak = max(peak, equity)
            drawdown = (peak - equity) / peak
            max_drawdown = max(max_drawdown, drawdown)
        return max_drawdown

    def get_trade_log_df(self):
        return pd.DataFrame([
            {
                "datetime": trade.datetime,
                "symbol": trade.symbol,
                "orderid": trade.orderid,
                "direction": trade.direction,
                "price": trade.price,
                "traded": trade.traded,
                "volume": trade.volume,
                "avgFillPrice": trade.avgFillPrice,
                "status": trade.status,
                "reference": trade.reference,
            }
            for trade in self.oms.trade_log
        ])

    def get_account_daily_df(self):
        return pd.DataFrame.from_dict(self.account_daily, orient="index")

    def on_timeslice(self, time_slice: "TimeSlice") -> None:
        marks_changed = self.mark_to_market(time_slice)
        rules_changed = bool(time_slice.security_updates)
        if not marks_changed and not rules_changed:
            return

        self.refresh_position_margins()
        self.refresh_account_totals()
        self.snapshot(time_slice.time)

    def mark_to_market(self, time_slice: "TimeSlice") -> bool:
        """Incrementally value only symbols with a new selected mark price."""
        mark_prices = self.resolve_mark_prices(time_slice)
        if not mark_prices:
            return False

        for symbol, mark_price in mark_prices.items():
            position = self.get_position(symbol)
            if position is None or position.volume == 0:
                self.unrealized_pnl_by_symbol.pop(symbol, None)
                continue

            unrealized_pnl = self.calculate_position_unrealized_pnl(
                symbol,
                mark_price,
            )
            self.unrealized_pnl_by_symbol[symbol] = unrealized_pnl

            contract_info = self.oms.contracts_log.get(symbol)
            if contract_info is not None:
                contract_info["unrealized_pnl"] = unrealized_pnl

        return True

    def refresh_position_margins(self) -> None:
        """Revalue margin from the latest Security state without rule copies."""
        for symbol, position in self.oms.positions.items():
            security = self.security_manager.get(symbol)
            if security is None:
                continue
            self._sync_oms_contract(symbol)
            mark_price = security.price
            if mark_price is None:
                mark_price = float(position.price)
            position.margin = (
                abs(float(position.volume))
                * float(mark_price)
                * float(security.multiplier)
                * float(security.margin_rate)
            )
            contract_info = self.oms.contracts_log.get(symbol)
            if contract_info is not None:
                contract_info["margin"] = position.margin
        self.oms.get_account("BACKTEST").margin = sum(
            position.margin for position in self.oms.positions.values()
        )

    def calculate_position_unrealized_pnl(
        self,
        symbol: str,
        mark_price: float,
    ) -> float:
        """Calculate PnL from signed net quantity and contract multiplier."""
        position = self.get_position(symbol)
        if position is None or position.volume == 0:
            return 0.0

        security = self.security_manager.get(symbol)
        size = float(security.multiplier if security is not None else 1.0)
        return (
            (float(mark_price) - float(position.price))
            * float(position.volume)
            * size
        )

    def refresh_account_totals(self) -> None:
        """Rebuild portfolio totals without changing symbols lacking new marks."""
        active_symbols = set(self.oms.positions)
        for symbol in tuple(self.unrealized_pnl_by_symbol):
            if symbol not in active_symbols:
                del self.unrealized_pnl_by_symbol[symbol]

        total_unrealized_pnl = sum(self.unrealized_pnl_by_symbol.values())
        account = self.oms.get_account("BACKTEST")
        account.unrealized_pnl = total_unrealized_pnl
        account.equity = account.cash + total_unrealized_pnl
        account.available = account.cash + total_unrealized_pnl - account.margin

    def snapshot(self, when) -> None:
        """Record current accounting state independently from valuation."""
        account = self.oms.get_account("BACKTEST")
        self.account_daily[when] = {
            "cash": account.cash,
            "margin": account.margin,
            "realized_pnl": account.realized_pnl,
            "unrealized_pnl": account.unrealized_pnl,
            "equity": account.equity,
            "available": account.available,
        }
        self.contract_daily[when] = self.oms.get_contract_log()
        self.position_daily[when] = deepcopy(self.oms.get_all_positions())

    def resolve_mark_prices(self, time_slice: "TimeSlice") -> dict[str, float]:
        return {
            update.symbol: update.price
            for update in time_slice.valuation_updates
        }

    def _sync_oms_contract(self, symbol: str) -> None:
        security = self.security_manager.get(symbol)
        if security is None:
            return
        self.oms.set_contract_params(
            symbol,
            size=security.multiplier,
            margin_rate=security.margin_rate,
            long_rate=(
                security.long_commission_rate
                if security.long_commission_rate is not None
                else security.commission_rate
            ),
            short_rate=(
                security.short_commission_rate
                if security.short_commission_rate is not None
                else security.commission_rate
            ),
        )


__all__ = ["AccountingManager"]
