"""Simulated broker positions, cash, margin, and mark-to-market accounting."""

from __future__ import annotations

from copy import deepcopy

from autotrade.backtest.commission_model import CommissionModel
from autotrade.backtest.margin_model import MarginModel
from autotrade.coreutils.constant import Direction
from autotrade.coreutils.object import (
    AccountData,
    PositionData,
    TradeData,
    ValuationUpdate,
)
from autotrade.engine.security_manager import SecurityManager


class AccountLedger:
    """Authoritative simulated-broker ledger, independent from framework OMS."""

    account_id = "BACKTEST"

    def __init__(
        self,
        *,
        initial_cash: float,
        security_manager: SecurityManager,
        commission_model: CommissionModel | None = None,
        margin_model: MarginModel | None = None,
    ) -> None:
        self.security_manager = security_manager
        self.commission_model = commission_model or CommissionModel()
        self.margin_model = margin_model or MarginModel()
        self.positions: dict[str, PositionData] = {}
        self.mark_prices: dict[str, float] = {}
        self.unrealized_pnl_by_symbol: dict[str, float] = {}
        self.realized_pnl_by_symbol: dict[str, float] = {}
        self.turnover_by_symbol: dict[str, float] = {}
        self.commission_by_symbol: dict[str, float] = {}

        self.account = AccountData("BACKTEST", accountid=self.account_id)
        self.account.cash = float(initial_cash)
        self.account.available = float(initial_cash)
        self.account.equity = float(initial_cash)

    def apply_trade(self, trade: TradeData) -> PositionData:
        previous = deepcopy(self.positions.get(trade.symbol))
        position = self._project_position(trade)
        security = self.security_manager.get(trade.symbol)
        multiplier = float(security.multiplier if security is not None else 1)
        turnover = abs(float(trade.volume)) * float(trade.price) * multiplier
        commission = self.commission_model.calculate(
            direction=trade.direction,
            price=trade.price,
            volume=trade.volume,
            security=security,
        )

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

        self.account.cash += realized_pnl - commission
        self.account.realized_pnl += realized_pnl
        self.realized_pnl_by_symbol[trade.symbol] = (
            self.realized_pnl_by_symbol.get(trade.symbol, 0.0) + realized_pnl
        )
        self.turnover_by_symbol[trade.symbol] = (
            self.turnover_by_symbol.get(trade.symbol, 0.0) + turnover
        )
        self.commission_by_symbol[trade.symbol] = (
            self.commission_by_symbol.get(trade.symbol, 0.0) + commission
        )
        self.mark_prices[trade.symbol] = float(trade.price)
        if position.volume == 0:
            self.unrealized_pnl_by_symbol.pop(trade.symbol, None)
        self._refresh_position_margins()
        self._refresh_portfolio()
        return deepcopy(position)

    def mark_to_market(
        self,
        updates: tuple[ValuationUpdate, ...],
    ) -> None:
        for update in updates:
            self.mark_prices[update.symbol] = float(update.price)
        self._mark_positions()
        self._refresh_position_margins()
        self._refresh_portfolio()

    def get_all_positions(self) -> list[PositionData]:
        return [deepcopy(position) for position in self.positions.values()]

    def _project_position(self, trade: TradeData) -> PositionData:
        current = self.positions.get(trade.symbol)
        old_volume = self._signed_volume(current)
        old_price = 0.0 if current is None else float(current.price)
        delta = (
            abs(float(trade.volume))
            if trade.direction == Direction.LONG
            else -abs(float(trade.volume))
        )
        new_volume = old_volume + delta
        if old_volume == 0 or old_volume * delta > 0:
            new_price = (
                float(trade.price)
                if old_volume == 0
                else (
                    old_price * abs(old_volume)
                    + float(trade.price) * abs(delta)
                ) / abs(new_volume)
            )
        elif new_volume == 0:
            new_price = 0.0
        elif old_volume * new_volume > 0:
            new_price = old_price
        else:
            new_price = float(trade.price)

        position = PositionData(
            gateway_name=trade.gateway_name,
            symbol=trade.symbol,
            exchange=trade.exchange,
            direction=Direction.NET,
            volume=new_volume,
            price=new_price,
            margin=0,
        )
        if new_volume == 0:
            self.positions.pop(trade.symbol, None)
        else:
            self.positions[trade.symbol] = position
        return position

    def _mark_positions(self) -> None:
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
            mark_price = self.mark_prices.get(symbol, float(position.price))
            position.margin = self.margin_model.calculate(
                position=position,
                mark_price=mark_price,
                security=security,
            )

    def _refresh_portfolio(self) -> None:
        self.account.margin = sum(
            float(position.margin) for position in self.positions.values()
        )
        self.account.unrealized_pnl = sum(self.unrealized_pnl_by_symbol.values())
        self.account.equity = self.account.cash + self.account.unrealized_pnl
        self.account.available = self.account.equity - self.account.margin

    @staticmethod
    def _signed_volume(position: PositionData | None) -> float:
        if position is None:
            return 0.0
        volume = float(position.volume)
        if position.direction == Direction.SHORT and volume > 0:
            return -volume
        return volume


__all__ = ["AccountLedger"]
