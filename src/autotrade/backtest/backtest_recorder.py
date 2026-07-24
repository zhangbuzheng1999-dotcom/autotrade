"""Read-only history recorder for backtest state."""

from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

import pandas as pd

from autotrade.engine.security_manager import SecurityManager

if TYPE_CHECKING:
    from autotrade.backtest.backtest_oms_engine import BacktestOms


class BacktestRecorder:
    """Copy authoritative OMS/Security state without calculating it."""

    def __init__(self) -> None:
        self.account_daily: dict = {}
        self.position_daily: dict = {}
        self.contract_daily: dict = {}

    def snapshot(
        self,
        when,
        oms: BacktestOms,
        security_manager: SecurityManager,
    ) -> None:
        account = oms.account
        self.account_daily[when] = {
            "cash": account.cash,
            "margin": account.margin,
            "realized_pnl": account.realized_pnl,
            "unrealized_pnl": account.unrealized_pnl,
            "equity": account.equity,
            "available": account.available,
        }
        self.position_daily[when] = deepcopy(oms.get_all_positions())

        symbols = (
            set(oms.positions)
            | set(oms.realized_pnl_by_symbol)
            | set(oms.turnover_by_symbol)
        )
        self.contract_daily[when] = {
            symbol: self._symbol_snapshot(symbol, oms, security_manager)
            for symbol in sorted(symbols)
        }

    @staticmethod
    def _symbol_snapshot(
        symbol: str,
        oms: BacktestOms,
        security_manager: SecurityManager,
    ) -> dict:
        position = oms.get_position(symbol)
        security = security_manager.get(symbol)
        return {
            "volume": 0.0 if position is None else position.volume,
            "margin": 0.0 if position is None else position.margin,
            "realized_pnl": oms.realized_pnl_by_symbol.get(symbol, 0.0),
            "unrealized_pnl": oms.unrealized_pnl_by_symbol.get(symbol, 0.0),
            "turnover": oms.turnover_by_symbol.get(symbol, 0.0),
            "commission": oms.commission_by_symbol.get(symbol, 0.0),
            "price": None if security is None else security.price,
        }

    @staticmethod
    def get_trade_log_df(oms: BacktestOms) -> pd.DataFrame:
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
            for trade in oms.trade_log
        ])

    def get_account_daily_df(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict(self.account_daily, orient="index")


__all__ = ["BacktestRecorder"]
