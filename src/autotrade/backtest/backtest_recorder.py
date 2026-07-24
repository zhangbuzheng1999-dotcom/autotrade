"""Read-only history recorder for backtest state."""

from __future__ import annotations

from copy import deepcopy
import pandas as pd

from autotrade.backtest.account_ledger import AccountLedger
from autotrade.engine.security_manager import SecurityManager
from autotrade.engine.oms_engine import OmsBase


class BacktestRecorder:
    """Copy authoritative OMS/Security state without calculating it."""

    def __init__(self) -> None:
        self.account_daily: dict = {}
        self.position_daily: dict = {}
        self.contract_daily: dict = {}

    def snapshot(
        self,
        when,
        ledger: AccountLedger,
        security_manager: SecurityManager,
    ) -> None:
        account = ledger.account
        self.account_daily[when] = {
            "cash": account.cash,
            "margin": account.margin,
            "realized_pnl": account.realized_pnl,
            "unrealized_pnl": account.unrealized_pnl,
            "equity": account.equity,
            "available": account.available,
        }
        self.position_daily[when] = deepcopy(ledger.get_all_positions())

        symbols = (
            set(ledger.positions)
            | set(ledger.realized_pnl_by_symbol)
            | set(ledger.turnover_by_symbol)
        )
        self.contract_daily[when] = {
            symbol: self._symbol_snapshot(symbol, ledger, security_manager)
            for symbol in sorted(symbols)
        }

    @staticmethod
    def _symbol_snapshot(
        symbol: str,
        ledger: AccountLedger,
        security_manager: SecurityManager,
    ) -> dict:
        position = ledger.positions.get(symbol)
        security = security_manager.get(symbol)
        return {
            "volume": 0.0 if position is None else position.volume,
            "margin": 0.0 if position is None else position.margin,
            "realized_pnl": ledger.realized_pnl_by_symbol.get(symbol, 0.0),
            "unrealized_pnl": ledger.unrealized_pnl_by_symbol.get(symbol, 0.0),
            "turnover": ledger.turnover_by_symbol.get(symbol, 0.0),
            "commission": ledger.commission_by_symbol.get(symbol, 0.0),
            "price": None if security is None else security.price,
        }

    @staticmethod
    def get_trade_log_df(oms: OmsBase) -> pd.DataFrame:
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
            for trade in oms.get_all_trades()
        ])

    def get_account_daily_df(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict(self.account_daily, orient="index")


__all__ = ["BacktestRecorder"]
