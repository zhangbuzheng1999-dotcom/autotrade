from __future__ import annotations

import pandas as pd

from ..ledger.engine import build_daily_snapshots
from ..market.base import MarketDataGateway
from ..storage.base import LedgerStorage


class LedgerRefreshService:
    def __init__(self, *, storage: LedgerStorage, market: MarketDataGateway):
        self.storage = storage
        self.market = market

    def refresh_daily(
        self,
        *,
        account: str,
        book_name: str,
        date: str | pd.Timestamp,
        persist: bool = True,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        snapshot_date = pd.to_datetime(date).normalize()
        trades = self.storage.load_trades(
            account=account,
            book_name=book_name,
            start_date=snapshot_date,
            end_date=snapshot_date,
        )
        prev_positions = self.storage.load_latest_positions(
            account=account,
            book_name=book_name,
            before_date=snapshot_date,
        )
        equity_history = self.storage.load_equity(
            account=account,
            book_name=book_name,
            end_date=snapshot_date,
        )
        if equity_history.empty:
            opening_cash = 0.0
            opening_realized_pnl = 0.0
            opening_fee = 0.0
        else:
            equity_history["date"] = pd.to_datetime(equity_history["date"]).dt.normalize()
            previous = equity_history.loc[equity_history["date"] < snapshot_date]
            if previous.empty:
                opening_cash = 0.0
                opening_realized_pnl = 0.0
                opening_fee = 0.0
            else:
                latest = previous.sort_values("date").iloc[-1]
                opening_cash = float(latest["cash"])
                opening_realized_pnl = float(latest["realized_pnl_cum"])
                opening_fee = float(latest["fee_cum"])

        order_book_ids = sorted(set(trades["order_book_id"].astype(str).tolist()) | set(prev_positions["order_book_id"].astype(str).tolist()))
        prices = self.market.get_prices(
            start_date=snapshot_date,
            end_date=snapshot_date,
            order_book_ids=order_book_ids or None,
        )

        positions, equity = build_daily_snapshots(
            date=snapshot_date,
            trade_df=trades,
            pre_position_df=prev_positions,
            price_df=prices,
            opening_cash=opening_cash,
            opening_realized_pnl=opening_realized_pnl,
            opening_fee=opening_fee,
        )

        if persist:
            self.storage.save_positions(date=snapshot_date, position_df=positions, overwrite=True)
            self.storage.save_equity(date=snapshot_date, equity_df=equity, overwrite=True)

        return positions, equity
