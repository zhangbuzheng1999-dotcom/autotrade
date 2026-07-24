from __future__ import annotations

import pandas as pd

from ..ledger.engine import replay_ledger
from ..market.base import MarketDataGateway
from ..storage.base import LedgerStorage


class LedgerRebuildService:
    def __init__(
        self,
        *,
        storage: LedgerStorage,
        market: MarketDataGateway,
        current_date_provider=None,
    ):
        self.storage = storage
        self.market = market
        self.current_date_provider = current_date_provider or (lambda: pd.Timestamp.now().normalize())

    def rebuild_history(
        self,
        *,
        account: str,
        book_name: str,
        start_date: str | pd.Timestamp | None = None,
        end_date: str | pd.Timestamp | None = None,
        initial_cash: float = 0.0,
        persist: bool = True,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        trades = self.storage.load_trades(
            account=account,
            book_name=book_name,
            start_date=start_date,
            end_date=end_date,
        )
        if trades.empty:
            return pd.DataFrame(), pd.DataFrame()

        effective_start = pd.to_datetime(start_date).normalize() if start_date is not None else pd.to_datetime(trades["trade_date"]).min().normalize()
        effective_end = (
            pd.to_datetime(end_date).normalize()
            if end_date is not None
            else pd.to_datetime(self.current_date_provider()).normalize()
        )
        prices = self.market.get_prices(
            start_date=effective_start,
            end_date=effective_end,
            order_book_ids=sorted(trades["order_book_id"].astype(str).unique().tolist()) or None,
        )
        positions, equity = replay_ledger(
            trades,
            prices,
            initial_cash=initial_cash,
        )

        if persist:
            if not positions.empty:
                for snapshot_date, day_df in positions.groupby("date", sort=True):
                    self.storage.save_positions(date=snapshot_date, position_df=day_df.drop(columns=["date"]), overwrite=True)
            if not equity.empty:
                for snapshot_date, day_df in equity.groupby("date", sort=True):
                    self.storage.save_equity(date=snapshot_date, equity_df=day_df, overwrite=True)

        return positions, equity
