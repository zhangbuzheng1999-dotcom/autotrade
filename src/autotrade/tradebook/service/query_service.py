from __future__ import annotations

import pandas as pd

from ..storage.base import LedgerStorage


class LedgerQueryService:
    def __init__(self, *, storage: LedgerStorage):
        self.storage = storage

    def get_latest_positions(
        self,
        *,
        account: str,
        book_name: str,
        asof_date: str | pd.Timestamp,
    ) -> pd.DataFrame:
        cutoff = pd.to_datetime(asof_date).normalize() + pd.Timedelta(days=1)
        return self.storage.load_latest_positions(
            account=account,
            book_name=book_name,
            before_date=cutoff,
        )

    def get_positions(
        self,
        *,
        account: str | None = None,
        book_name: str | None = None,
        start_date: str | pd.Timestamp | None = None,
        end_date: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        return self.storage.load_positions(
            account=account,
            book_name=book_name,
            start_date=start_date,
            end_date=end_date,
        )

    def get_equity_series(
        self,
        *,
        account: str | None = None,
        book_name: str | None = None,
        start_date: str | pd.Timestamp | None = None,
        end_date: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        return self.storage.load_equity(
            account=account,
            book_name=book_name,
            start_date=start_date,
            end_date=end_date,
        )
