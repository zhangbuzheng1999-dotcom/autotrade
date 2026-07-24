from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class LedgerStorage(ABC):
    @abstractmethod
    def save_trades(
        self,
        *,
        trade_df: pd.DataFrame,
        overwrite: bool = False,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def load_trades(
        self,
        *,
        account: str | None = None,
        book_name: str | None = None,
        start_date: str | pd.Timestamp | None = None,
        end_date: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def load_latest_positions(
        self,
        *,
        account: str,
        book_name: str,
        before_date: str | pd.Timestamp,
    ) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def load_positions(
        self,
        *,
        account: str | None = None,
        book_name: str | None = None,
        start_date: str | pd.Timestamp | None = None,
        end_date: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def save_positions(
        self,
        *,
        date: str | pd.Timestamp,
        position_df: pd.DataFrame,
        overwrite: bool = True,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def load_equity(
        self,
        *,
        account: str | None = None,
        book_name: str | None = None,
        start_date: str | pd.Timestamp | None = None,
        end_date: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def save_equity(
        self,
        *,
        date: str | pd.Timestamp,
        equity_df: pd.DataFrame,
        overwrite: bool = True,
    ) -> None:
        raise NotImplementedError
