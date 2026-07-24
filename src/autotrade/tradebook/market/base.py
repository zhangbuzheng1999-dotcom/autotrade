from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class MarketDataGateway(ABC):
    @abstractmethod
    def get_prices(
        self,
        *,
        start_date: str | pd.Timestamp,
        end_date: str | pd.Timestamp,
        order_book_ids: list[str] | None = None,
    ) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def get_instruments(
        self,
        *,
        order_book_ids: list[str],
    ) -> pd.DataFrame:
        raise NotImplementedError
