from __future__ import annotations

import pandas as pd

from ..ledger.schema import PRICE_COLUMNS
from ..storage.schema import INSTRUMENT_COLUMNS

from .base import MarketDataGateway


class InMemoryMarketDataGateway(MarketDataGateway):
    def __init__(self, price_df: pd.DataFrame | None = None, instrument_df: pd.DataFrame | None = None):
        self._price_df = pd.DataFrame(columns=PRICE_COLUMNS) if price_df is None else price_df.copy()
        self._instrument_df = pd.DataFrame(columns=INSTRUMENT_COLUMNS) if instrument_df is None else instrument_df.copy()
        for col in PRICE_COLUMNS:
            if col not in self._price_df.columns:
                self._price_df[col] = pd.NA
        self._price_df = self._price_df[PRICE_COLUMNS].copy()
        for col in INSTRUMENT_COLUMNS:
            if col not in self._instrument_df.columns:
                self._instrument_df[col] = pd.NA
        self._instrument_df = self._instrument_df[INSTRUMENT_COLUMNS].copy()

    def seed_prices(self, price_df: pd.DataFrame) -> None:
        self._price_df = price_df.copy()
        for col in PRICE_COLUMNS:
            if col not in self._price_df.columns:
                self._price_df[col] = pd.NA
        self._price_df = self._price_df[PRICE_COLUMNS].copy()

    def seed_instruments(self, instrument_df: pd.DataFrame) -> None:
        self._instrument_df = instrument_df.copy()
        for col in INSTRUMENT_COLUMNS:
            if col not in self._instrument_df.columns:
                self._instrument_df[col] = pd.NA
        self._instrument_df = self._instrument_df[INSTRUMENT_COLUMNS].copy()

    def get_prices(
        self,
        *,
        start_date: str | pd.Timestamp,
        end_date: str | pd.Timestamp,
        order_book_ids: list[str] | None = None,
    ) -> pd.DataFrame:
        prices = self._price_df.copy()
        if prices.empty:
            return prices

        start = pd.to_datetime(start_date).normalize()
        end = pd.to_datetime(end_date).normalize()
        prices["date"] = pd.to_datetime(prices["date"]).dt.normalize()
        mask = (prices["date"] >= start) & (prices["date"] <= end)
        if order_book_ids:
            mask &= prices["order_book_id"].astype(str).isin([str(x) for x in order_book_ids])
        return prices.loc[mask, PRICE_COLUMNS].sort_values(["date", "order_book_id"]).reset_index(drop=True)

    def get_instruments(
        self,
        *,
        order_book_ids: list[str],
    ) -> pd.DataFrame:
        instruments = self._instrument_df.copy()
        if instruments.empty:
            return instruments
        mask = instruments["order_book_id"].astype(str).isin([str(x) for x in order_book_ids])
        return instruments.loc[mask, INSTRUMENT_COLUMNS].sort_values("order_book_id").reset_index(drop=True)
