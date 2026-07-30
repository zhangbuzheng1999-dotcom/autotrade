from __future__ import annotations

import numpy as np
import pandas as pd

from autotrade.analytics.options import calculate_black97_greeks
from autotrade.data.ricequant.base import FetchMode, FetchStatus
from autotrade.data.ricequant.service.futures import FuturePriceService
from autotrade.data.ricequant.service.options import (
    OptionInstrumentService,
    OptionPriceService,
)
from autotrade.data.ricequant.spec.calculated_options import CalculatedOptionGreeksSpec


def _successful_data(result, resource: str) -> pd.DataFrame:
    if result.status != FetchStatus.SUCCESS:
        raise RuntimeError(f"{resource} SOURCE_ONLY failed") from result.error
    return pd.DataFrame() if result.data is None else result.data.copy()


class CalculatedOptionGreeksDataSource:
    """Build a complete option-symbol cross-section from SOURCE_ONLY inputs."""

    def __init__(
        self,
        spec: CalculatedOptionGreeksSpec | None = None,
        *,
        option_instrument_service=None,
        option_price_service=None,
        future_price_service=None,
    ):
        self.spec = spec or CalculatedOptionGreeksSpec()
        self._option_instrument_service = option_instrument_service
        self._option_price_service = option_price_service
        self._future_price_service = future_price_service

    @property
    def option_instrument_service(self):
        if self._option_instrument_service is None:
            self._option_instrument_service = OptionInstrumentService()
        return self._option_instrument_service

    @property
    def option_price_service(self):
        if self._option_price_service is None:
            self._option_price_service = OptionPriceService()
        return self._option_price_service

    @property
    def future_price_service(self):
        if self._future_price_service is None:
            self._future_price_service = FuturePriceService()
        return self._future_price_service

    def fetch(self, **filters) -> pd.DataFrame:
        filters = self.spec.fill_default_filters(
            self.spec.normalize_query_filters(
                {key: value for key, value in filters.items() if value is not None}
            )
        )
        self.spec.validate_filters(filters, FetchMode.SOURCE_ONLY)

        # Deliberately SOURCE_ONLY: a live calculation must not mix DB metadata
        # with source prices.
        instruments = _successful_data(
            self.option_instrument_service.get(
                mode=FetchMode.SOURCE_ONLY,
                persist=False,
                market=filters["market"],
            ),
            "option instruments",
        )
        instruments = self._resolve_universe(instruments, filters)
        if instruments.empty:
            return self.spec.normalize_df(pd.DataFrame(), filters)

        option_ids = instruments["order_book_id"].astype(str).unique().tolist()
        option_prices = _successful_data(
            self.option_price_service.get(
                mode=FetchMode.SOURCE_ONLY,
                persist=False,
                order_book_ids=option_ids,
                start_date=filters["start_date"],
                end_date=filters["end_date"],
                frequency=filters["frequency"],
                fields=[
                    "open", "close", "high", "low", "total_turnover",
                    "volume", "open_interest",
                ],
                market=filters["market"],
            ),
            "option prices",
        )
        if option_prices.empty:
            return self.spec.normalize_df(pd.DataFrame(), filters)

        panel = option_prices.merge(
            instruments[
                [
                    "order_book_id", "underlying_order_book_id",
                    "underlying_symbol", "maturity_date", "strike_price",
                    "option_type",
                ]
            ],
            on="order_book_id",
            how="left",
            validate="many_to_one",
        )
        panel["date"] = pd.to_datetime(panel["date"])
        panel["maturity_date"] = pd.to_datetime(panel["maturity_date"])
        panel["t_days"] = (panel["maturity_date"] - panel["date"]).dt.days
        panel["risk_free_rate"] = float(filters["risk_free_rate"])
        panel["option_price"] = pd.to_numeric(panel[filters["price_type"]], errors="coerce")
        panel["opt_symbol"] = panel["underlying_symbol"].astype(str)

        pieces = []
        for opt_symbol, symbol_panel in panel.groupby("opt_symbol", sort=False):
            pieces.append(self._attach_forward(symbol_panel, filters))
        calculation_input = pd.concat(pieces, ignore_index=True) if pieces else panel
        calculated = calculate_black97_greeks(calculation_input)

        calculated["date"] = pd.to_datetime(calculated["date"]).dt.date
        calculated["maturity_date"] = pd.to_datetime(
            calculated["maturity_date"], errors="coerce"
        ).dt.date
        calculated["price_type"] = filters["price_type"]
        calculated["frequency"] = filters["frequency"]
        calculated["market"] = filters["market"]
        calculated["model_id"] = filters["model_id"]
        calculated["model_version"] = filters["model_version"]
        return self.spec.normalize_df(calculated, filters)

    @staticmethod
    def _resolve_universe(instruments: pd.DataFrame, filters: dict) -> pd.DataFrame:
        result = instruments.copy()
        result["order_book_id"] = result["order_book_id"].astype(str)
        requested_ids = filters.get("order_book_ids")
        opt_symbol = filters.get("opt_symbol")

        if requested_ids:
            requested = result[result["order_book_id"].isin(requested_ids)]
            missing = set(requested_ids) - set(requested["order_book_id"])
            if missing:
                raise ValueError(
                    f"Unknown option order_book_ids from SOURCE_ONLY instruments: "
                    f"{sorted(missing)[:20]}"
                )
            symbols = requested["underlying_symbol"].dropna().astype(str).unique()
            result = result[result["underlying_symbol"].astype(str).isin(symbols)]
        elif opt_symbol:
            symbol = str(opt_symbol)
            values = result["underlying_symbol"].astype(str)
            result = result[values.eq(symbol) | values.str.split(".").str[0].eq(symbol)]

        start = pd.to_datetime(filters["start_date"])
        end = pd.to_datetime(filters["end_date"])
        listed = pd.to_datetime(result["listed_date"], errors="coerce")
        maturity = pd.to_datetime(result["maturity_date"], errors="coerce")
        return result[(listed <= end) & (maturity >= start)].copy()

    def _attach_forward(self, panel: pd.DataFrame, filters: dict) -> pd.DataFrame:
        result = panel.copy()
        underlying_ids = result["underlying_order_book_id"].dropna().astype(str).unique()
        is_future = len(underlying_ids) > 0 and all("." not in value for value in underlying_ids)
        if not is_future:
            return self._attach_parity_forward(result)

        future_prices = _successful_data(
            self.future_price_service.get(
                mode=FetchMode.SOURCE_ONLY,
                persist=False,
                order_book_ids=underlying_ids.tolist(),
                start_date=filters["start_date"],
                end_date=filters["end_date"],
                frequency=filters["frequency"],
                fields=["close"],
                market=filters["market"],
            ),
            "future prices",
        )
        forward = future_prices[
            ["order_book_id", "date", "close"]
        ].rename(
            columns={
                "order_book_id": "underlying_order_book_id",
                "close": "forward_price",
            }
        )
        forward["date"] = pd.to_datetime(forward["date"])
        result = result.merge(
            forward,
            on=["underlying_order_book_id", "date"],
            how="left",
            validate="many_to_one",
        )
        result["forward_method"] = "future_close"
        return result

    @staticmethod
    def _attach_parity_forward(panel: pd.DataFrame) -> pd.DataFrame:
        result = panel.copy()
        quotes = result.pivot_table(
            index=["date", "t_days", "strike_price"],
            columns="option_type",
            values="option_price",
            aggfunc="median",
        ).reset_index()
        if not {"C", "P"}.issubset(quotes.columns):
            result["forward_price"] = np.nan
            result["forward_method"] = "put_call_parity"
            return result

        maturity = quotes["t_days"] / 365.0
        quotes["forward_candidate"] = quotes["strike_price"] + np.exp(
            float(result["risk_free_rate"].iloc[0]) * maturity
        ) * (quotes["C"] - quotes["P"])
        forward = (
            quotes.groupby(["date", "t_days"], as_index=False)["forward_candidate"]
            .median()
            .rename(columns={"forward_candidate": "forward_price"})
        )
        result = result.merge(forward, on=["date", "t_days"], how="left")
        result["forward_method"] = "put_call_parity"
        return result
