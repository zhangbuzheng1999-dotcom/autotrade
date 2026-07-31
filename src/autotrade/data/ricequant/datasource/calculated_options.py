from __future__ import annotations

import pandas as pd

from autotrade.analytics.options.cal_ivx import cal_ivx
from autotrade.analytics.options.cal_opt_greek import (
    calculate_option_greeks_for_dates,
)
from autotrade.analytics.options.opt_forward_curve import (
    build_forward_curves_by_date,
)
from autotrade.data.ricequant.base import FetchMode, FetchStatus
from autotrade.data.ricequant.service.futures import FuturePriceService
from autotrade.data.ricequant.service.index import IndexPriceService
from autotrade.data.ricequant.service.options import (
    OptionInstrumentService,
    OptionPriceService,
)
from autotrade.data.ricequant.spec.calculated_options import (
    CalculatedOptionGreeksSpec,
    CalculatedOptionIVXSpec,
)


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
        underlying_price_service=None,
    ):
        self.spec = spec or CalculatedOptionGreeksSpec()
        self._option_instrument_service = option_instrument_service
        self._option_price_service = option_price_service
        self._future_price_service = future_price_service
        self._underlying_price_service = underlying_price_service

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

    @property
    def underlying_price_service(self):
        if self._underlying_price_service is None:
            # IndexPriceService and ETFPriceService both delegate SOURCE_ONLY
            # to rqdatac.get_price; this service is used as the generic
            # non-futures underlying price adapter.
            self._underlying_price_service = IndexPriceService()
        return self._underlying_price_service

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
        greek_input = calculation_input[
            [
                "order_book_id", "date", "option_price", "forward_price",
                "strike_price", "t_days", "risk_free_rate", "option_type",
            ]
        ].rename(
            columns={
                "option_price": "close",
                "t_days": "T_days",
                "risk_free_rate": "r",
            }
        )
        greek_df = calculate_option_greeks_for_dates(
            greek_input,
            n_jobs=1,
            show_progress=False,
        )
        calculated = calculation_input.merge(
            greek_df,
            on=["order_book_id", "date"],
            how="left",
            validate="one_to_one",
        )

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
            return self._attach_parity_forward(result, filters)

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

    def _attach_parity_forward(
        self,
        panel: pd.DataFrame,
        filters: dict,
    ) -> pd.DataFrame:
        result = panel.copy()
        underlying_ids = (
            result["underlying_order_book_id"].dropna().astype(str).unique().tolist()
        )
        underlying_prices = _successful_data(
            self.underlying_price_service.get(
                mode=FetchMode.SOURCE_ONLY,
                persist=False,
                order_book_ids=underlying_ids,
                start_date=filters["start_date"],
                end_date=filters["end_date"],
                frequency=filters["frequency"],
                fields=["close"],
                market=filters["market"],
            ),
            "underlying prices",
        )
        underlying_prices = underlying_prices[
            ["order_book_id", "date", "close"]
        ].rename(
            columns={
                "order_book_id": "underlying_order_book_id",
                "close": "underlying_price",
            }
        )
        underlying_prices["date"] = pd.to_datetime(underlying_prices["date"])
        result = result.merge(
            underlying_prices,
            on=["underlying_order_book_id", "date"],
            how="left",
            validate="many_to_one",
        )

        forward_input = result[
            [
                "date", "option_price", "t_days", "strike_price", "option_type",
                "risk_free_rate", "underlying_price", "volume",
            ]
        ].copy()
        forward_input.columns = [
            "trade_date", "price", "T_days", "K", "flag", "r",
            "underlying_price", "volume",
        ]
        forward_result = build_forward_curves_by_date(
            forward_input,
            date_col="trade_date",
            mode="implied_forward",
            weight_col="volume",
            robust_method="weighted_mean",
            min_pairs=1,
            max_rel_dispersion=None,
            fallback_to_spot=True,
            fill_missing=True,
            n_jobs=1,
            show_progress=False,
        )
        forward = forward_result.maturity_panel[
            ["trade_date", "T_days", "F_final"]
        ].rename(
            columns={
                "trade_date": "date",
                "T_days": "t_days",
                "F_final": "forward_price",
            }
        )
        forward["date"] = pd.to_datetime(forward["date"])
        result = result.merge(forward, on=["date", "t_days"], how="left")
        result["forward_method"] = "cfutures_implied_weighted_mean"
        return result


class CalculatedOptionIVXDataSource:
    """Calculate one daily IVX value from a complete option-symbol panel."""

    def __init__(
        self,
        spec: CalculatedOptionIVXSpec | None = None,
        *,
        option_instrument_service=None,
        option_price_service=None,
    ):
        self.spec = spec or CalculatedOptionIVXSpec()
        self._option_instrument_service = option_instrument_service
        self._option_price_service = option_price_service

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

    def fetch(self, **filters) -> pd.DataFrame:
        filters = self.spec.fill_default_filters(
            self.spec.normalize_query_filters(
                {key: value for key, value in filters.items() if value is not None}
            )
        )
        self.spec.validate_filters(filters, FetchMode.SOURCE_ONLY)

        instruments = _successful_data(
            self.option_instrument_service.get(
                mode=FetchMode.SOURCE_ONLY,
                persist=False,
                market=filters["market"],
            ),
            "option instruments",
        )
        instruments = self._resolve_symbol_universe(instruments, filters)
        if instruments.empty:
            return self.spec.normalize_df(pd.DataFrame(), filters)

        option_ids = instruments["order_book_id"].astype(str).unique().tolist()
        prices = _successful_data(
            self.option_price_service.get(
                mode=FetchMode.SOURCE_ONLY,
                persist=False,
                order_book_ids=option_ids,
                start_date=filters["start_date"],
                end_date=filters["end_date"],
                frequency=filters["frequency"],
                fields=["close"],
                market=filters["market"],
            ),
            "option prices",
        )
        if prices.empty:
            return self.spec.normalize_df(pd.DataFrame(), filters)

        panel = prices[["order_book_id", "date", filters["price_type"]]].merge(
            instruments[
                [
                    "order_book_id", "maturity_date", "strike_price",
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
        panel["option_price"] = pd.to_numeric(
            panel[filters["price_type"]], errors="coerce"
        )
        panel["risk_free_rate"] = float(filters["risk_free_rate"])

        ivx_input = panel[
            [
                "date", "option_price", "t_days", "strike_price", "option_type",
                "risk_free_rate",
            ]
        ].copy()
        ivx_input.columns = ["date", "price", "T_days", "K", "flag", "r"]
        option_count = ivx_input.groupby("date").size().rename("option_count")
        ivx = cal_ivx(ivx_input, n_jobs=1, show_progress=False)
        ivx.index.name = "date"
        result = ivx.rename("ivx").to_frame().join(option_count).reset_index()
        result["near_t_days"] = None
        result["next_t_days"] = None
        result["near_variance"] = None
        result["next_variance"] = None
        result["opt_symbol"] = str(filters["opt_symbol"])
        result["target_days"] = int(filters["target_days"])
        result["min_days"] = int(filters["min_days"])
        result["risk_free_rate"] = float(filters["risk_free_rate"])
        result["method"] = filters["method"]
        result["price_type"] = filters["price_type"]
        result["frequency"] = filters["frequency"]
        result["market"] = filters["market"]
        result["model_version"] = filters["model_version"]
        return self.spec.normalize_df(result, filters)

    @staticmethod
    def _resolve_symbol_universe(
        instruments: pd.DataFrame,
        filters: dict,
    ) -> pd.DataFrame:
        symbol = str(filters["opt_symbol"])
        values = instruments["underlying_symbol"].astype(str)
        result = instruments[
            values.eq(symbol) | values.str.split(".").str[0].eq(symbol)
        ].copy()
        start = pd.to_datetime(filters["start_date"])
        end = pd.to_datetime(filters["end_date"])
        listed = pd.to_datetime(result["listed_date"], errors="coerce")
        maturity = pd.to_datetime(result["maturity_date"], errors="coerce")
        return result[(listed <= end) & (maturity >= start)].copy()
