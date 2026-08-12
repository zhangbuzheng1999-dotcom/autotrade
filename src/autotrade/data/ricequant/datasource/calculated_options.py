from __future__ import annotations

import pandas as pd

from autotrade.option.analytics.ivx import cal_ivx
from autotrade.option.analytics.greeks import (
    calculate_option_greeks_for_dates,
)
from autotrade.option.analytics.forward_curve import (
    build_forward_curves_by_date,
)
from autotrade.data.ricequant.base import FetchMode, FetchStatus
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
        raise RuntimeError(f"{resource} fetch failed") from result.error
    return pd.DataFrame() if result.data is None else result.data.copy()


def _normalize_input_mode(value) -> FetchMode:
    """Normalize the storage/source choice for calculated-resource inputs."""
    if value is None:
        return FetchMode.SOURCE_ONLY
    if isinstance(value, FetchMode):
        mode = value
    else:
        try:
            mode = FetchMode(str(value).lower())
        except ValueError as exc:
            raise ValueError(
                "input_mode must be FetchMode.DB_ONLY or FetchMode.SOURCE_ONLY"
            ) from exc
    if mode not in {FetchMode.DB_ONLY, FetchMode.SOURCE_ONLY}:
        raise ValueError(
            "input_mode must be FetchMode.DB_ONLY or FetchMode.SOURCE_ONLY"
        )
    return mode


class CalculatedOptionGreeksDataSource:
    """Build a complete option-symbol cross-section from selectable inputs."""

    def __init__(
        self,
        spec: CalculatedOptionGreeksSpec | None = None,
        *,
        option_instrument_service=None,
        option_price_service=None,
    ):
        self.spec = spec or CalculatedOptionGreeksSpec()
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
        input_mode = _normalize_input_mode(filters.pop("input_mode", None))
        filters = self.spec.fill_default_filters(
            self.spec.normalize_query_filters(
                {key: value for key, value in filters.items() if value is not None}
            )
        )
        self.spec.validate_filters(filters, FetchMode.SOURCE_ONLY)

        # Instruments and prices always use the same input mode so a calculation
        # cannot accidentally mix DB metadata with live source prices.
        instruments = _successful_data(
            self.option_instrument_service.get(
                mode=input_mode,
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
                mode=input_mode,
                persist=False,
                order_book_ids=option_ids,
                start_date=filters["start_date"],
                end_date=filters["end_date"],
                frequency=filters["frequency"],
                time_slice=filters.get("time_slice"),
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

        # DB_ONLY repositories return the complete stored row, including fields
        # such as strike_price that are also owned by the instrument snapshot.
        # Keep only market columns here to avoid merge suffixes and make DB and
        # source inputs expose the same calculation schema.
        market_columns = [
            "order_book_id", "date", "datetime", "trading_date",
            "open", "close", "high", "low",
            "total_turnover", "volume", "open_interest", "strike_price",
        ]
        option_prices = option_prices[
            [column for column in market_columns if column in option_prices.columns]
        ].copy()
        panel = option_prices.merge(
            instruments[
                [
                    "order_book_id", "underlying_order_book_id",
                    "underlying_symbol", "maturity_date", "strike_price",
                    "option_type",
                ]
            ].rename(columns={"strike_price": "instrument_strike_price"}),
            on="order_book_id",
            how="left",
            validate="many_to_one",
        )
        if "strike_price" not in panel:
            panel["strike_price"] = panel["instrument_strike_price"]
        else:
            panel["strike_price"] = panel["strike_price"].fillna(
                panel["instrument_strike_price"]
            )
        panel = panel.drop(columns="instrument_strike_price")
        minute = filters["frequency"] == "1m"
        observation_col = "datetime" if minute else "date"
        panel[observation_col] = pd.to_datetime(panel[observation_col])
        if minute:
            if "trading_date" not in panel:
                panel["trading_date"] = panel["datetime"].dt.date
            panel["trading_date"] = pd.to_datetime(panel["trading_date"])
            maturity_base = panel["trading_date"]
        else:
            maturity_base = panel["date"]
        panel["maturity_date"] = pd.to_datetime(panel["maturity_date"])
        panel["t_days"] = (panel["maturity_date"] - maturity_base).dt.days
        panel["risk_free_rate"] = float(filters["risk_free_rate"])
        panel["option_price"] = pd.to_numeric(panel[filters["price_type"]], errors="coerce")
        panel["opt_symbol"] = panel["underlying_symbol"].astype(str)

        pieces = []
        for opt_symbol, symbol_panel in panel.groupby("opt_symbol", sort=False):
            pieces.append(
                self._attach_parity_forward(
                    symbol_panel,
                    observation_col=observation_col,
                )
            )
        calculation_input = pd.concat(pieces, ignore_index=True) if pieces else panel
        greek_input = calculation_input[
            [
                "order_book_id", observation_col, "option_price", "forward_price",
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
            date_col=observation_col,
            n_jobs=1,
            show_progress=False,
        )
        if minute:
            greek_df = greek_df.rename(columns={"date": "datetime"})
        calculated = calculation_input.merge(
            greek_df,
            on=["order_book_id", observation_col],
            how="left",
            validate="one_to_one",
        )

        if minute:
            calculated["datetime"] = pd.to_datetime(calculated["datetime"])
            calculated["trading_date"] = pd.to_datetime(
                calculated["trading_date"]
            ).dt.date
        else:
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

    @staticmethod
    def _attach_parity_forward(
        panel: pd.DataFrame,
        *,
        observation_col: str = "date",
    ) -> pd.DataFrame:
        result = panel.copy()
        forward_input = result[
            [
                observation_col, "option_price", "t_days", "strike_price", "option_type",
                "risk_free_rate", "volume",
            ]
        ].copy()
        forward_input.columns = [
            "observation_time", "price", "T_days", "K", "flag", "r", "volume",
        ]
        forward_result = build_forward_curves_by_date(
            forward_input,
            date_col="observation_time",
            mode="implied_forward",
            weight_col="volume",
            robust_method="weighted_mean",
            min_pairs=1,
            max_rel_dispersion=None,
            fallback_to_spot=False,
            fill_missing=True,
            n_jobs=1,
            show_progress=False,
        )
        forward = forward_result.maturity_panel[
            ["trade_date", "T_days", "F_final"]
        ].rename(
            columns={
                "trade_date": observation_col,
                "T_days": "t_days",
                "F_final": "forward_price",
            }
        )
        forward[observation_col] = pd.to_datetime(forward[observation_col])
        result = result.merge(
            forward,
            on=[observation_col, "t_days"],
            how="left",
            validate="many_to_one",
        )
        result["forward_method"] = "put_call_parity_weighted_mean"
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
