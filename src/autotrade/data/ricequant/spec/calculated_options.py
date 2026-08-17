from __future__ import annotations

from typing import Any

import pandas as pd

from autotrade.data.ricequant.base import BaseRQSpec, FetchMode


class CalculatedOptionGreeksSpec(BaseRQSpec):
    RESOURCE_NAME = "calculated_option_greeks"
    RESOURCE_TYPE = "timeseries"
    STORAGE_BACKEND = "clickhouse"
    WRITE_MODE = "timeseries_append"
    DATABASE = "rq_option_data"
    TABLE_PREFIX = "calculated_option_greeks"
    SUPPORTED_FREQUENCIES = {"1d", "1m"}

    API_PARAMS = {
        "opt_symbol", "order_book_ids", "start_date", "end_date", "frequency",
        "market", "risk_free_rate", "price_type", "model_id", "model_version",
        "input_mode", "time_slice",
    }
    DB_QUERY_FIELDS = API_PARAMS | {
        "order_book_id", "date", "datetime", "trading_date",
        "underlying_order_book_id", "maturity_date",
        "option_type", "forward_method",
    }
    API_REQUIRED_FILTERS = {"start_date", "end_date"}
    DB_REQUIRED_FILTERS = set()
    DEFAULT_FILTERS = {
        "frequency": "1d",
        "market": "cn",
        "risk_free_rate": 0.03,
        "price_type": "close",
        "model_id": "black97",
        "model_version": "parity_v1",
    }
    DAILY_COLUMNS = [
        "order_book_id", "date", "opt_symbol", "underlying_order_book_id",
        "maturity_date", "strike_price", "option_type", "option_price",
        "forward_price", "risk_free_rate", "t_days", "iv", "delta", "gamma",
        "vega", "theta", "rho", "vanna", "vomma", "charm", "forward_method",
        "price_type", "frequency", "market", "model_id", "model_version",
    ]
    MINUTE_COLUMNS = [
        "order_book_id", "datetime", "trading_date", "opt_symbol",
        "underlying_order_book_id", "maturity_date", "strike_price",
        "option_type", "option_price", "forward_price", "risk_free_rate",
        "t_days", "iv", "delta", "gamma", "vega", "theta", "rho",
        "vanna", "vomma", "charm", "forward_method", "price_type",
        "frequency", "market", "model_id", "model_version",
    ]
    COLUMNS = DAILY_COLUMNS

    def normalize_query_filters(self, filters: dict[str, Any]) -> dict[str, Any]:
        result = dict(filters)
        if "order_book_id" in result and "order_book_ids" not in result:
            result["order_book_ids"] = [result.pop("order_book_id")]
        if "order_book_ids" in result:
            value = result["order_book_ids"]
            result["order_book_ids"] = [value] if isinstance(value, str) else list(value)
        return result

    def validate_filters(self, filters: dict[str, Any], mode: FetchMode) -> None:
        super().validate_filters(filters, mode)
        frequency = filters.get("frequency")
        if frequency not in self.SUPPORTED_FREQUENCIES:
            raise ValueError(
                "calculated_option_greeks supports frequency in {'1d', '1m'}"
            )
        time_slice = filters.get("time_slice")
        if time_slice is not None:
            if frequency != "1m":
                raise ValueError(
                    "calculated_option_greeks time_slice requires frequency='1m'"
                )
            if not isinstance(time_slice, (list, tuple)) or len(time_slice) != 2:
                raise ValueError("time_slice must be a (start_time, end_time) pair")
            start_time, end_time = (
                self._normalize_time_value(value) for value in time_slice
            )
            if start_time > end_time:
                raise ValueError("time_slice start_time must be <= end_time")
        if filters.get("price_type") != "close":
            raise ValueError("calculated_option_greeks currently supports price_type='close' only")
        if mode != FetchMode.DB_ONLY and not (
            filters.get("opt_symbol") or filters.get("order_book_ids")
        ):
            raise ValueError(
                "SOURCE_ONLY requires opt_symbol or order_book_ids to resolve calculation scope"
            )

    def resolve_table(self, filters: dict[str, Any]) -> str:
        frequency = filters.get("frequency")
        if frequency not in self.SUPPORTED_FREQUENCIES:
            raise ValueError(f"unsupported calculated Greeks frequency={frequency}")
        return f"{self.TABLE_PREFIX}_{frequency}"

    def resolve_db_filter_specs(self, filters: dict[str, Any]) -> dict[str, dict[str, Any]]:
        minute = filters.get("frequency") == "1m"
        time_col = "trading_date" if minute else "date"
        return {
            "order_book_id": {"column": "order_book_id", "op": "eq"},
            "order_book_ids": {"column": "order_book_id", "op": "in"},
            "opt_symbol": {"column": "opt_symbol", "op": "eq"},
            "start_date": {"column": time_col, "op": "gte"},
            "end_date": {"column": time_col, "op": "lte"},
            "date": {"column": "date", "op": "eq"},
            "datetime": {"column": "datetime", "op": "eq"},
            "trading_date": {"column": "trading_date", "op": "eq"},
            "time_slice": {"column": "datetime", "op": "time_between"},
            "_datetime_scan_start": {"column": "datetime", "op": "gte"},
            "_datetime_scan_end": {"column": "datetime", "op": "lt"},
            "_datetime_time_intervals": {
                "column": "datetime", "op": "datetime_intervals"
            },
            "frequency": {"column": "frequency", "op": "eq"},
            "market": {"column": "market", "op": "eq"},
            "price_type": {"column": "price_type", "op": "eq"},
            "model_id": {"column": "model_id", "op": "eq"},
            "model_version": {"column": "model_version", "op": "eq"},
            "underlying_order_book_id": {"column": "underlying_order_book_id", "op": "eq"},
            "maturity_date": {"column": "maturity_date", "op": "eq"},
            "option_type": {"column": "option_type", "op": "eq"},
            "forward_method": {"column": "forward_method", "op": "eq"},
        }

    def normalize_db_query_filters(self, filters: dict[str, Any]) -> dict[str, Any]:
        result = dict(filters)
        for key in ("start_date", "end_date", "date", "trading_date", "maturity_date"):
            if key in result:
                result[key] = pd.to_datetime(result[key]).date()
        if "datetime" in result:
            result["datetime"] = pd.to_datetime(result["datetime"])
        if result.get("frequency") == "1m":
            if "start_date" in result:
                result["_datetime_scan_start"] = (
                    pd.Timestamp(result["start_date"]) - pd.Timedelta(days=1)
                )
            if "end_date" in result:
                result["_datetime_scan_end"] = (
                    pd.Timestamp(result["end_date"]) + pd.Timedelta(days=1)
                )
            if "time_slice" in result:
                result["time_slice"] = tuple(
                    self._normalize_time_value(value).strftime("%H:%M:%S")
                    for value in result["time_slice"]
                )
                if "_datetime_scan_start" in result and "_datetime_scan_end" in result:
                    start_clock, end_clock = result["time_slice"]
                    calendar_days = pd.date_range(
                        result["_datetime_scan_start"].normalize(),
                        result["_datetime_scan_end"].normalize() - pd.Timedelta(days=1),
                        freq="D",
                    )
                    result["_datetime_time_intervals"] = [
                        (
                            pd.Timestamp(f"{day.date()} {start_clock}"),
                            pd.Timestamp(f"{day.date()} {end_clock}"),
                        )
                        for day in calendar_days
                    ]
                    result.pop("_datetime_scan_start", None)
                    result.pop("_datetime_scan_end", None)
        return result

    def normalize_df(self, df: pd.DataFrame, filters=None) -> pd.DataFrame:
        result = pd.DataFrame() if df is None else pd.DataFrame(df).copy()
        frequency = (filters or {}).get("frequency", "1d")
        columns = self.MINUTE_COLUMNS if frequency == "1m" else self.DAILY_COLUMNS
        for column in columns:
            if column not in result:
                result[column] = None
        result["maturity_date"] = pd.to_datetime(
            result["maturity_date"], errors="coerce"
        ).dt.date
        if frequency == "1m":
            result["datetime"] = pd.to_datetime(result["datetime"], errors="coerce")
            result["trading_date"] = pd.to_datetime(
                result["trading_date"], errors="coerce"
            ).dt.date
        else:
            result["date"] = pd.to_datetime(result["date"], errors="coerce").dt.date
        return result[columns]

    @staticmethod
    def _normalize_time_value(value):
        try:
            return pd.to_datetime(str(value)).time()
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid calculated Greeks time value: {value!r}") from exc

    def split_filters(self, filters):
        return dict(filters), dict(filters)


class CalculatedOptionIVXSpec(BaseRQSpec):
    RESOURCE_NAME = "calculated_option_ivx"
    RESOURCE_TYPE = "timeseries"
    STORAGE_BACKEND = "clickhouse"
    WRITE_MODE = "timeseries_append"
    DATABASE = "rq_option_data"
    TABLE = "calculated_option_ivx_1d"

    API_PARAMS = {
        "opt_symbol", "start_date", "end_date", "frequency", "market",
        "risk_free_rate", "price_type", "target_days", "min_days", "method",
        "model_version",
    }
    DB_QUERY_FIELDS = API_PARAMS | {"date"}
    API_REQUIRED_FILTERS = {"opt_symbol", "start_date", "end_date"}
    DB_REQUIRED_FILTERS = set()
    DEFAULT_FILTERS = {
        "frequency": "1d",
        "market": "cn",
        "risk_free_rate": 0.03,
        "price_type": "close",
        "target_days": 30,
        "min_days": 7,
        "method": "model_free_variance",
        "model_version": "cfutures_v1",
    }
    COLUMNS = [
        "date", "opt_symbol", "ivx", "target_days", "min_days",
        "near_t_days", "next_t_days", "near_variance", "next_variance",
        "option_count", "risk_free_rate", "method", "price_type", "frequency",
        "market", "model_version",
    ]

    def validate_filters(self, filters: dict[str, Any], mode: FetchMode) -> None:
        super().validate_filters(filters, mode)
        if filters.get("frequency") != "1d":
            raise ValueError("calculated_option_ivx currently supports frequency='1d' only")
        if filters.get("price_type") != "close":
            raise ValueError("calculated_option_ivx currently supports price_type='close' only")
        if filters.get("method") != "model_free_variance":
            raise ValueError(
                "calculated_option_ivx currently supports method='model_free_variance' only"
            )
        if int(filters["target_days"]) <= 0 or int(filters["min_days"]) < 0:
            raise ValueError("target_days must be positive and min_days must be non-negative")
        if int(filters["target_days"]) != 30 or int(filters["min_days"]) != 7:
            raise ValueError(
                "cfutures-compatible IVX requires target_days=30 and min_days=7"
            )

    def resolve_db_filter_specs(self, filters: dict[str, Any]) -> dict[str, dict[str, Any]]:
        return {
            "opt_symbol": {"column": "opt_symbol", "op": "eq"},
            "start_date": {"column": "date", "op": "gte"},
            "end_date": {"column": "date", "op": "lte"},
            "date": {"column": "date", "op": "eq"},
            "frequency": {"column": "frequency", "op": "eq"},
            "market": {"column": "market", "op": "eq"},
            "price_type": {"column": "price_type", "op": "eq"},
            "target_days": {"column": "target_days", "op": "eq"},
            "min_days": {"column": "min_days", "op": "eq"},
            "risk_free_rate": {"column": "risk_free_rate", "op": "eq"},
            "method": {"column": "method", "op": "eq"},
            "model_version": {"column": "model_version", "op": "eq"},
        }

    def normalize_db_query_filters(self, filters: dict[str, Any]) -> dict[str, Any]:
        result = dict(filters)
        for key in ("start_date", "end_date", "date"):
            if key in result:
                result[key] = pd.to_datetime(result[key]).date()
        return result

    def normalize_df(self, df: pd.DataFrame, filters=None) -> pd.DataFrame:
        result = pd.DataFrame() if df is None else pd.DataFrame(df).copy()
        for column in self.COLUMNS:
            if column not in result:
                result[column] = None
        result["date"] = pd.to_datetime(result["date"], errors="coerce").dt.date
        return result[self.COLUMNS]

    def split_filters(self, filters):
        return dict(filters), dict(filters)
