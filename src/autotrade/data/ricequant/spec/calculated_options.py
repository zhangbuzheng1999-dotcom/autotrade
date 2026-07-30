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
    TABLE = "calculated_option_greeks_1d"

    API_PARAMS = {
        "opt_symbol", "order_book_ids", "start_date", "end_date", "frequency",
        "market", "risk_free_rate", "price_type", "model_id", "model_version",
    }
    DB_QUERY_FIELDS = API_PARAMS | {
        "order_book_id", "date", "underlying_order_book_id", "maturity_date",
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
        "model_version": "autotrade_v1",
    }
    COLUMNS = [
        "order_book_id", "date", "opt_symbol", "underlying_order_book_id",
        "maturity_date", "strike_price", "option_type", "option_price",
        "forward_price", "risk_free_rate", "t_days", "iv", "delta", "gamma",
        "vega", "theta", "rho", "vanna", "vomma", "charm", "forward_method",
        "price_type", "frequency", "market", "model_id", "model_version",
    ]

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
        if filters.get("frequency") != "1d":
            raise ValueError("calculated_option_greeks currently supports frequency='1d' only")
        if filters.get("price_type") != "close":
            raise ValueError("calculated_option_greeks currently supports price_type='close' only")
        if mode != FetchMode.DB_ONLY and not (
            filters.get("opt_symbol") or filters.get("order_book_ids")
        ):
            raise ValueError(
                "SOURCE_ONLY requires opt_symbol or order_book_ids to resolve calculation scope"
            )

    def resolve_db_filter_specs(self, filters: dict[str, Any]) -> dict[str, dict[str, Any]]:
        return {
            "order_book_id": {"column": "order_book_id", "op": "eq"},
            "order_book_ids": {"column": "order_book_id", "op": "in"},
            "opt_symbol": {"column": "opt_symbol", "op": "eq"},
            "start_date": {"column": "date", "op": "gte"},
            "end_date": {"column": "date", "op": "lte"},
            "date": {"column": "date", "op": "eq"},
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
        for key in ("start_date", "end_date", "date", "maturity_date"):
            if key in result:
                result[key] = pd.to_datetime(result[key]).date()
        return result

    def normalize_df(self, df: pd.DataFrame, filters=None) -> pd.DataFrame:
        result = pd.DataFrame() if df is None else pd.DataFrame(df).copy()
        for column in self.COLUMNS:
            if column not in result:
                result[column] = None
        for column in ("date", "maturity_date"):
            result[column] = pd.to_datetime(result[column], errors="coerce").dt.date
        return result[self.COLUMNS]

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
        "model_version": "autotrade_v1",
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
