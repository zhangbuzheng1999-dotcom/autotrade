# autotrade/data/ricequant/spec/futures.py

from __future__ import annotations

from typing import Any

import pandas as pd

from autotrade.data.ricequant.base import BaseRQSpec, FetchMode


class FutureInstrumentSpec(BaseRQSpec):
    """
    all_instruments(type='Future')

    API层固定：
        type = 'Future'

    SOURCE支持：
        - date
        - market

    DB支持：
        - order_book_id
        - symbol
        - start_date   -> listed_date >=
        - end_date     -> de_listed_date <=
        - industry_name
        - trading_code
        - underlying_order_book_id
        - underlying_symbol
        - exchange
        - product
        - date         -> 某日可交易合约语义
    """

    RESOURCE_NAME = "future_instruments"
    RESOURCE_TYPE = "snapshot"

    DATABASE = "rq_future_data"
    TABLE = "future_instruments"

    PRIMARY_KEYS = ["order_book_id"]

    API_PARAMS = {
        "date",
        "market",
    }

    API_REQUIRED_FILTERS = set()

    DB_QUERY_FIELDS = {
        "order_book_id",
        "symbol",
        "start_date",
        "end_date",
        "date",
        "market",
        "industry_name",
        "trading_code",
        "underlying_order_book_id",
        "underlying_symbol",
        "exchange",
        "product",
    }

    DB_REQUIRED_FILTERS = set()

    DEFAULT_FILTERS = {
        "market": "cn",
    }

    DATE_FIELDS = {
        "date",
        "start_date",
        "end_date",
        "listed_date",
        "de_listed_date",
        "maturity_date",
        "start_delivery_date",
        "end_delivery_date",
    }

    CODE_FIELDS = {
        "order_book_id",
        "underlying_order_book_id",
    }

    COLUMNS = [
        "order_book_id",
        "symbol",
        "margin_rate",
        "round_lot",
        "listed_date",
        "de_listed_date",
        "industry_name",
        "trading_code",
        "market_tplus",
        "type",
        "contract_multiplier",
        "underlying_order_book_id",
        "underlying_symbol",
        "maturity_date",
        "exchange",
        "trading_hours",
        "product",
        "start_delivery_date",
        "end_delivery_date",
        "market",
    ]

    FIXED_TYPE = "Future"

    def normalize_query_filters(self, filters: dict[str, Any]) -> dict[str, Any]:
        result = dict(filters)

        # 不允许外部再传 type；统一固定为 Future
        result.pop("type", None)

        return result

    def fill_default_filters(self, filters: dict[str, Any]) -> dict[str, Any]:
        merged = super().fill_default_filters(filters)
        return merged

    def validate_filters(
        self,
        filters: dict[str, Any],
        mode: FetchMode,
    ) -> None:
        super().validate_filters(filters, mode)

        market = filters.get("market")
        if market not in {"cn", "hk"}:
            raise ValueError("future_instruments market must be one of {'cn', 'hk'}")

    def resolve_database(self, filters: dict[str, Any]) -> str:
        return "rq_future_data"

    def resolve_table(self, filters: dict[str, Any]) -> str:
        return "future_instruments"

    def resolve_db_filter_specs(self, filters: dict[str, Any]) -> dict[str, dict[str, Any]]:
        return {
            "order_book_id": {"column": "order_book_id", "op": "eq"},
            "symbol": {"column": "symbol", "op": "eq"},
            "market": {"column": "market", "op": "eq"},
            "industry_name": {"column": "industry_name", "op": "eq"},
            "trading_code": {"column": "trading_code", "op": "eq"},
            "underlying_order_book_id": {"column": "underlying_order_book_id", "op": "eq"},
            "underlying_symbol": {"column": "underlying_symbol", "op": "eq"},
            "exchange": {"column": "exchange", "op": "eq"},
            "product": {"column": "product", "op": "eq"},

            # 上市区间语义
            "start_date": {"column": "listed_date", "op": "gte"},
            "end_date": {"column": "de_listed_date", "op": "lte"},

            # 某天可交易合约语义：
            # listed_date <= date AND de_listed_date >= date
            # 这里在 filter_df 里专门处理
            "date": {"column": "listed_date", "op": "lte"},
        }

    def split_filters(
        self,
        filters: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        api_filters = {}
        post_filters = {}

        for k, v in filters.items():
            if k in self.API_PARAMS:
                api_filters[k] = v

        # source 后置过滤沿用 db 语义字段
        db_specs = self.resolve_db_filter_specs(filters)
        for k, v in filters.items():
            if k in db_specs or k == "date":
                post_filters[k] = v

        return api_filters, post_filters

    def normalize_df(
        self,
        df: pd.DataFrame,
        filters: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        if df is None:
            return pd.DataFrame()

        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)

        if isinstance(df.index, pd.MultiIndex):
            df = df.reset_index()
        elif df.index.name is not None:
            df = df.reset_index()

        result = df.copy()

        # 确保所有标准列存在
        for col in self.COLUMNS:
            if col not in result.columns:
                result[col] = None

        for col in [
            "listed_date",
            "de_listed_date",
            "maturity_date",
            "start_delivery_date",
            "end_delivery_date",
        ]:
            if col in result.columns:
                s = pd.to_datetime(result[col], errors="coerce")
                result[col] = s.dt.date
                result[col] = result[col].where(s.notna(), None)

        result["type"] = self.FIXED_TYPE
        result["market"] = (filters or {}).get("market", "cn")

        return result[self.COLUMNS]

    def filter_df(
        self,
        df: pd.DataFrame,
        filters: dict[str, Any],
    ) -> pd.DataFrame:
        if df.empty:
            return df

        result = df.copy()

        for col in [
            "listed_date",
            "de_listed_date",
            "maturity_date",
            "start_delivery_date",
            "end_delivery_date",
        ]:
            if col in result.columns:
                result[col] = pd.to_datetime(result[col], errors="coerce").dt.date

        # 先处理 date 的特殊语义：
        # 指定日期可交易的期货合约
        date_value = filters.get("date")
        if date_value is not None:
            d = pd.to_datetime(date_value).date()

            if "listed_date" in result.columns:
                result = result[result["listed_date"].isna() | (result["listed_date"] <= d)]

            if "de_listed_date" in result.columns:
                result = result[result["de_listed_date"].isna() | (result["de_listed_date"] >= d)]

        normalized_filters = dict(filters)

        if "start_date" in normalized_filters:
            normalized_filters["start_date"] = pd.to_datetime(normalized_filters["start_date"]).date()

        if "end_date" in normalized_filters:
            normalized_filters["end_date"] = pd.to_datetime(normalized_filters["end_date"]).date()

        # date 已经手动处理过，这里去掉避免重复
        normalized_filters.pop("date", None)

        return super().filter_df(result, normalized_filters)
