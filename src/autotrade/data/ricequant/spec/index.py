# autotrade/data/ricequant/spec/index.py

from __future__ import annotations

from typing import Any

import pandas as pd

from autotrade.data.ricequant.base import BaseRQSpec, FetchMode


class IndexInstrumentSpec(BaseRQSpec):
    """
    all_instruments(type='INDX')

    API层固定：
        type = 'INDX'

    SOURCE支持：
        - date
        - market

    DB支持：
        - order_book_id
        - symbol
        - industry_code
        - industry_name
        - board_type
        - exchange
        - status
        - special_type
        - market
        - date   -> 某日可交易合约语义
    """

    RESOURCE_NAME = "index_instruments"
    RESOURCE_TYPE = "snapshot"

    DATABASE = "rq_index_data"
    TABLE = "index_instruments"

    PRIMARY_KEYS = ["order_book_id"]

    API_PARAMS = {
        "date",
        "market",
    }

    API_REQUIRED_FILTERS = set()

    DB_QUERY_FIELDS = {
        "order_book_id",
        "symbol",
        "industry_code",
        "industry_name",
        "board_type",
        "exchange",
        "status",
        "special_type",
        "market",
        "date",
    }

    DB_REQUIRED_FILTERS = set()

    DEFAULT_FILTERS = {
        "market": "cn",
    }

    DATE_FIELDS = {
        "date",
        "listed_date",
        "de_listed_date",
        "purchasedate",
        "base_date",
    }

    CODE_FIELDS = {
        "order_book_id",
        "underlying_order_book_id",
    }

    COLUMNS = [
        "order_book_id",
        "symbol",
        "abbrev_symbol",
        "round_lot",
        "sector_code",
        "sector_code_name",
        "industry_code",
        "industry_name",
        "listed_date",
        "issue_price",
        "de_listed_date",
        "type",
        "underlying_order_book_id",
        "underlying_name",
        "concept_names",
        "exchange",
        "board_type",
        "status",
        "special_type",
        "trading_hours",
        "least_redeem",
        "cross_market",
        "market_tplus",
        "purchasedate",
        "base_date",
        "base_point",
        "market",
    ]

    FIXED_TYPE = "INDX"

    def normalize_query_filters(self, filters: dict[str, Any]) -> dict[str, Any]:
        result = dict(filters)
        result.pop("type", None)
        return result

    def validate_filters(
        self,
        filters: dict[str, Any],
        mode: FetchMode,
    ) -> None:
        super().validate_filters(filters, mode)

        market = filters.get("market")
        if market not in {"cn", "hk"}:
            raise ValueError("index_instruments market must be one of {'cn', 'hk'}")

    def resolve_database(self, filters: dict[str, Any]) -> str:
        return "rq_index_data"

    def resolve_table(self, filters: dict[str, Any]) -> str:
        return "index_instruments"

    def resolve_db_filter_specs(self, filters: dict[str, Any]) -> dict[str, dict[str, Any]]:
        return {
            "order_book_id": {"column": "order_book_id", "op": "eq"},
            "symbol": {"column": "symbol", "op": "eq"},
            "industry_code": {"column": "industry_code", "op": "eq"},
            "industry_name": {"column": "industry_name", "op": "eq"},
            "board_type": {"column": "board_type", "op": "eq"},
            "exchange": {"column": "exchange", "op": "eq"},
            "status": {"column": "status", "op": "eq"},
            "special_type": {"column": "special_type", "op": "eq"},
            "market": {"column": "market", "op": "eq"},

            # date 表达某日可交易
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

        for col in self.COLUMNS:
            if col not in result.columns:
                result[col] = None

        for col in [
            "listed_date",
            "de_listed_date",
            "purchasedate",
            "base_date",
        ]:
            if col in result.columns:
                s = pd.to_datetime(result[col], errors="coerce")
                result[col] = s.dt.date
                result[col] = result[col].where(s.notna(), None)

        result["type"] = self.FIXED_TYPE
        result["market"] = (filters or {}).get("market", "cn")

        if "order_book_id" not in result.columns:
            raise ValueError("index_instruments dataframe missing order_book_id column")

        if result["order_book_id"].isna().any():
            bad_rows = result[result["order_book_id"].isna()]
            raise ValueError(
                f"index_instruments dataframe contains null order_book_id rows: {len(bad_rows)}"
            )

        return result[self.COLUMNS]

    def filter_df(
        self,
        df: pd.DataFrame,
        filters: dict[str, Any],
    ) -> pd.DataFrame:
        if df.empty:
            return df

        result = df.copy()

        for col in ["listed_date", "de_listed_date", "purchasedate", "base_date"]:
            if col in result.columns:
                result[col] = pd.to_datetime(result[col], errors="coerce").dt.date

        date_value = filters.get("date")
        if date_value is not None:
            d = pd.to_datetime(date_value).date()

            if "listed_date" in result.columns:
                result = result[result["listed_date"].isna() | (result["listed_date"] <= d)]

            if "de_listed_date" in result.columns:
                result = result[result["de_listed_date"].isna() | (result["de_listed_date"] >= d)]

        normalized_filters = dict(filters)
        normalized_filters.pop("date", None)

        return super().filter_df(result, normalized_filters)
