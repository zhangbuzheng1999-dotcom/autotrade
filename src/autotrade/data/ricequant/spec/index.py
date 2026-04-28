# autotrade/data/ricequant/spec/index.py

from __future__ import annotations

from typing import Any

import pandas as pd

from autotrade.data.ricequant.base import BaseRQSpec, FetchMode


class IndexPriceSpec(BaseRQSpec):
    RESOURCE_NAME = "index_price"
    RESOURCE_TYPE = "timeseries"
    STORAGE_BACKEND = "clickhouse"
    WRITE_MODE = "timeseries_append"
    DATABASE = "rq_index_data"
    TABLE_PREFIX = "index_price"
    FIXED_TYPE = "INDX"

    PRIMARY_KEYS = []
    SUPPORTED_FREQUENCIES = {"1d", "1w", "1m", "5m", "15m", "30m", "60m"}
    MINUTE_FREQUENCIES = {"1m", "5m", "15m", "30m", "60m"}
    DAILY_FREQUENCIES = {"1d", "1w"}
    API_PARAMS = {
        "order_book_ids", "start_date", "end_date", "frequency", "fields",
        "adjust_type", "skip_suspended", "expect_df", "time_slice", "market",
    }
    API_REQUIRED_FILTERS = {"order_book_ids", "frequency"}
    DB_QUERY_FIELDS = {
        "frequency", "market", "order_book_id", "order_book_ids", "date", "datetime",
        "start_date", "end_date", "open", "close", "high", "low", "limit_up",
        "limit_down", "total_turnover", "volume", "num_trades", "prev_close",
        "settlement", "prev_settlement", "open_interest", "dominant_id", "strike_price",
        "contract_multiplier", "iopv", "day_session_open",
    }
    DB_REQUIRED_FILTERS = {"frequency"}
    DATE_FIELDS = {"date", "datetime", "start_date", "end_date"}
    CODE_FIELDS = {"order_book_id", "order_book_ids"}
    DEFAULT_FILTERS = {
        "market": "cn", "adjust_type": "none", "skip_suspended": False,
        "expect_df": True, "frequency": "1d",
    }

    @classmethod
    def is_minute_frequency(cls, frequency: str | None) -> bool:
        return frequency in cls.MINUTE_FREQUENCIES

    @classmethod
    def is_daily_frequency(cls, frequency: str | None) -> bool:
        return frequency in cls.DAILY_FREQUENCIES

    def _normalize_order_book_ids(self, value: Any) -> list[str] | None:
        if value is None:
            return None
        if isinstance(value, str):
            return [value]
        if isinstance(value, tuple):
            return list(value)
        if isinstance(value, list):
            return value
        return list(value)

    def normalize_query_filters(self, filters: dict[str, Any]) -> dict[str, Any]:
        result = dict(filters)
        result.pop("type", None)
        if "order_book_id" in result and "order_book_ids" not in result:
            result["order_book_ids"] = self._normalize_order_book_ids(result["order_book_id"])
        if "order_book_ids" in result:
            result["order_book_ids"] = self._normalize_order_book_ids(result["order_book_ids"])
        return result

    def validate_filters(self, filters: dict[str, Any], mode: FetchMode) -> None:
        super().validate_filters(filters, mode)
        frequency = filters.get("frequency")
        if frequency not in self.SUPPORTED_FREQUENCIES:
            raise ValueError(
                "tick frequency is not supported in current version; "
                "supported frequencies are ['1d', '1w', '1m', '5m', '15m', '30m', '60m']"
            )
        if frequency == "1w" and filters.get("expect_df") is False:
            raise ValueError("weekly price query requires expect_df=True")
        if mode in {FetchMode.SOURCE_ONLY, FetchMode.DB_THEN_SOURCE} and not filters.get("order_book_ids"):
            raise ValueError(f"{self.RESOURCE_NAME} requires order_book_ids for mode={mode.value}")

    def resolve_database(self, filters: dict[str, Any]) -> str:
        return self.DATABASE

    def resolve_table(self, filters: dict[str, Any]) -> str:
        frequency = filters.get("frequency")
        if not frequency:
            raise ValueError(f"{self.RESOURCE_NAME} requires frequency for table routing")
        return f"{self.TABLE_PREFIX}_{frequency}"

    def resolve_db_filter_specs(self, filters: dict[str, Any]) -> dict[str, dict[str, Any]]:
        frequency = filters.get("frequency")
        time_col = "datetime" if self.is_minute_frequency(frequency) else "date"
        return {
            "frequency": {"column": "frequency", "op": "eq"},
            "market": {"column": "market", "op": "eq"},
            "order_book_id": {"column": "order_book_id", "op": "eq"},
            "order_book_ids": {"column": "order_book_id", "op": "in"},
            "start_date": {"column": time_col, "op": "gte"},
            "end_date": {"column": time_col, "op": "lte"},
            "date": {"column": "date", "op": "eq"},
            "datetime": {"column": "datetime", "op": "eq"},
            "open": {"column": "open", "op": "eq"},
            "close": {"column": "close", "op": "eq"},
            "high": {"column": "high", "op": "eq"},
            "low": {"column": "low", "op": "eq"},
            "limit_up": {"column": "limit_up", "op": "eq"},
            "limit_down": {"column": "limit_down", "op": "eq"},
            "total_turnover": {"column": "total_turnover", "op": "eq"},
            "volume": {"column": "volume", "op": "eq"},
            "num_trades": {"column": "num_trades", "op": "eq"},
            "prev_close": {"column": "prev_close", "op": "eq"},
            "settlement": {"column": "settlement", "op": "eq"},
            "prev_settlement": {"column": "prev_settlement", "op": "eq"},
            "open_interest": {"column": "open_interest", "op": "eq"},
            "dominant_id": {"column": "dominant_id", "op": "eq"},
            "strike_price": {"column": "strike_price", "op": "eq"},
            "contract_multiplier": {"column": "contract_multiplier", "op": "eq"},
            "iopv": {"column": "iopv", "op": "eq"},
            "day_session_open": {"column": "day_session_open", "op": "eq"},
        }

    def normalize_db_query_filters(self, filters: dict[str, Any]) -> dict[str, Any]:
        result = dict(filters)
        frequency = result.get("frequency")
        if self.is_minute_frequency(frequency):
            if "start_date" in result:
                result["start_date"] = pd.to_datetime(result["start_date"])
            if "end_date" in result:
                end_raw = pd.to_datetime(result["end_date"])
                if end_raw.time() == pd.Timestamp(end_raw.date()).time():
                    end_raw = end_raw + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
                result["end_date"] = end_raw
            if "datetime" in result:
                result["datetime"] = pd.to_datetime(result["datetime"])
        else:
            if "start_date" in result:
                result["start_date"] = pd.to_datetime(result["start_date"]).date()
            if "end_date" in result:
                result["end_date"] = pd.to_datetime(result["end_date"]).date()
            if "date" in result:
                result["date"] = pd.to_datetime(result["date"]).date()
        return result

    def split_filters(self, filters: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        api_filters = {}
        post_filters = {}
        db_specs = self.resolve_db_filter_specs(filters)
        for k, v in filters.items():
            if k in self.API_PARAMS:
                api_filters[k] = v
            if k in db_specs:
                post_filters[k] = v
        return api_filters, post_filters

    def normalize_df(self, df: pd.DataFrame, filters: dict[str, Any] | None = None) -> pd.DataFrame:
        if df is None:
            return pd.DataFrame()
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)
        if isinstance(df.index, pd.MultiIndex):
            df = df.reset_index()
        elif df.index.name is not None:
            df = df.reset_index()
        filters = filters or {}
        frequency = filters.get("frequency")
        if self.is_daily_frequency(frequency):
            if "date" not in df.columns:
                if "datetime" in df.columns:
                    df = df.rename(columns={"datetime": "date"})
                else:
                    raise ValueError("daily/weekly price dataframe missing 'date' column")
            df["date"] = pd.to_datetime(df["date"]).dt.date
            if "datetime" in df.columns:
                df = df.drop(columns=["datetime"])
            if "trading_date" in df.columns:
                df = df.drop(columns=["trading_date"])
        elif self.is_minute_frequency(frequency):
            if "datetime" not in df.columns:
                if "date" in df.columns:
                    df = df.rename(columns={"date": "datetime"})
                else:
                    raise ValueError("minute price dataframe missing 'datetime' column")
            df["datetime"] = pd.to_datetime(df["datetime"])
            if "date" in df.columns:
                df = df.drop(columns=["date"])
            if "trading_date" in df.columns:
                df = df.drop(columns=["trading_date"])
        else:
            raise ValueError(f"unsupported frequency={frequency}")
        order_book_ids = filters.get("order_book_ids")
        if "order_book_id" not in df.columns and order_book_ids and len(order_book_ids) == 1:
            df["order_book_id"] = order_book_ids[0]
        df["type"] = self.FIXED_TYPE
        df["frequency"] = filters.get("frequency")
        df["market"] = filters.get("market", "cn")
        return df

    def filter_df(self, df: pd.DataFrame, filters: dict[str, Any]) -> pd.DataFrame:
        if df.empty:
            return df
        result = df.copy()
        if "date" in result.columns:
            result["date"] = pd.to_datetime(result["date"]).dt.date
        if "datetime" in result.columns:
            result["datetime"] = pd.to_datetime(result["datetime"])
        post_filters = dict(filters)
        if "start_date" in post_filters:
            if self.is_minute_frequency(filters.get("frequency")):
                post_filters["start_date"] = pd.to_datetime(post_filters["start_date"])
            else:
                post_filters["start_date"] = pd.to_datetime(post_filters["start_date"]).date()
        if "end_date" in post_filters:
            if self.is_minute_frequency(filters.get("frequency")):
                end_raw = pd.to_datetime(post_filters["end_date"])
                if end_raw.time() == pd.Timestamp(end_raw.date()).time():
                    end_raw = end_raw + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
                post_filters["end_date"] = end_raw
            else:
                post_filters["end_date"] = pd.to_datetime(post_filters["end_date"]).date()
        return super().filter_df(result, post_filters)

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
    STORAGE_BACKEND = "mysql"
    WRITE_MODE = "snapshot_upsert"

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
                s = pd.to_datetime(result[col], errors="coerce", format="mixed")
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
                result[col] = pd.to_datetime(result[col], errors="coerce", format="mixed").dt.date

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
