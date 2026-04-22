from __future__ import annotations

from typing import Any

import pandas as pd

from autotrade.data.ricequant.base import BaseRQSpec, FetchMode



class PriceSpec(BaseRQSpec):
    """
    RiceQuant get_price spec

    当前版本 price 只支持以下资产数据库：
    - CS      -> rq_stock_data
    - ETF     -> rq_etf_data
    - Future  -> rq_future_data
    - Option  -> rq_option_data
    - INDX    -> rq_index_data

    其他类型暂不支持，后续如需支持必须扩展数据库路由与建表逻辑。
    """

    RESOURCE_NAME = "price"
    RESOURCE_TYPE = "timeseries"
    STORAGE_BACKEND = "clickhouse"
    WRITE_MODE = "timeseries_append"

    DATABASE = ""
    TABLE = ""

    PRIMARY_KEYS = []

    SUPPORTED_FREQUENCIES = {
        "1d", "1w", "1m", "5m", "15m", "30m", "60m"
    }

    # 当前版本只支持这些 type
    SUPPORTED_TYPES = {
        "CS",
        "ETF",
        "Future",
        "Option",
        "INDX",
    }

    TYPE_DATABASE_MAP = {
        "CS": "rq_stock_data",
        "ETF": "rq_etf_data",
        "Future": "rq_future_data",
        "Option": "rq_option_data",
        "INDX": "rq_index_data",
    }

    TYPE_TABLE_PREFIX_MAP = {
        "CS": "stock_price",
        "ETF": "etf_price",
        "Future": "future_price",
        "Option": "option_price",
        "INDX": "index_price",
    }

    MINUTE_FREQUENCIES = {"1m", "5m", "15m", "30m", "60m"}
    DAILY_FREQUENCIES = {"1d", "1w"}

    API_PARAMS = {
        "type",
        "order_book_ids",
        "start_date",
        "end_date",
        "frequency",
        "fields",
        "adjust_type",
        "skip_suspended",
        "expect_df",
        "time_slice",
        "market",
    }

    API_REQUIRED_FILTERS = {
        "type",
        "order_book_ids",
        "frequency",
    }

    DB_QUERY_FIELDS = {
        "type",
        "frequency",
        "market",

        "order_book_id",
        "order_book_ids",

        "date",
        "datetime",
        "trading_date",
        "start_date",
        "end_date",

        "open",
        "close",
        "high",
        "low",
        "limit_up",
        "limit_down",
        "total_turnover",
        "volume",
        "num_trades",
        "prev_close",
        "settlement",
        "prev_settlement",
        "open_interest",
        "dominant_id",
        "strike_price",
        "contract_multiplier",
        "iopv",
        "day_session_open",
    }

    DB_REQUIRED_FILTERS = {
        "type",
        "frequency",
    }

    DATE_FIELDS = {"date", "datetime", "trading_date", "start_date", "end_date"}
    CODE_FIELDS = {"order_book_id", "order_book_ids"}

    DEFAULT_FILTERS = {
        "market": "cn",
        "adjust_type": "pre",
        "skip_suspended": False,
        "expect_df": True,
        "frequency": "1d",
    }

    COLUMNS: list[str] = []

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

        if "order_book_id" in result and "order_book_ids" not in result:
            result["order_book_ids"] = self._normalize_order_book_ids(result["order_book_id"])

        if "order_book_ids" in result:
            result["order_book_ids"] = self._normalize_order_book_ids(result["order_book_ids"])

        return result

    def validate_filters(
        self,
        filters: dict[str, Any],
        mode: FetchMode,
    ) -> None:
        super().validate_filters(filters, mode)

        asset_type = filters.get("type")
        if asset_type not in self.SUPPORTED_TYPES:
            raise ValueError(
                f"{self.RESOURCE_NAME} unsupported type={asset_type}. "
                f"Current version only supports {sorted(self.SUPPORTED_TYPES)}. "
                f"If you need this type, please extend database routing and table initialization first."
            )

        frequency = filters.get("frequency")
        if frequency not in self.SUPPORTED_FREQUENCIES:
            raise ValueError(
                "tick frequency is not supported in current version; "
                "supported frequencies are ['1d', '1w', '1m', '5m', '15m', '30m', '60m']"
            )

        if frequency == "1w" and filters.get("expect_df") is False:
            raise ValueError("weekly price query requires expect_df=True")

        if mode in {FetchMode.SOURCE_ONLY, FetchMode.DB_THEN_SOURCE}:
            order_book_ids = filters.get("order_book_ids")
            if not order_book_ids:
                raise ValueError(
                    f"{self.RESOURCE_NAME} requires order_book_ids for mode={mode.value}"
                )

    def resolve_database(self, filters: dict[str, Any]) -> str:
        asset_type = filters.get("type")

        database = self.TYPE_DATABASE_MAP.get(asset_type)
        if database is None:
            raise ValueError(
                f"{self.RESOURCE_NAME} unsupported type={asset_type}. "
                f"Current version only supports {sorted(self.TYPE_DATABASE_MAP.keys())}. "
                f"If you need this type, please extend database routing first."
            )
        return database

    def resolve_table(self, filters: dict[str, Any]) -> str:
        asset_type = filters.get("type")
        frequency = filters.get("frequency")

        if not asset_type:
            raise ValueError(f"{self.RESOURCE_NAME} requires type for table routing")
        if not frequency:
            raise ValueError(f"{self.RESOURCE_NAME} requires frequency for table routing")

        prefix = self.TYPE_TABLE_PREFIX_MAP.get(asset_type)
        if prefix is None:
            raise ValueError(
                f"{self.RESOURCE_NAME} unsupported type={asset_type}. "
                f"Current version only supports {sorted(self.TYPE_TABLE_PREFIX_MAP.keys())}. "
                f"If you need this type, please extend table routing first."
            )

        return f"{prefix}_{frequency}"

    def resolve_db_filter_specs(self, filters: dict[str, Any]) -> dict[str, dict[str, Any]]:
        frequency = filters.get("frequency")
        # DB-side start/end filters should preserve the same day-range semantics
        # as the RiceQuant API. For minute bars, filtering on `datetime` with a
        # date-only value like `2024-01-10` would collapse the upper bound to
        # midnight and miss all intraday rows, so use `trading_date` instead.
        time_col = "trading_date" if self.is_minute_frequency(frequency) else "date"

        return {
            "type": {"column": "type", "op": "eq"},
            "frequency": {"column": "frequency", "op": "eq"},
            "market": {"column": "market", "op": "eq"},

            "order_book_id": {"column": "order_book_id", "op": "eq"},
            "order_book_ids": {"column": "order_book_id", "op": "in"},

            "start_date": {"column": time_col, "op": "gte"},
            "end_date": {"column": time_col, "op": "lte"},

            "date": {"column": "date", "op": "eq"},
            "datetime": {"column": "datetime", "op": "eq"},
            "trading_date": {"column": "trading_date", "op": "eq"},

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

    def split_filters(
        self,
        filters: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        api_filters = {}
        post_filters = {}

        db_specs = self.resolve_db_filter_specs(filters)

        for k, v in filters.items():
            if k in self.API_PARAMS:
                api_filters[k] = v
            if k in db_specs:
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

            start_date = filters.get("start_date")
            end_date = filters.get("end_date")

            if "trading_date" not in df.columns:
                if start_date is not None and end_date is not None and str(start_date) == str(end_date):
                    df["trading_date"] = pd.to_datetime(start_date).date()
                else:
                    df["trading_date"] = None

        else:
            raise ValueError(f"unsupported frequency={frequency}")

        order_book_ids = filters.get("order_book_ids")
        if "order_book_id" not in df.columns and order_book_ids and len(order_book_ids) == 1:
            df["order_book_id"] = order_book_ids[0]

        df["type"] = filters.get("type")
        df["frequency"] = filters.get("frequency")
        df["market"] = filters.get("market", "cn")

        return df

    def filter_df(
        self,
        df: pd.DataFrame,
        filters: dict[str, Any],
    ) -> pd.DataFrame:
        if df.empty:
            return df

        result = df.copy()

        if "date" in result.columns:
            result["date"] = pd.to_datetime(result["date"]).dt.date

        if "datetime" in result.columns:
            result["datetime"] = pd.to_datetime(result["datetime"])

        post_filters = dict(filters)

        if "start_date" in post_filters:
            frequency = filters.get("frequency")
            if self.is_minute_frequency(frequency):
                post_filters["start_date"] = pd.to_datetime(post_filters["start_date"])
            else:
                post_filters["start_date"] = pd.to_datetime(post_filters["start_date"]).date()

        if "end_date" in post_filters:
            frequency = filters.get("frequency")
            if self.is_minute_frequency(frequency):
                end_raw = pd.to_datetime(post_filters["end_date"])
                if end_raw.time() == pd.Timestamp(end_raw.date()).time():
                    end_raw = end_raw + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
                post_filters["end_date"] = end_raw
            else:
                post_filters["end_date"] = pd.to_datetime(post_filters["end_date"]).date()

        return super().filter_df(result, post_filters)

class TradingDatesSpec(BaseRQSpec):
    """
    get_trading_dates(start_date, end_date, market='cn')

    SOURCE_ONLY / DB_THEN_SOURCE:
        - 必须传 start_date
        - 必须传 end_date

    DB_ONLY:
        - 也要求 start_date / end_date
        - 支持 market 过滤
    """

    RESOURCE_NAME = "trading_dates"
    RESOURCE_TYPE = "timeseries"
    STORAGE_BACKEND = "mysql"
    WRITE_MODE = "timeseries_append"

    DATABASE = "rq_data"
    TABLE = "trading_dates"

    PRIMARY_KEYS = ["market", "trading_date"]

    API_PARAMS = {
        "start_date",
        "end_date",
        "market",
    }

    API_REQUIRED_FILTERS = {
        "start_date",
        "end_date",
    }

    DB_QUERY_FIELDS = {
        "start_date",
        "end_date",
        "trading_date",
        "market",
    }

    DB_REQUIRED_FILTERS = {
        "start_date",
        "end_date",
    }

    DEFAULT_FILTERS = {
        "market": "cn",
    }

    DATE_FIELDS = {
        "start_date",
        "end_date",
        "trading_date",
    }

    CODE_FIELDS = set()

    COLUMNS = [
        "trading_date",
        "market",
    ]

    SUPPORTED_MARKETS = {"cn", "hk"}

    def validate_filters(
        self,
        filters: dict[str, Any],
        mode: FetchMode,
    ) -> None:
        super().validate_filters(filters, mode)

        market = filters.get("market")
        if market not in self.SUPPORTED_MARKETS:
            raise ValueError(
                f"{self.RESOURCE_NAME} unsupported market={market}, "
                f"supported={sorted(self.SUPPORTED_MARKETS)}"
            )

        start_date = pd.to_datetime(filters.get("start_date")).date()
        end_date = pd.to_datetime(filters.get("end_date")).date()
        if start_date > end_date:
            raise ValueError(
                f"{self.RESOURCE_NAME} requires start_date <= end_date, "
                f"got {start_date} > {end_date}"
            )

    def resolve_database(self, filters: dict[str, Any]) -> str:
        return "rq_data"

    def resolve_table(self, filters: dict[str, Any]) -> str:
        return "trading_dates"

    def resolve_db_filter_specs(self, filters: dict[str, Any]) -> dict[str, dict[str, Any]]:
        return {
            "market": {"column": "market", "op": "eq"},
            "trading_date": {"column": "trading_date", "op": "eq"},
            "start_date": {"column": "trading_date", "op": "gte"},
            "end_date": {"column": "trading_date", "op": "lte"},
        }

    def normalize_df(
        self,
        df: pd.DataFrame,
        filters: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        if df is None:
            return pd.DataFrame(columns=self.COLUMNS)

        # rqdatac.get_trading_dates 返回 list[datetime.date]
        if isinstance(df, list):
            df = pd.DataFrame({"trading_date": df})
        elif not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)

        if isinstance(df.index, pd.MultiIndex):
            df = df.reset_index()
        elif df.index.name is not None:
            df = df.reset_index()

        result = df.copy()

        if "trading_date" not in result.columns:
            if len(result.columns) == 1:
                result = result.rename(columns={result.columns[0]: "trading_date"})
            else:
                raise ValueError(
                    f"{self.RESOURCE_NAME} dataframe missing trading_date column, "
                    f"columns={list(result.columns)}"
                )

        s = pd.to_datetime(result["trading_date"], errors="coerce")
        result["trading_date"] = s.dt.date
        result["trading_date"] = result["trading_date"].where(s.notna(), None)

        result["market"] = (filters or {}).get("market", "cn")

        if result["trading_date"].isna().any():
            bad_rows = result[result["trading_date"].isna()]
            raise ValueError(
                f"{self.RESOURCE_NAME} dataframe contains null trading_date rows: {len(bad_rows)}"
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
        if "trading_date" in result.columns:
            result["trading_date"] = pd.to_datetime(result["trading_date"], errors="coerce").dt.date

        normalized_filters = dict(filters)
        if "start_date" in normalized_filters:
            normalized_filters["start_date"] = pd.to_datetime(normalized_filters["start_date"]).date()
        if "end_date" in normalized_filters:
            normalized_filters["end_date"] = pd.to_datetime(normalized_filters["end_date"]).date()

        return super().filter_df(result, normalized_filters)

