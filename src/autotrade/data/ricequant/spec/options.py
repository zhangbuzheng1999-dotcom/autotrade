# autotrade/data/ricequant/spec/options.py

from __future__ import annotations

from typing import Any

import pandas as pd


from autotrade.data.ricequant.base import BaseRQSpec, FetchMode


class OptionInstrumentSpec(BaseRQSpec):
    """
    all_instruments(type='Option')

    API层固定：
        type = 'Option'

    SOURCE支持：
        - date
        - market

    DB支持：
        - order_book_id
        - symbol
        - underlying_symbol
        - exercise_type
        - option_type
        - product_name
        - exchange
        - start_date -> listed_date >=
        - end_date   -> maturity_date <=
        - date       -> 某日可交易合约语义
    """

    RESOURCE_NAME = "option_instruments"
    RESOURCE_TYPE = "snapshot"
    STORAGE_BACKEND = "mysql"
    WRITE_MODE = "snapshot_upsert"

    DATABASE = "rq_option_data"
    TABLE = "option_instruments"

    PRIMARY_KEYS = ["order_book_id"]

    API_PARAMS = {
        "date",
        "market",
    }

    API_REQUIRED_FILTERS = set()

    DB_QUERY_FIELDS = {
        "order_book_id",
        "symbol",
        "underlying_symbol",
        "exercise_type",
        "option_type",
        "product_name",
        "exchange",
        "market",
        "start_date",
        "end_date",
        "date",
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
        "maturity_date",
    }

    CODE_FIELDS = {
        "order_book_id",
        "underlying_order_book_id",
    }

    COLUMNS = [
        "order_book_id",
        "symbol",
        "round_lot",
        "listed_date",
        "type",
        "contract_multiplier",
        "underlying_order_book_id",
        "underlying_symbol",
        "maturity_date",
        "exchange",
        "strike_price",
        "option_type",
        "exercise_type",
        "market_tplus",
        "product_name",
        "market",
    ]

    FIXED_TYPE = "Option"

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
            raise ValueError("option_instruments market must be one of {'cn', 'hk'}")

        exercise_type = filters.get("exercise_type")
        if exercise_type is not None and exercise_type not in {"E", "A"}:
            raise ValueError("exercise_type must be one of {'E', 'A'}")

        option_type = filters.get("option_type")
        if option_type is not None and option_type not in {"C", "P"}:
            raise ValueError("option_type must be one of {'C', 'P'}")

    def resolve_database(self, filters: dict[str, Any]) -> str:
        return "rq_option_data"

    def resolve_table(self, filters: dict[str, Any]) -> str:
        return "option_instruments"

    def resolve_db_filter_specs(self, filters: dict[str, Any]) -> dict[str, dict[str, Any]]:
        return {
            "order_book_id": {"column": "order_book_id", "op": "eq"},
            "symbol": {"column": "symbol", "op": "eq"},
            "underlying_symbol": {"column": "underlying_symbol", "op": "eq"},
            "exercise_type": {"column": "exercise_type", "op": "eq"},
            "option_type": {"column": "option_type", "op": "eq"},
            "product_name": {"column": "product_name", "op": "eq"},
            "exchange": {"column": "exchange", "op": "eq"},
            "market": {"column": "market", "op": "eq"},

            "start_date": {"column": "listed_date", "op": "gte"},
            "end_date": {"column": "maturity_date", "op": "lte"},

            # 某日可交易语义：listed_date <= date <= maturity_date
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
            "maturity_date",
        ]:
            if col in result.columns:
                s = pd.to_datetime(result[col], errors="coerce")
                result[col] = s.dt.date
                result[col] = result[col].where(s.notna(), None)

        result["type"] = self.FIXED_TYPE
        result["market"] = (filters or {}).get("market", "cn")

        if "order_book_id" not in result.columns:
            raise ValueError("option_instruments dataframe missing order_book_id column")

        if result["order_book_id"].isna().any():
            bad_rows = result[result["order_book_id"].isna()]
            raise ValueError(
                f"option_instruments dataframe contains null order_book_id rows: {len(bad_rows)}"
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

        for col in ["listed_date", "maturity_date"]:
            if col in result.columns:
                result[col] = pd.to_datetime(result[col], errors="coerce").dt.date

        date_value = filters.get("date")
        if date_value is not None:
            d = pd.to_datetime(date_value).date()

            if "listed_date" in result.columns:
                result = result[result["listed_date"].isna() | (result["listed_date"] <= d)]

            if "maturity_date" in result.columns:
                result = result[result["maturity_date"].isna() | (result["maturity_date"] >= d)]

        normalized_filters = dict(filters)

        if "start_date" in normalized_filters:
            normalized_filters["start_date"] = pd.to_datetime(normalized_filters["start_date"]).date()

        if "end_date" in normalized_filters:
            normalized_filters["end_date"] = pd.to_datetime(normalized_filters["end_date"]).date()

        normalized_filters.pop("date", None)

        return super().filter_df(result, normalized_filters)

class OptionGreeksSpec(BaseRQSpec):
    """
    options.get_greeks

    API语义：
        options.get_greeks(
            order_book_ids,
            start_date=None,
            end_date=None,
            fields=None,
            model='implied_forward',
            price_type='close',
            frequency='1d',
            market='cn'
        )

    规则：
    - SOURCE_ONLY / DB_THEN_SOURCE:
        必须传 order_book_ids、start_date
    - DB_ONLY:
        允许不传 order_book_ids
        其他参数要求与 API 保持一致（即 start_date 仍必填）

    设计说明：
    - 数据库尽量贴近 rqdatac 原始返回结构
    - 日频主时间键：trading_date
    - 分钟主时间键：datetime
    - greek 字段统一使用真实返回列名：
        iv, delta, gamma, vega, theta, rho
    """

    RESOURCE_NAME = "option_greeks"
    RESOURCE_TYPE = "timeseries"
    STORAGE_BACKEND = "clickhouse"
    WRITE_MODE = "timeseries_append"

    DATABASE = "rq_option_data"
    TABLE = ""

    PRIMARY_KEYS = []

    SUPPORTED_FREQUENCIES = {"1d", "1m"}
    SUPPORTED_MODELS = {"implied_forward", "last"}
    SUPPORTED_PRICE_TYPES = {"close", "settlement"}

    DAILY_FREQUENCIES = {"1d"}
    MINUTE_FREQUENCIES = {"1m"}

    API_PARAMS = {
        "order_book_ids",
        "start_date",
        "end_date",
        "fields",
        "model",
        "price_type",
        "frequency",
        "market",
    }

    API_REQUIRED_FILTERS = {
        "order_book_ids",
        "start_date",
    }

    DB_QUERY_FIELDS = {
        "order_book_id",
        "order_book_ids",
        "start_date",
        "end_date",
        "trading_date",
        "datetime",
        "model",
        "price_type",
        "frequency",
        "market",
        "iv",
        "delta",
        "gamma",
        "vega",
        "theta",
        "rho",
        "fields",
    }

    DB_REQUIRED_FILTERS = {
        "start_date",
    }

    DEFAULT_FILTERS = {
        "model": "implied_forward",
        "price_type": "close",
        "frequency": "1d",
        "market": "cn",
    }

    DATE_FIELDS = {"trading_date", "datetime", "start_date", "end_date"}
    CODE_FIELDS = {"order_book_id", "order_book_ids"}

    COLUMNS: list[str] = []

    GREEK_COLUMNS = [
        "iv",
        "delta",
        "gamma",
        "vega",
        "theta",
        "rho",
    ]

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

        if "fields" in result and isinstance(result["fields"], str):
            result["fields"] = [result["fields"]]

        return result

    def validate_filters(
        self,
        filters: dict[str, Any],
        mode: FetchMode,
    ) -> None:
        super().validate_filters(filters, mode)

        frequency = filters.get("frequency")
        if frequency not in self.SUPPORTED_FREQUENCIES:
            raise ValueError(
                f"{self.RESOURCE_NAME} unsupported frequency={frequency}, "
                f"supported={sorted(self.SUPPORTED_FREQUENCIES)}"
            )

        model = filters.get("model")
        if model not in self.SUPPORTED_MODELS:
            raise ValueError(
                f"{self.RESOURCE_NAME} unsupported model={model}, "
                f"supported={sorted(self.SUPPORTED_MODELS)}"
            )

        price_type = filters.get("price_type")
        if price_type not in self.SUPPORTED_PRICE_TYPES:
            raise ValueError(
                f"{self.RESOURCE_NAME} unsupported price_type={price_type}, "
                f"supported={sorted(self.SUPPORTED_PRICE_TYPES)}"
            )

        market = filters.get("market")
        if market != "cn":
            raise ValueError(f"{self.RESOURCE_NAME} currently only supports market='cn'")

        if frequency == "1m" and price_type != "close":
            raise ValueError("option_greeks 1m frequency requires price_type='close'")

        fields = filters.get("fields")
        if fields is not None:
            illegal = set(fields) - set(self.GREEK_COLUMNS)
            if illegal:
                raise ValueError(
                    f"{self.RESOURCE_NAME} unsupported fields={sorted(illegal)}, "
                    f"supported={self.GREEK_COLUMNS}"
                )

    def resolve_database(self, filters: dict[str, Any]) -> str:
        return "rq_option_data"

    def resolve_table(self, filters: dict[str, Any]) -> str:
        frequency = filters.get("frequency")
        if frequency not in self.SUPPORTED_FREQUENCIES:
            raise ValueError(
                f"{self.RESOURCE_NAME} unsupported frequency={frequency} for table routing"
            )
        return f"option_greeks_{frequency}"

    def resolve_db_filter_specs(self, filters: dict[str, Any]) -> dict[str, dict[str, Any]]:
        frequency = filters.get("frequency")
        time_col = "datetime" if self.is_minute_frequency(frequency) else "trading_date"

        return {
            "order_book_id": {"column": "order_book_id", "op": "eq"},
            "order_book_ids": {"column": "order_book_id", "op": "in"},
            "start_date": {"column": time_col, "op": "gte"},
            "end_date": {"column": time_col, "op": "lte"},
            "trading_date": {"column": "trading_date", "op": "eq"},
            "datetime": {"column": "datetime", "op": "eq"},
            "model": {"column": "model", "op": "eq"},
            "price_type": {"column": "price_type", "op": "eq"},
            "frequency": {"column": "frequency", "op": "eq"},
            "market": {"column": "market", "op": "eq"},
            "iv": {"column": "iv", "op": "eq"},
            "delta": {"column": "delta", "op": "eq"},
            "gamma": {"column": "gamma", "op": "eq"},
            "vega": {"column": "vega", "op": "eq"},
            "theta": {"column": "theta", "op": "eq"},
            "rho": {"column": "rho", "op": "eq"},
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
        elif not isinstance(df.index, pd.RangeIndex):
            df = df.reset_index()

        filters = filters or {}
        frequency = filters.get("frequency")

        # 统一 greek 列名大小写
        rename_map = {
            "Delta": "delta",
            "Gamma": "gamma",
            "Vega": "vega",
            "Theta": "theta",
            "Rho": "rho",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        if self.is_daily_frequency(frequency):
            # rqdatac 日频真实返回是 trading_date
            if "trading_date" not in df.columns:
                if "date" in df.columns:
                    df = df.rename(columns={"date": "trading_date"})
                elif "datetime" in df.columns:
                    df = df.rename(columns={"datetime": "trading_date"})
                elif "index" in df.columns:
                    df = df.rename(columns={"index": "trading_date"})
                else:
                    raise ValueError(
                        f"option_greeks daily dataframe missing 'trading_date' column, "
                        f"columns={list(df.columns)}"
                    )

            df["trading_date"] = pd.to_datetime(df["trading_date"]).dt.date

            if "datetime" in df.columns:
                df = df.drop(columns=["datetime"])

        elif self.is_minute_frequency(frequency):
            if "datetime" not in df.columns:
                if "date" in df.columns:
                    df = df.rename(columns={"date": "datetime"})
                elif "trading_date" in df.columns:
                    df = df.rename(columns={"trading_date": "datetime"})
                elif "index" in df.columns:
                    df = df.rename(columns={"index": "datetime"})
                else:
                    raise ValueError(
                        f"option_greeks minute dataframe missing 'datetime' column, "
                        f"columns={list(df.columns)}"
                    )

            df["datetime"] = pd.to_datetime(df["datetime"])

            if "trading_date" in df.columns:
                df = df.drop(columns=["trading_date"])

        else:
            raise ValueError(f"unsupported frequency={frequency}")

        order_book_ids = filters.get("order_book_ids")
        if "order_book_id" not in df.columns and order_book_ids and len(order_book_ids) == 1:
            df["order_book_id"] = order_book_ids[0]

        for col in self.GREEK_COLUMNS:
            if col not in df.columns:
                df[col] = None

        df["model"] = filters.get("model", "implied_forward")
        df["price_type"] = filters.get("price_type", "close")
        df["frequency"] = filters.get("frequency", "1d")
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

        if "trading_date" in result.columns:
            result["trading_date"] = pd.to_datetime(result["trading_date"]).dt.date

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
