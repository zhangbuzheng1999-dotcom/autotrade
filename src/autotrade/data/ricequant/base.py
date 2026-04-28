# autotrade/data/ricequant/base.py

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable

import math
import warnings
from datetime import date, datetime
from decimal import Decimal

import numpy as np
import pandas as pd


from autotrade.coreutils.config import DatabaseInfo

from autotrade.data.ricequant._clickhouse import ClickHouseClient


def _import_pymysql():
    try:
        import pymysql
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pymysql is required for RiceQuant repository/database operations. "
            "Install it in the active environment, for example: pip install pymysql"
        ) from exc
    return pymysql


# ============================================================
# Enums / DTO
# ============================================================

class FetchMode(Enum):
    DB_ONLY = "db_only"
    SOURCE_ONLY = "source_only"
    DB_THEN_SOURCE = "db_then_source"


class FetchStatus(Enum):
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class FetchResult:
    status: FetchStatus
    data: pd.DataFrame | None = None
    error: Exception | None = None


# ============================================================
# Utils
# ============================================================

def normalize_mysql_value(v: Any) -> Any:
    """
    Normalize Python / pandas / numpy values into MySQL-safe values.
    """

    # 最优先兜底 pandas 缺失值（NaT / NaN / None）
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    if v is None:
        return None

    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return v

    if isinstance(v, np.floating):
        v = float(v)
        if math.isnan(v) or math.isinf(v):
            return None
        return v

    if isinstance(v, (pd.Timestamp, np.datetime64)):
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        return pd.to_datetime(v).to_pydatetime()

    if isinstance(v, (datetime, date)):
        return v

    if isinstance(v, Decimal):
        return v

    if isinstance(v, (int, np.integer)):
        return int(v)

    if isinstance(v, str):
        if v.strip().lower() in {"nat", "nan", "none", ""}:
            return None
        return v

    return str(v)



def chunked(iterable: list, size: int) -> Iterable[list]:
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


# ============================================================
# Spec
# ============================================================

class BaseRQSpec(ABC):
    """
    Resource spec:
    - centralizes API rules
    - centralizes DB query rules
    - centralizes routing rules
    - centralizes dataframe normalization rules
    """

    RESOURCE_NAME: str = ""

    # fallback default values only
    DATABASE: str = ""
    TABLE: str = ""

    RESOURCE_TYPE: str = "snapshot"   # snapshot / timeseries
    STORAGE_BACKEND: str = "mysql"    # mysql / clickhouse
    WRITE_MODE: str = ""              # snapshot_upsert / timeseries_append

    PRIMARY_KEYS: list[str] = []
    COLUMNS: list[str] = []

    # -------- source-side semantics --------
    API_PARAMS: set[str] = set()
    API_REQUIRED_FILTERS: set[str] = set()

    # -------- db-side semantics --------
    # 这里只做“允许哪些逻辑字段参与 DB 查询校验”
    DB_QUERY_FIELDS: set[str] = set()
    DB_REQUIRED_FILTERS: set[str] = set()

    DATE_FIELDS: set[str] = set()
    CODE_FIELDS: set[str] = set()

    DEFAULT_FILTERS: dict[str, Any] = {}

    # supported operators for db filter compilation
    SUPPORTED_DB_OPERATORS = {"eq", "in", "gte", "lte", "gt", "lt"}

    # --------------------------------------------------------
    # query normalization
    # --------------------------------------------------------
    def normalize_query_filters(self, filters: dict[str, Any]) -> dict[str, Any]:
        """
        Hook for subclasses.
        Used to normalize external query semantics.
        """
        return dict(filters)

    def fill_default_filters(self, filters: dict[str, Any]) -> dict[str, Any]:
        merged = dict(self.DEFAULT_FILTERS)
        merged.update({k: v for k, v in filters.items() if v is not None})
        return merged

    # --------------------------------------------------------
    # validation
    # --------------------------------------------------------
    def validate_filters(
        self,
        filters: dict[str, Any],
        mode: FetchMode,
    ) -> None:
        if not isinstance(filters, dict):
            raise TypeError("filters must be dict")

        if mode == FetchMode.DB_ONLY:
            allowed = (
                self.DB_QUERY_FIELDS
                | self.API_PARAMS
                | set(self.DEFAULT_FILTERS.keys())
            )
            required = self.DB_REQUIRED_FILTERS
        else:
            # SOURCE_ONLY / DB_THEN_SOURCE 必须遵守 API 语义
            allowed = self.API_PARAMS | set(self.DEFAULT_FILTERS.keys())
            required = self.API_REQUIRED_FILTERS

        illegal = set(filters.keys()) - allowed
        if illegal:
            raise ValueError(
                f"{self.RESOURCE_NAME} got unsupported filters for mode={mode.value}: "
                f"{sorted(illegal)}"
            )

        missing = [k for k in required if filters.get(k) is None]
        if missing:
            raise ValueError(
                f"{self.RESOURCE_NAME} missing required filters for mode={mode.value}: "
                f"{missing}"
            )

    # --------------------------------------------------------
    # routing
    # --------------------------------------------------------
    def resolve_database(self, filters: dict[str, Any]) -> str:
        if not self.DATABASE:
            raise ValueError(f"{self.RESOURCE_NAME} DATABASE is not configured")
        return self.DATABASE

    def resolve_table(self, filters: dict[str, Any]) -> str:
        if not self.TABLE:
            raise ValueError(f"{self.RESOURCE_NAME} TABLE is not configured")
        return self.TABLE

    def resolve_storage_backend(self, filters: dict[str, Any]) -> str:
        return self.STORAGE_BACKEND

    def resolve_write_mode(self, filters: dict[str, Any]) -> str:
        if self.WRITE_MODE:
            return self.WRITE_MODE
        if self.RESOURCE_TYPE == "snapshot":
            return "snapshot_upsert"
        return "timeseries_append"

    def normalize_db_query_filters(self, filters: dict[str, Any]) -> dict[str, Any]:
        return dict(filters)

    # --------------------------------------------------------
    # db filter specs
    # --------------------------------------------------------
    def resolve_db_filter_specs(self, filters: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """
        Return a mapping from logical filter field -> db filter rule.

        Example:
        {
            "order_book_ids": {"column": "order_book_id", "op": "in"},
            "start_date": {"column": "date", "op": "gte"},
            "end_date": {"column": "date", "op": "lte"},
            "type": {"column": "type", "op": "eq"},
        }
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement resolve_db_filter_specs()"
        )

    # --------------------------------------------------------
    # filter split
    # --------------------------------------------------------
    def split_filters(
        self,
        filters: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """
        Split into:
        - api_filters: directly passed to rqdata api
        - post_filters: applied on dataframe after api fetch
        """
        api_filters = {}
        post_filters = {}

        db_specs = self.resolve_db_filter_specs(filters)

        for k, v in filters.items():
            if k in self.API_PARAMS:
                api_filters[k] = v
            if k in db_specs:
                post_filters[k] = v

        return api_filters, post_filters

    # --------------------------------------------------------
    # dataframe normalize / filter
    # --------------------------------------------------------
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

        return df

    def filter_df(
        self,
        df: pd.DataFrame,
        filters: dict[str, Any],
    ) -> pd.DataFrame:
        """
        Generic dataframe filter using db filter specs:
        - eq
        - in
        - gte
        - lte
        - gt
        - lt
        """
        if df.empty:
            return df

        result = df
        db_specs = self.resolve_db_filter_specs(filters)

        for logical_field, value in filters.items():
            if value is None:
                continue

            if logical_field not in db_specs:
                continue

            rule = db_specs[logical_field]
            column = rule["column"]
            op = rule["op"]

            if column not in result.columns:
                continue

            series = result[column]

            if op == "eq":
                result = result[series == value]

            elif op == "in":
                if isinstance(value, str):
                    value = [value]
                elif not isinstance(value, (list, tuple, set)):
                    value = [value]
                value = list(value)
                if not value:
                    return result.iloc[0:0]
                result = result[series.isin(value)]

            elif op == "gte":
                result = result[series >= value]

            elif op == "lte":
                result = result[series <= value]

            elif op == "gt":
                result = result[series > value]

            elif op == "lt":
                result = result[series < value]

            else:
                raise ValueError(
                    f"{self.RESOURCE_NAME} unsupported db operator={op} "
                    f"for logical_field={logical_field}"
                )

        return result


# ============================================================
# Repository
# ============================================================

class BaseRQRepository:
    """
    Generic MySQL repository driven by spec.
    Supports dynamic database / table routing.
    """

    BATCH_SIZE = 2000

    def __init__(self, spec: BaseRQSpec):
        self.spec = spec
        self._base_conn_args = dict(
            host=DatabaseInfo.host,
            port=DatabaseInfo.port,
            user=DatabaseInfo.user,
            passwd=DatabaseInfo.password,
            charset="utf8mb4",
            autocommit=True,
        )

    @contextmanager
    def get_conn(self, database: str):
        if not database:
            raise ValueError("database is required")

        pymysql = _import_pymysql()
        conn = pymysql.connect(
            **self._base_conn_args,
            database=database,
        )
        try:
            yield conn
        finally:
            conn.close()

    def query(self, **filters) -> pd.DataFrame:
        filters = {k: v for k, v in filters.items() if v is not None}
        filters = self.spec.normalize_query_filters(filters)
        filters = self.spec.fill_default_filters(filters)
        self.spec.validate_filters(filters, mode=FetchMode.DB_ONLY)
        filters = self.spec.normalize_db_query_filters(filters)

        database = self.spec.resolve_database(filters)
        table = self.spec.resolve_table(filters)
        db_filter_specs = self.spec.resolve_db_filter_specs(filters)

        where_sql, params = self._build_where(filters, db_filter_specs)

        sql = f"""
        SELECT *
        FROM {table}
        {where_sql}
        """

        with self.get_conn(database) as conn:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                return pd.read_sql(sql, conn, params=params)

    def insert_ignore(self, df: pd.DataFrame, **filters) -> None:
        if df is None or df.empty:
            return

        filters = {k: v for k, v in filters.items() if v is not None}
        filters = self.spec.normalize_query_filters(filters)
        filters = self.spec.fill_default_filters(filters)

        database = self.spec.resolve_database(filters)
        table = self.spec.resolve_table(filters)

        df = self._align_columns(df)

        cols = df.columns.tolist()
        col_names = ",".join(self._escape_column(c) for c in cols)
        placeholders = ",".join(["%s"] * len(cols))

        sql = f"""
        INSERT IGNORE INTO {table} ({col_names})
        VALUES ({placeholders})
        """

        raw_values = df[cols].values.tolist()
        values = [
            [normalize_mysql_value(v) for v in row]
            for row in raw_values
        ]

        with self.get_conn(database) as conn:
            cursor = conn.cursor()

            try:
                cursor.executemany(sql, values)
                return
            except Exception:
                pass

            for batch in chunked(values, self.BATCH_SIZE):
                try:
                    cursor.executemany(sql, batch)
                except Exception:
                    for row in batch:
                        try:
                            cursor.execute(sql, row)
                        except Exception:
                            continue

    def upsert(self, df: pd.DataFrame, **filters) -> None:
        if df is None or df.empty:
            return

        if not self.spec.PRIMARY_KEYS:
            raise ValueError(
                f"{self.spec.RESOURCE_NAME} spec.PRIMARY_KEYS is required for upsert"
            )

        filters = {k: v for k, v in filters.items() if v is not None}
        filters = self.spec.normalize_query_filters(filters)
        filters = self.spec.fill_default_filters(filters)

        database = self.spec.resolve_database(filters)
        table = self.spec.resolve_table(filters)

        df = self._align_columns(df)

        cols = df.columns.tolist()
        col_names = ",".join(self._escape_column(c) for c in cols)
        placeholders = ",".join(["%s"] * len(cols))

        update_cols = [c for c in cols if c not in self.spec.PRIMARY_KEYS]
        if not update_cols:
            raise ValueError(
                f"{self.spec.RESOURCE_NAME} has no non-primary-key columns for upsert"
            )

        update_clause = ", ".join(
            f"{self._escape_column(c)} = VALUES({self._escape_column(c)})"
            for c in update_cols
        )

        sql = f"""
        INSERT INTO {table} ({col_names})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause}
        """

        raw_values = df[cols].values.tolist()
        values = [
            [normalize_mysql_value(v) for v in row]
            for row in raw_values
        ]

        with self.get_conn(database) as conn:
            cursor = conn.cursor()
            for batch in chunked(values, self.BATCH_SIZE):
                cursor.executemany(sql, batch)

    def _align_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.spec.COLUMNS:
            return df.copy()

        result = df.copy()
        for col in self.spec.COLUMNS:
            if col not in result.columns:
                result[col] = None

        return result[self.spec.COLUMNS]

    def _build_where(
        self,
        filters: dict[str, Any],
        db_filter_specs: dict[str, dict[str, Any]],
    ) -> tuple[str, list[Any]]:
        clauses = []
        params = []

        for logical_field, value in filters.items():
            if value is None:
                continue

            if logical_field not in db_filter_specs:
                continue

            rule = db_filter_specs[logical_field]
            column = rule["column"]
            op = rule["op"]

            clause, clause_params = self._compile_filter_clause(column, op, value)
            if clause:
                clauses.append(clause)
                params.extend(clause_params)

        if clauses:
            return "WHERE " + " AND ".join(clauses), params

        return "", params

    def _compile_filter_clause(
        self,
        column: str,
        op: str,
        value: Any,
    ) -> tuple[str, list[Any]]:
        if op not in self.spec.SUPPORTED_DB_OPERATORS:
            raise ValueError(
                f"{self.spec.RESOURCE_NAME} unsupported db operator={op}"
            )

        col = self._escape_column(column)

        if op == "eq":
            return f"{col} = %s", [value]

        if op == "in":
            if isinstance(value, str):
                values = [value]
            elif isinstance(value, (list, tuple, set)):
                values = list(value)
            else:
                values = [value]

            if not values:
                return "1=0", []

            placeholders = ",".join(["%s"] * len(values))
            return f"{col} IN ({placeholders})", values

        if op == "gte":
            return f"{col} >= %s", [value]

        if op == "lte":
            return f"{col} <= %s", [value]

        if op == "gt":
            return f"{col} > %s", [value]

        if op == "lt":
            return f"{col} < %s", [value]

        raise ValueError(f"unsupported operator={op}")

    @staticmethod
    def _escape_column(col: str) -> str:
        return f"`{col}`"


class BaseClickHouseRepository:
    """
    Generic ClickHouse repository driven by spec.
    """

    CHUNK_SIZE = 5000

    def __init__(self, spec: BaseRQSpec):
        self.spec = spec
        self.client = ClickHouseClient()

    def query(self, **filters) -> pd.DataFrame:
        filters = {k: v for k, v in filters.items() if v is not None}
        filters = self.spec.normalize_query_filters(filters)
        filters = self.spec.fill_default_filters(filters)
        self.spec.validate_filters(filters, mode=FetchMode.DB_ONLY)
        filters = self.spec.normalize_db_query_filters(filters)

        chunk_field = self._find_chunkable_in_filter(filters)

        if chunk_field is None:
            return self._query_once(filters)

        all_parts = []
        values = list(filters[chunk_field])

        for batch in chunked(values, self.CHUNK_SIZE):
            batch_filters = dict(filters)
            batch_filters[chunk_field] = batch
            df_part = self._query_once(batch_filters)
            if df_part is not None and not df_part.empty:
                all_parts.append(df_part)

        if not all_parts:
            return pd.DataFrame()

        return pd.concat(all_parts, ignore_index=True)

    def _query_once(self, filters: dict[str, Any]) -> pd.DataFrame:
        database = self.spec.resolve_database(filters)
        table = self.spec.resolve_table(filters)
        db_filter_specs = self.spec.resolve_db_filter_specs(filters)
        where_sql = self._build_where(filters, db_filter_specs)

        sql = f"SELECT * FROM `{database}`.`{table}` FINAL {where_sql}"
        return self.client.query_df(sql, database=database)

    def _find_chunkable_in_filter(self, filters: dict[str, Any]) -> str | None:
        db_filter_specs = self.spec.resolve_db_filter_specs(filters)

        for logical_field, value in filters.items():
            if logical_field not in db_filter_specs:
                continue

            rule = db_filter_specs[logical_field]
            if rule.get("op") != "in":
                continue

            if isinstance(value, str):
                continue

            if isinstance(value, (list, tuple, set)) and len(value) > self.CHUNK_SIZE:
                return logical_field

        return None

    def insert_dataframe(self, df: pd.DataFrame, **filters) -> None:
        if df is None or df.empty:
            return

        filters = {k: v for k, v in filters.items() if v is not None}
        filters = self.spec.normalize_query_filters(filters)
        filters = self.spec.fill_default_filters(filters)

        database = self.spec.resolve_database(filters)
        table = self.spec.resolve_table(filters)
        df = self._align_columns(df)
        self.client.insert_dataframe(database=database, table=table, df=df)

    def _align_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.spec.COLUMNS:
            return df.copy()

        result = df.copy()
        for col in self.spec.COLUMNS:
            if col not in result.columns:
                result[col] = None

        return result[self.spec.COLUMNS]

    def _build_where(
        self,
        filters: dict[str, Any],
        db_filter_specs: dict[str, dict[str, Any]],
    ) -> str:
        clauses = []

        for logical_field, value in filters.items():
            if value is None or logical_field not in db_filter_specs:
                continue

            rule = db_filter_specs[logical_field]
            clause = self._compile_filter_clause(rule["column"], rule["op"], value)
            if clause:
                clauses.append(clause)

        if not clauses:
            return ""

        return "WHERE " + " AND ".join(clauses)

    def _compile_filter_clause(self, column: str, op: str, value: Any) -> str:
        col = f"`{column}`"

        if op == "eq":
            return f"{col} = {self._format_value(value)}"
        if op == "in":
            values = value if isinstance(value, (list, tuple, set)) else [value]
            values = list(values)
            if not values:
                return "0"
            return f"{col} IN ({', '.join(self._format_value(v) for v in values)})"
        if op == "gte":
            return f"{col} >= {self._format_value(value)}"
        if op == "lte":
            return f"{col} <= {self._format_value(value)}"
        if op == "gt":
            return f"{col} > {self._format_value(value)}"
        if op == "lt":
            return f"{col} < {self._format_value(value)}"

        raise ValueError(f"unsupported operator={op}")

    @staticmethod
    def _format_value(value: Any) -> str:
        if isinstance(value, pd.Timestamp):
            value = value.to_pydatetime()
        elif isinstance(value, np.datetime64):
            value = pd.to_datetime(value).to_pydatetime()

        if isinstance(value, datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
        if isinstance(value, date):
            return f"'{value.isoformat()}'"
        if isinstance(value, str):
            escaped = value.replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped}'"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float, np.integer, np.floating)):
            return str(value)
        if value is None:
            return "NULL"

        escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"

class BackendRoutingRepository:
    def __init__(
        self,
        spec: BaseRQSpec,
        *,
        mysql_repo_cls: type[BaseRQRepository] | None = None,
        clickhouse_repo_cls: type[BaseClickHouseRepository] | None = None,
    ):
        self.spec = spec
        self._mysql_repo = mysql_repo_cls(spec) if mysql_repo_cls is not None else None
        self._clickhouse_repo = (
            clickhouse_repo_cls(spec) if clickhouse_repo_cls is not None else None
        )

    def _select_repo(self, filters: dict[str, Any]):
        backend = self.spec.resolve_storage_backend(filters)
        if backend == "mysql":
            if self._mysql_repo is None:
                raise ValueError(f"{self.spec.RESOURCE_NAME} mysql repository is not configured")
            return self._mysql_repo
        if backend == "clickhouse":
            if self._clickhouse_repo is None:
                raise ValueError(
                    f"{self.spec.RESOURCE_NAME} clickhouse repository is not configured"
                )
            return self._clickhouse_repo
        raise ValueError(f"{self.spec.RESOURCE_NAME} unsupported storage backend={backend}")

    def query(self, **filters) -> pd.DataFrame:
        return self._select_repo(filters).query(**filters)

    def insert_ignore(self, df: pd.DataFrame, **filters) -> None:
        return self._select_repo(filters).insert_ignore(df, **filters)

    def upsert(self, df: pd.DataFrame, **filters) -> None:
        return self._select_repo(filters).upsert(df, **filters)

    def insert_dataframe(self, df: pd.DataFrame, **filters) -> None:
        repo = self._select_repo(filters)
        if not hasattr(repo, "insert_dataframe"):
            raise ValueError(f"{self.spec.RESOURCE_NAME} repository does not support insert_dataframe")
        return repo.insert_dataframe(df, **filters)


# ============================================================
# DataSource
# ============================================================

class BaseRQDataSource(ABC):
    """
    Generic rqdata datasource driven by spec.

    SOURCE_ONLY / DB_THEN_SOURCE 必须遵守 API 语义。
    """

    def __init__(self, spec: BaseRQSpec):
        self.spec = spec

    def fetch(self, **filters) -> pd.DataFrame:
        filters = {k: v for k, v in filters.items() if v is not None}
        filters = self.spec.normalize_query_filters(filters)
        filters = self.spec.fill_default_filters(filters)
        self.spec.validate_filters(filters, mode=FetchMode.SOURCE_ONLY)

        api_filters, post_filters = self.spec.split_filters(filters)

        df = self._call_api(**api_filters)
        df = self.spec.normalize_df(df, filters=filters)
        df = self.spec.filter_df(df, post_filters)

        return df

    @abstractmethod
    def _call_api(self, **api_filters) -> pd.DataFrame:
        raise NotImplementedError


# ============================================================
# Service
# ============================================================

class BaseRQService:
    """
    Unified service:
    - DB_ONLY
    - SOURCE_ONLY
    - DB_THEN_SOURCE

    Rules:
    - DB_ONLY: DB query semantics
    - SOURCE_ONLY / DB_THEN_SOURCE: API semantics
    """

    def __init__(
        self,
        *,
        spec: BaseRQSpec,
        repo: BaseRQRepository,
        source: BaseRQDataSource,
    ):
        self.spec = spec
        self.repo = repo
        self.source = source

    def get(
        self,
        *,
        mode: FetchMode = FetchMode.DB_THEN_SOURCE,
        persist: bool = True,
        refresh: bool = False,
        **filters,
    ) -> FetchResult:
        try:
            filters = {k: v for k, v in filters.items() if v is not None}
            filters = self.spec.normalize_query_filters(filters)
            filters = self.spec.fill_default_filters(filters)
            self.spec.validate_filters(filters, mode=mode)

            if mode == FetchMode.DB_ONLY:
                df = self.repo.query(**filters)
                return FetchResult(FetchStatus.SUCCESS, data=df)

            if mode == FetchMode.SOURCE_ONLY:
                df = self.source.fetch(**filters)
                if persist:
                    self._persist(df, filters)
                return FetchResult(FetchStatus.SUCCESS, data=df)

            # DB_THEN_SOURCE
            if refresh:
                df = self.source.fetch(**filters)
                if persist:
                    self._persist(df, filters)
                return FetchResult(FetchStatus.SUCCESS, data=df)

            df_db = self.repo.query(**filters)
            if not df_db.empty:
                return FetchResult(FetchStatus.SUCCESS, data=df_db)

            df_src = self.source.fetch(**filters)
            if persist:
                self._persist(df_src, filters)

            return FetchResult(FetchStatus.SUCCESS, data=df_src)

        except Exception as e:
            return FetchResult(FetchStatus.FAILED, error=e)

    def _persist(self, df: pd.DataFrame, filters: dict[str, Any]) -> None:
        if df is None or df.empty:
            return

        storage_backend = self.spec.resolve_storage_backend(filters)
        write_mode = self.spec.resolve_write_mode(filters)

        if storage_backend == "clickhouse":
            self.repo.insert_dataframe(df, **filters)
            return

        if write_mode == "snapshot_upsert":
            self.repo.upsert(df, **filters)
            return

        if write_mode == "timeseries_append":
            self.repo.insert_ignore(df, **filters)
            return

        raise ValueError(
            f"{self.spec.RESOURCE_NAME} unsupported write mode={write_mode} "
            f"for backend={storage_backend}"
        )
