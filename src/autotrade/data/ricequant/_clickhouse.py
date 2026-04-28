from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
import warnings

import clickhouse_connect
import numpy as np
import pandas as pd
from clickhouse_connect.driver.exceptions import DatabaseError

from autotrade.coreutils.config import ClickHouseInfo


def normalize_clickhouse_value(v: Any, *, column_name: str | None = None) -> Any:
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    if v is None:
        return None

    if isinstance(v, (pd.Timestamp, np.datetime64)):
        ts = pd.to_datetime(v)
        if column_name is not None and column_name.endswith("date") and column_name != "datetime":
            return ts.date()
        return ts.to_pydatetime()

    if isinstance(v, datetime):
        if column_name is not None and column_name.endswith("date") and column_name != "datetime":
            return v.date()
        return v

    if isinstance(v, date):
        return v

    if isinstance(v, Decimal):
        return float(v)

    if isinstance(v, np.integer):
        return int(v)

    if isinstance(v, np.floating):
        value = float(v)
        if np.isnan(value) or np.isinf(value):
            return None
        return value

    return v


class ClickHouseClient:
    MAX_PARTITIONS_PER_INSERT_BATCH = 90
    FALLBACK_ROW_BATCH_SIZE = 5000

    def __init__(self):
        self.host = ClickHouseInfo.host
        self.http_port = ClickHouseInfo.http_port
        self.user = ClickHouseInfo.user
        self.password = ClickHouseInfo.password
        self.default_database = ClickHouseInfo.database
        self._clients: dict[str, Any] = {}

    def _get_client(self, database: str | None = None):
        target_database = database or self.default_database
        if target_database not in self._clients:
            self._clients[target_database] = clickhouse_connect.get_client(
                host=self.host,
                port=self.http_port,
                username=self.user,
                password=self.password,
                database=target_database,
            )
        return self._clients[target_database]

    def execute(self, sql: str, *, database: str | None = None) -> None:
        client = self._get_client(database)
        client.command(sql)

    def query_df(self, sql: str, *, database: str | None = None) -> pd.DataFrame:
        client = self._get_client(database)
        result = client.query_df(sql)
        if result is None:
            return pd.DataFrame()
        return result

    def insert_dataframe(
            self,
            *,
            database: str,
            table: str,
            df: pd.DataFrame,
    ) -> None:
        if df is None or df.empty:
            return

        client = self._get_client(database)
        df = df.copy()
        column_names = list(df.columns)

        try:
            self._insert_rows(client, table=table, df=df, column_names=column_names)
            return
        except DatabaseError as exc:
            if not self._is_too_many_partitions_error(exc):
                raise

        warnings.warn(
            f"ClickHouse insert into {database}.{table} hit too many partitions in one block; "
            "retrying with batched inserts.",
            RuntimeWarning,
            stacklevel=2,
        )

        time_col = self._detect_time_column(df)
        if time_col is not None:
            try:
                for batch_df in self._build_time_partition_batches(df, time_col):
                    self._insert_rows(client, table=table, df=batch_df, column_names=column_names)
                return
            except DatabaseError as exc:
                if not self._is_too_many_partitions_error(exc):
                    raise
                warnings.warn(
                    f"Time-partitioned retry for {database}.{table} still hit partition limits; "
                    "falling back to row batches.",
                    RuntimeWarning,
                    stacklevel=2,
                )
        else:
            warnings.warn(
                f"Could not detect a time column for {database}.{table}; falling back to row batches.",
                RuntimeWarning,
                stacklevel=2,
            )

        for batch_df in self._build_row_batches(df):
            self._insert_rows(client, table=table, df=batch_df, column_names=column_names)

    def _insert_rows(self, client, *, table: str, df: pd.DataFrame, column_names: list[str]) -> None:
        if df.empty:
            return

        data = []
        for row in df.itertuples(index=False, name=None):
            normalized_row = [
                normalize_clickhouse_value(v, column_name=col)
                for col, v in zip(column_names, row)
            ]
            data.append(normalized_row)

        client.insert(
            table=table,
            data=data,
            column_names=column_names,
            settings={
                "async_insert": 1,
                "wait_for_async_insert": 1,
            },
        )

    @staticmethod
    def _is_too_many_partitions_error(exc: Exception) -> bool:
        message = str(exc)
        return "Code: 252" in message or "Too many partitions for single INSERT block" in message

    @staticmethod
    def _detect_time_column(df: pd.DataFrame) -> str | None:
        preferred = ["datetime", "date", "trading_date"]
        for col in preferred:
            if col in df.columns:
                return col

        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                return col

        for col in df.columns:
            lowered = col.lower()
            if "time" not in lowered and "date" not in lowered:
                continue
            parsed = pd.to_datetime(df[col], errors="coerce", format="mixed")
            if parsed.notna().any():
                return col

        return None

    def _build_time_partition_batches(self, df: pd.DataFrame, time_col: str) -> list[pd.DataFrame]:
        parsed = pd.to_datetime(df[time_col], errors="coerce", format="mixed")
        working = df.copy()
        working["_partition_key"] = parsed.dt.to_period("M").astype("string")
        working["_sort_key"] = parsed
        working = working.sort_values(["_partition_key", "_sort_key"], kind="stable")

        batches: list[pd.DataFrame] = []
        current_partition_keys: list[str] = []
        current_parts: list[pd.DataFrame] = []

        for partition_key, part_df in working.groupby("_partition_key", dropna=False, sort=False):
            if len(current_partition_keys) >= self.MAX_PARTITIONS_PER_INSERT_BATCH:
                batch_df = pd.concat(current_parts, ignore_index=True).drop(
                    columns=["_partition_key", "_sort_key"],
                    errors="ignore",
                )
                batches.append(batch_df)
                current_partition_keys = []
                current_parts = []

            current_partition_keys.append(str(partition_key))
            current_parts.append(part_df)

        if current_parts:
            batch_df = pd.concat(current_parts, ignore_index=True).drop(
                columns=["_partition_key", "_sort_key"],
                errors="ignore",
            )
            batches.append(batch_df)

        return batches

    def _build_row_batches(self, df: pd.DataFrame) -> list[pd.DataFrame]:
        return [
            df.iloc[start:start + self.FALLBACK_ROW_BATCH_SIZE].copy()
            for start in range(0, len(df), self.FALLBACK_ROW_BATCH_SIZE)
        ]
