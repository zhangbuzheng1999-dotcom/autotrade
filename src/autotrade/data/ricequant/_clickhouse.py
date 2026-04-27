from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import clickhouse_connect
import numpy as np
import pandas as pd

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
