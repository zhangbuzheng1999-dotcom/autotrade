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
        self.client = clickhouse_connect.get_client(
            host=ClickHouseInfo.host,
            port=ClickHouseInfo.http_port,
            username=ClickHouseInfo.user,
            password=ClickHouseInfo.password,
            database=ClickHouseInfo.database,
        )
        self.default_database = ClickHouseInfo.database

    def execute(self, sql: str, *, database: str | None = None) -> None:
        self.client.command(sql, settings=None)

    def query_df(self, sql: str, *, database: str | None = None) -> pd.DataFrame:
        result = self.client.query_df(sql)
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

        df = df.copy()
        for col in df.columns:
            df[col] = df[col].map(lambda v: normalize_clickhouse_value(v, column_name=col))

        self.client.insert_df(
            table=f"{database}.{table}",
            df=df,
            settings={
                "async_insert": 1,
                "wait_for_async_insert": 1,
            },
        )
