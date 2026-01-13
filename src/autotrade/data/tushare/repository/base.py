import pymysql
import pandas as pd
from contextlib import contextmanager
from autotrade.coreutils.config import DatabaseInfo
import warnings
import logging
import math
import numpy as np
from datetime import date, datetime
from decimal import Decimal
from typing import Iterable


# ===============================
# 日志配置（只配置一次）
# ===============================
logger = logging.getLogger("autotrade.repository")

if not logger.handlers:
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # 1文件日志
    file_handler = logging.FileHandler(
        "repository_error.log", encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.WARNING)

    # 2控制台日志（打印出来）
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.WARNING)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.setLevel(logging.WARNING)


# ===============================
# MySQL 值归一化（最终兜底）
# ===============================
def normalize_mysql_value(v):
    if v is None:
        return None

    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return v

    if isinstance(v, (np.floating,)):
        v = float(v)
        if math.isnan(v) or math.isinf(v):
            return None
        return v

    if isinstance(v, (pd.Timestamp, np.datetime64)):
        if pd.isna(v):
            return None
        return pd.to_datetime(v).to_pydatetime()

    if isinstance(v, (datetime, date)):
        return v

    if isinstance(v, Decimal):
        return v

    if isinstance(v, (int, np.integer)):
        return int(v)

    if isinstance(v, str):
        return v

    if pd.isna(v):
        return None

    # 兜底：强制字符串化，保证不炸库
    return str(v)


def chunked(iterable: list, size: int) -> Iterable[list]:
    """把大列表拆成小批次"""
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


# ===============================
# BaseRepository
# ===============================
class BaseRepository:
    """
    Base repository with unified query semantics
    and production-grade fault tolerance.
    """

    TABLE: str
    DATABASE: str | None = None

    DATE_FIELD: str | None = None
    TS_CODE_FIELD: str | None = None
    EXCHANGE_FIELD: str | None = None

    # 批量参数（可按表覆盖）
    BATCH_SIZE = 2000

    def __init__(self):
        if not self.DATABASE:
            raise ValueError(
                f"{self.__class__.__name__} must define DATABASE"
            )

        self._conn_args = dict(
            host=DatabaseInfo.host,
            port=DatabaseInfo.port,
            user=DatabaseInfo.user,
            passwd=DatabaseInfo.password,
            database=self.DATABASE,
            charset="utf8mb4",
            autocommit=True,
        )

    @contextmanager
    def get_conn(self):
        conn = pymysql.connect(**self._conn_args)
        try:
            yield conn
        finally:
            conn.close()

    # ===============================
    # 查询（不变）
    # ===============================
    def query(
        self,
        *,
        ts_code: str | None = None,
        exchange: str | None = None,
        date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        where, params = self._build_where(
            ts_code=ts_code,
            exchange=exchange,
            date=date,
            start_date=start_date,
            end_date=end_date,
        )

        sql = f"""
        SELECT *
        FROM {self.TABLE}
        {where}
        """

        with self.get_conn() as conn:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                return pd.read_sql(sql, conn, params=params)

    def _build_where(
        self,
        *,
        ts_code,
        exchange,
        date,
        start_date,
        end_date,
    ):
        clauses = []
        params = []

        if ts_code is not None and self.TS_CODE_FIELD:
            clauses.append(f"{self.TS_CODE_FIELD} = %s")
            params.append(ts_code)

        if exchange is not None and self.EXCHANGE_FIELD:
            clauses.append(f"{self.EXCHANGE_FIELD} = %s")
            params.append(exchange)

        if date is not None and self.DATE_FIELD:
            clauses.append(f"{self.DATE_FIELD} = %s")
            params.append(date)

        if (start_date is not None or end_date is not None) and self.DATE_FIELD:
            clauses.append(f"{self.DATE_FIELD} BETWEEN %s AND %s")
            params.extend([start_date, end_date])

        if clauses:
            return "WHERE " + " AND ".join(clauses), params
        return "", params

    # ===============================
    # 生产级 insert_ignore
    # ===============================
    def insert_ignore(self, df: pd.DataFrame):
        if df.empty:
            return

        cols = df.columns.tolist()
        placeholders = ",".join(["%s"] * len(cols))
        col_names = ",".join(self._escape_column(c) for c in cols)

        sql = f"""
        INSERT IGNORE INTO {self.TABLE} ({col_names})
        VALUES ({placeholders})
        """

        raw_values = df[cols].values.tolist()
        values = [
            [normalize_mysql_value(v) for v in row]
            for row in raw_values
        ]

        with self.get_conn() as conn:
            cursor = conn.cursor()

            # ===== 第一层：大批量写入 =====
            try:
                cursor.executemany(sql, values)
                return
            except Exception as e:
                logger.warning(
                    f"[BATCH FAILED] table={self.TABLE}, rows={len(values)}, error={e}"
                )

            # ===== 第二层：分批写入（避免全表回退到逐行）=====
            for batch in chunked(values, self.BATCH_SIZE):
                try:
                    cursor.executemany(sql, batch)
                except Exception as e:
                    logger.warning(
                        f"[SUB-BATCH FAILED] table={self.TABLE}, batch_size={len(batch)}, error={e}"
                    )

                    # ===== 第三层：逐行隔离异常 =====
                    for row in batch:
                        try:
                            cursor.execute(sql, row)
                        except Exception as row_e:
                            logger.error(
                                f"[ROW FAILED] table={self.TABLE}, row={row}, error={row_e}"
                            )

    def _escape_column(self, col: str) -> str:
        # MySQL 关键字 or 保守策略：统一加反引号
        return f"`{col}`"