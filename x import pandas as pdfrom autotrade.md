```
import pandas as pd
from autotrade.data.tushare.datasource.base import BaseDataSource
from autotrade.data.tushare.datasource.base_tushare import BaseTushareSource,TusharePaginator
from autotrade.coreutils.config import TushareInfo


class TushareOptBasicSource(BaseDataSource, BaseTushareSource):

    MAX_LIMIT = 10000

    FIELDS = [
        "ts_code",
        "exchange",
        "name",
        "per_unit",
        "opt_code",
        "opt_type",
        "call_put",
        "exercise_type",
        "exercise_price",
        "s_month",
        "maturity_date",
        "list_price",
        "list_date",
        "delist_date",
        "last_edate",
        "last_ddate",
        "quote_unit",
        "min_price_chg",
    ]

    def __init__(self, *, pro=None, token=None):
        BaseTushareSource.__init__(
            self,
            pro=pro,
            token=token,
            default_token=TushareInfo.token,
        )
        self.paginator = TusharePaginator(
            api_func=self.pro.opt_basic,
            limit=self.MAX_LIMIT,
        )

    def _fetch_impl(
        self,
        *,
        ts_code,
        exchange,
        date,
        start_date,
        end_date,
    ) -> pd.DataFrame:
        if start_date or end_date:
            raise ValueError("opt_basic does not support date range")

        filters = {
            "ts_code": ts_code or "",
            "exchange": exchange or "",
            "list_date": date or "",
        }

        return self.paginator.fetch(filters=filters, fields=self.FIELDS)


class TushareOptDailySource(BaseDataSource, BaseTushareSource):

    MAX_LIMIT = 15000

    FIELDS = [
        "ts_code",
        "trade_date",
        "exchange",
        "pre_settle",
        "pre_close",
        "open",
        "high",
        "low",
        "close",
        "settle",
        "vol",
        "amount",
        "oi",
    ]

    def __init__(self, *, pro=None, token=None):
        BaseTushareSource.__init__(
            self,
            pro=pro,
            token=token,
            default_token=TushareInfo.token,
        )
        self.paginator = TusharePaginator(
            api_func=self.pro.opt_daily,
            limit=self.MAX_LIMIT,
        )

    def _fetch_impl(
        self,
        *,
        ts_code,
        exchange,
        date,
        start_date,
        end_date,
    ) -> pd.DataFrame:
        # date 语义优先
        if date is not None:
            start_date = date
            end_date = date

        filters = {
            "ts_code": ts_code or "",
            "exchange": exchange or "",
            "start_date": start_date or "",
            "end_date": end_date or "",
        }

        return self.paginator.fetch(filters=filters, fields=self.FIELDS)
```





```
option_basic_repo
```

只定义表和数据库名称，还有字段映射

```
# autotrade/repository/option_basic_repo.py
import pandas as pd
from autotrade.data.tushare.repository.base import BaseRepository

class OptionBasicRepository(BaseRepository):
    DATABASE = "option_data"
    TABLE = "option_basic"

    TS_CODE_FIELD = "ts_code"
    EXCHANGE_FIELD = "exchange"
    DATE_FIELD = "list_date"

    def query(
        self,
        *,
        ts_code=None,
        exchange=None,
        date=None,
        start_date=None,
        end_date=None,
    ):
        if start_date or end_date:
            raise ValueError(
                "option_basic does not support date range query; "
                "use `date` (list_date) instead"
            )

        return super().query(
            ts_code=ts_code,
            exchange=exchange,
            date=date,
            start_date=start_date,
            end_date=end_date,
        )

class OptionDailyRepository(BaseRepository):
    DATABASE = "option_data"
    TABLE = "option_daily"

    TS_CODE_FIELD = "ts_code"
    EXCHANGE_FIELD = "exchange"
    DATE_FIELD = "trade_date"
```



在base里面具体查询

```
# autotrade/repository/base.py
import pymysql
import pandas as pd
from contextlib import contextmanager
from autotrade.coreutils.config import DatabaseInfo
import warnings




class BaseRepository:
    """
    Base repository with unified query semantics.

    This class:
    - DOES NOT assume any field exists
    - ONLY builds SQL according to declared field mappings
    """

    TABLE: str
    DATABASE: str | None = None   # 由子类指定

    DATE_FIELD: str | None = None
    TS_CODE_FIELD: str | None = None
    EXCHANGE_FIELD: str | None = None
    # ---- 子类必须显式声明（不存在就设 None）----
    DATE_FIELD: str | None = None
    TS_CODE_FIELD: str | None = None
    EXCHANGE_FIELD: str | None = None

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
            database=self.DATABASE,      # ← 关键
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
    # 统一查询入口（无业务判断）
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
    # ===============================
    # WHERE 构造（纯映射，不校验）
    # ===============================
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
    # 通用 insert
    # ===============================
    def insert_ignore(self, df: pd.DataFrame):
        if df.empty:
            return

        cols = df.columns.tolist()
        placeholders = ",".join(["%s"] * len(cols))
        col_names = ",".join(cols)

        sql = f"""
        INSERT IGNORE INTO {self.TABLE} ({col_names})
        VALUES ({placeholders})
        """

        with self.get_conn() as conn:
            cursor = conn.cursor()
            cursor.executemany(sql, df[cols].values.tolist())
```