import pandas as pd
from autotrade.data.tushare.datasource.base import BaseDataSource
from autotrade.coreutils.config import TushareInfo
import tushare as ts
from typing import Optional
import time
from typing import Callable


# ================分页器==================
class BaseTushareSource:
    """
    Base class for Tushare-based data sources.

    Rule:
    - NEVER use ts.set_token
    - ALWAYS construct pro explicitly
    """

    def __init__(
            self,
            *,
            pro=None,
            token: Optional[str] = None,
            default_token: Optional[str] = None,
            http_url: str = "https://jiaoch.site",
    ):
        # 1️⃣ 外部传入 pro，最高优先级（用于测试 / mock）
        if pro is not None:
            self.pro = pro
            return

        # 2️⃣ 决定 token
        token = token or default_token
        if not token:
            raise ValueError("No tushare token provided")

        # 3️⃣ 显式构造 pro（关键）
        pro = ts.pro_api(token)

        # 🔴 私有源必须显式设置
        pro._DataApi__token = token
        pro._DataApi__http_url = http_url

        self.pro = pro


class TushareExecutor:
    """
    Unified executor for tushare APIs.

    Responsibilities:
    - pagination
    - retry
    - code / code_list dispatch
    """

    def __init__(
            self,
            *,
            api_func: Callable,
            limit: int,
            build_filters: Callable,
            sleep: float = 0.3,
            max_retry: int = 3,
            retry_sleep: float = 1.0,
    ):
        self.api_func = api_func
        self.limit = limit
        self.build_filters = build_filters
        self.sleep = sleep
        self.max_retry = max_retry
        self.retry_sleep = retry_sleep

    # ===============================
    # public entry
    # ===============================
    def fetch(
            self,
            *,
            code=None,
            code_list=None,
            fields=None,
            **kwargs,
    ) -> pd.DataFrame:

        if code and code_list:
            raise ValueError("Only one of code or code_list allowed")

        # -------- single code / no code_list --------
        if code or not code_list:
            filters = self.build_filters(code=code, **kwargs)
            return self._fetch_paginated(filters, fields)

        # -------- code_list --------
        dfs = []
        for c in code_list:
            filters = self.build_filters(code=c, **kwargs)
            df = self._fetch_paginated(filters, fields)
            if df is not None and not df.empty:
                dfs.append(df)
            time.sleep(self.sleep)

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)

    # ===============================
    # internals
    # ===============================
    def _fetch_paginated(self, filters: dict, fields):
        offset = 0
        dfs = []

        while True:
            params = dict(filters)
            params["limit"] = self.limit
            params["offset"] = offset

            df = self._fetch_with_retry(params, fields)
            if df is None or df.empty:
                break

            dfs.append(df)

            if len(df) < self.limit:
                break

            offset += self.limit
            time.sleep(self.sleep)

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)

    def _fetch_with_retry(self, params: dict, fields):
        for attempt in range(1, self.max_retry + 1):
            try:
                return self.api_func(**params, fields=fields)
            except Exception as e:
                if attempt >= self.max_retry:
                    raise RuntimeError(
                        f"Tushare request failed after {self.max_retry} retries, "
                        f"params={params}"
                    ) from e
                time.sleep(self.retry_sleep)


# ===============期权数据===================
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

        self.executor = TushareExecutor(
            api_func=self.pro.opt_basic,
            limit=self.MAX_LIMIT,
            build_filters=self._build_filters,
        )

    # ===============================
    # semantic → tushare mapping
    # ===============================
    def _build_filters(self, *, code, exchange=None, date=None, **_):
        return {
            "ts_code": code or "",
            "exchange": exchange or "",
            "list_date": date or "",
        }

    # ===============================
    # semantic enforcement
    # ===============================
    def _fetch_impl(
            self,
            *,
            code=None,
            code_list=None,
            exchange=None,
            date=None,
            start_date=None,
            end_date=None,
    ) -> pd.DataFrame:
        if start_date or end_date:
            raise ValueError("opt_basic does not support date range")

        return self.executor.fetch(
            code=code,
            code_list=code_list,
            exchange=exchange,
            date=date,
            fields=self.FIELDS,
        )

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

        self.executor = TushareExecutor(
            api_func=self.pro.opt_daily,
            limit=self.MAX_LIMIT,
            build_filters=self._build_filters,
        )

    # ===============================
    # semantic → tushare mapping
    # ===============================
    def _build_filters(
        self,
        *,
        code,
        exchange=None,
        date=None,
        start_date=None,
        end_date=None,
        **_,
    ):
        # 单日语义 → 区间
        if date is not None:
            start_date = date
            end_date = date

        return {
            "ts_code": code or "",
            "exchange": exchange or "",
            "start_date": start_date or "",
            "end_date": end_date or "",
        }

    # ===============================
    # semantic enforcement
    # ===============================
    def _fetch_impl(
        self,
        *,
        code=None,
        code_list=None,
        exchange=None,
        date=None,
        start_date=None,
        end_date=None,
    ) -> pd.DataFrame:
        return self.executor.fetch(
            code=code,
            code_list=code_list,
            exchange=exchange,
            date=date,
            start_date=start_date,
            end_date=end_date,
            fields=self.FIELDS,
        )

# ===============ETF数据===================
class TushareEtfBasicSource(BaseDataSource, BaseTushareSource):
    MAX_LIMIT = 5000

    FIELDS = [
        "ts_code",
        "csname",
        "extname",
        "cname",
        "index_code",
        "index_name",
        "setup_date",
        "list_date",
        "list_status",
        "exchange",
        "mgr_name",
        "custod_name",
        "mgt_fee",
        "etf_type",
    ]

    def __init__(self, *, pro=None, token=None):
        BaseTushareSource.__init__(
            self,
            pro=pro,
            token=token,
            default_token=TushareInfo.token,
        )

        self.executor = TushareExecutor(
            api_func=self.pro.etf_basic,
            limit=self.MAX_LIMIT,
            build_filters=self._build_filters,
        )

    # ===============================
    # semantic → tushare mapping
    # ===============================
    def _build_filters(self, *, code, exchange=None, date=None, **_):
        return {
            "ts_code": code or "",
            "exchange": exchange or "",
            "list_date": date or "",
        }

    # ===============================
    # semantic enforcement
    # ===============================
    def _fetch_impl(
        self,
        *,
        code=None,
        code_list=None,
        exchange=None,
        date=None,
        start_date=None,
        end_date=None,
    ):
        if start_date or end_date:
            raise ValueError("etf_basic does not support date range")

        return self.executor.fetch(
            code=code,
            code_list=code_list,
            exchange=exchange,
            date=date,
            fields=self.FIELDS,
        )

class TushareEtfFundDaily(BaseDataSource, BaseTushareSource):
    MAX_LIMIT = 2000

    FIELDS = [
        "ts_code",
        "trade_date",
        "pre_close",
        "open",
        "high",
        "low",
        "close",
        "change",
        "pct_chg",
        "vol",
        "amount",
    ]

    def __init__(self, *, pro=None, token=None):
        BaseTushareSource.__init__(
            self,
            pro=pro,
            token=token,
            default_token=TushareInfo.token,
        )

        self.executor = TushareExecutor(
            api_func=self.pro.fund_daily,
            limit=self.MAX_LIMIT,
            build_filters=self._build_filters,
        )

    # ===============================
    # semantic → tushare mapping
    # ===============================
    def _build_filters(
        self,
        *,
        code,
        date=None,
        start_date=None,
        end_date=None,
        **_,
    ):
        if date is not None:
            start_date = date
            end_date = date

        return {
            "ts_code": code or "",
            "start_date": start_date or "",
            "end_date": end_date or "",
        }

    # ===============================
    # semantic enforcement
    # ===============================
    def _fetch_impl(
        self,
        *,
        code=None,
        code_list=None,
        exchange=None,
        date=None,
        start_date=None,
        end_date=None,
    ):
        if exchange:
            raise ValueError("fund_daily does not support exchange")

        return self.executor.fetch(
            code=code,
            code_list=code_list,
            date=date,
            start_date=start_date,
            end_date=end_date,
            fields=self.FIELDS,
        )

class TushareEtfFundAdj(BaseDataSource, BaseTushareSource):
    MAX_LIMIT = 2000

    FIELDS = [
        "ts_code",
        "trade_date",
        "adj_factor",
    ]

    def __init__(self, *, pro=None, token=None):
        BaseTushareSource.__init__(
            self,
            pro=pro,
            token=token,
            default_token=TushareInfo.token,
        )

        self.executor = TushareExecutor(
            api_func=self.pro.fund_adj,
            limit=self.MAX_LIMIT,
            build_filters=self._build_filters,
        )

    # ===============================
    # semantic → tushare mapping
    # ===============================
    def _build_filters(
        self,
        *,
        code,
        date=None,
        start_date=None,
        end_date=None,
        **_,
    ):
        if date is not None:
            start_date = date
            end_date = date

        return {
            "ts_code": code or "",
            "start_date": start_date or "",
            "end_date": end_date or "",
        }

    # ===============================
    # semantic enforcement
    # ===============================
    def _fetch_impl(
        self,
        *,
        code=None,
        code_list=None,
        exchange=None,
        date=None,
        start_date=None,
        end_date=None,
    ):
        if exchange:
            raise ValueError("fund_adj does not support exchange")

        return self.executor.fetch(
            code=code,
            code_list=code_list,
            date=date,
            start_date=start_date,
            end_date=end_date,
            fields=self.FIELDS,
        )


class TushareFutBasicSource(BaseDataSource, BaseTushareSource):
    MAX_LIMIT = 10000

    FIELDS = [
        "ts_code",
        "symbol",
        "exchange",
        "name",
        "fut_code",
        "multiplier",
        "trade_unit",
        "per_unit",
        "quote_unit",
        "quote_unit_desc",
        "d_mode_desc",
        "list_date",
        "delist_date",
        "d_month",
        "last_ddate",
    ]

    def __init__(self, *, pro=None, token=None):
        BaseTushareSource.__init__(
            self,
            pro=pro,
            token=token,
            default_token=TushareInfo.token,
        )

        self.executor = TushareExecutor(
            api_func=self.pro.fut_basic,
            limit=self.MAX_LIMIT,
            build_filters=self._build_filters,
        )

    # ===============================
    # semantic → tushare mapping
    # ===============================
    def _build_filters(self, *, code, exchange=None, date=None, **_):
        return {
            "ts_code": code or "",
            "exchange": exchange or "",
            "list_date": date or "",
        }

    # ===============================
    # semantic enforcement
    # ===============================
    def _fetch_impl(
        self,
        *,
        code=None,
        code_list=None,
        exchange=None,
        date=None,
        start_date=None,
        end_date=None,
    ) -> pd.DataFrame:
        if start_date or end_date:
            raise ValueError("fut_basic does not support date range")

        return self.executor.fetch(
            code=code,
            code_list=code_list,
            exchange=exchange,
            date=date,
            fields=self.FIELDS,
        )
class TushareFutDaily(BaseDataSource, BaseTushareSource):
    MAX_LIMIT = 2000

    FIELDS = [
        "ts_code",
        "trade_date",
        "pre_close",
        "pre_settle",
        "open",
        "high",
        "low",
        "close",
        "settle",
        "change1",
        "change2",
        "vol",
        "amount",
        "oi",
        "oi_chg",
    ]

    def __init__(self, *, pro=None, token=None):
        BaseTushareSource.__init__(
            self,
            pro=pro,
            token=token,
            default_token=TushareInfo.token,
        )

        self.executor = TushareExecutor(
            api_func=self.pro.fut_daily,
            limit=self.MAX_LIMIT,
            build_filters=self._build_filters,
        )

    # ===============================
    # semantic → tushare mapping
    # ===============================
    def _build_filters(
        self,
        *,
        code,
        exchange=None,
        date=None,
        start_date=None,
        end_date=None,
        **_,
    ):
        # date 语义优先
        if date is not None:
            start_date = date
            end_date = date

        return {
            "ts_code": code or "",
            "exchange": exchange or "",
            "start_date": start_date or "",
            "end_date": end_date or "",
        }

    # ===============================
    # semantic enforcement
    # ===============================
    def _fetch_impl(
        self,
        *,
        code=None,
        code_list=None,
        exchange=None,
        date=None,
        start_date=None,
        end_date=None,
    ) -> pd.DataFrame:
        return self.executor.fetch(
            code=code,
            code_list=code_list,
            exchange=exchange,
            date=date,
            start_date=start_date,
            end_date=end_date,
            fields=self.FIELDS,
        )


class TushareIndexBasicSource(BaseDataSource, BaseTushareSource):
    MAX_LIMIT = 10000

    FIELDS = [
        "ts_code",
        "symbol",
        "exchange",
        "name",
        "fut_code",
        "multiplier",
        "trade_unit",
        "per_unit",
        "quote_unit",
        "quote_unit_desc",
        "d_mode_desc",
        "list_date",
        "delist_date",
        "d_month",
        "last_ddate",
    ]

    def __init__(self, *, pro=None, token=None):
        BaseTushareSource.__init__(
            self,
            pro=pro,
            token=token,
            default_token=TushareInfo.token,
        )

        self.executor = TushareExecutor(
            api_func=self.pro.index_basic,
            limit=self.MAX_LIMIT,
            build_filters=self._build_filters,
        )

    # ===============================
    # semantic → tushare mapping
    # ===============================
    def _build_filters(self, *, code, exchange=None, date=None, **_):
        return {
            "ts_code": code or "",
            "exchange": exchange or "",
            "list_date": date or "",
        }

    # ===============================
    # semantic enforcement
    # ===============================
    def _fetch_impl(
        self,
        *,
        code=None,
        code_list=None,
        exchange=None,
        date=None,
        start_date=None,
        end_date=None,
    ) -> pd.DataFrame:
        if start_date or end_date:
            raise ValueError("fut_basic does not support date range")

        return self.executor.fetch(
            code=code,
            code_list=code_list,
            exchange=exchange,
            date=date,
            fields=self.FIELDS,
        )

class TushareIndexDaily(BaseDataSource, BaseTushareSource):
    MAX_LIMIT = 8000

    FIELDS = [
        "ts_code",
        "trade_date",
        "pre_close",
        "pre_settle",
        "open",
        "high",
        "low",
        "close",
        "settle",
        "change1",
        "change2",
        "vol",
        "amount",
        "oi",
        "oi_chg",
    ]

    def __init__(self, *, pro=None, token=None):
        BaseTushareSource.__init__(
            self,
            pro=pro,
            token=token,
            default_token=TushareInfo.token,
        )

        self.executor = TushareExecutor(
            api_func=self.pro.index_daily,
            limit=self.MAX_LIMIT,
            build_filters=self._build_filters,
        )

    # ===============================
    # semantic → tushare mapping
    # ===============================
    def _build_filters(
        self,
        *,
        code,
        exchange=None,
        date=None,
        start_date=None,
        end_date=None,
        **_,
    ):
        # date 语义优先
        if date is not None:
            start_date = date
            end_date = date

        return {
            "ts_code": code or "",
            "exchange": exchange or "",
            "start_date": start_date or "",
            "end_date": end_date or "",
        }

    # ===============================
    # semantic enforcement
    # ===============================
    def _fetch_impl(
        self,
        *,
        code=None,
        code_list=None,
        exchange=None,
        date=None,
        start_date=None,
        end_date=None,
    ) -> pd.DataFrame:
        return self.executor.fetch(
            code=code,
            code_list=code_list,
            exchange=exchange,
            date=date,
            start_date=start_date,
            end_date=end_date,
            fields=self.FIELDS,
        )

if __name__ == "__main__":
    from autotrade.coreutils.config import load_env

    load_env("d:/.env")
    from autotrade.data.tushare.init_tushare_db import create_index_data,create_etf_data
    create_index_data()
    create_etf_data()
    basic = TushareOptDailySource()
    res = basic.fetch(code_list=['LH2603-C-11800.DCE', 'LH2603-C-11600.DCE'])

    import tushare as ts
    import pandas as pd
    # token秘钥（把给咱们的token复制过来哈）
    token = "3c6dc3a8a4f8347f1edbe7eb3280a058886a2ef38f6d123fbfa18bfdaa6e"
    pro = ts.pro_api(token)
    pro._DataApi__token = token  # 保证有这个代码，不然不可以获取
    pro._DataApi__http_url = 'https://jiaoch.site'  # 保证有这个代码，不然不可以获取
    # 测试接口(换成自己的接口）
    res = pro.fut_basic(ts_code='SN1507.SHF')
    pro.opt_daily(start_date='20190101', end_date='20190102')
    df = pro.opt_daily(trade_date='20181212')

    df = pro.index_daily(ts_code='000016.SH')


