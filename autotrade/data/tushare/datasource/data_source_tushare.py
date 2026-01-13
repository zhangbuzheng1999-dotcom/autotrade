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

    Priority:
    1. pro (explicit)
    2. token (explicit)
    3. token from config
    """

    def __init__(self, *, pro=None, token: Optional[str] = None, default_token: str | None = None):
        if pro is not None:
            self.pro = pro
            return

        if token is not None:
            pro = ts.pro_api(token)
            pro._DataApi__token = token  # 保证有这个代码，不然不可以获取
            pro._DataApi__http_url = 'https://jiaoch.site'  # 保证有这个代码，不然不可以获取
            self.pro = pro
            return

        if default_token is None:
            raise ValueError("No tushare token provided")

        ts.set_token(default_token)
        self.pro = ts.pro_api()


class TusharePaginator:
    """
    Generic paginator for Tushare pro APIs with retry support.
    """

    def __init__(
            self,
            api_func: Callable,
            *,
            limit: int,
            sleep: float = 0.3,
            max_retry: int = 3,
            retry_sleep: float = 1.0,
    ):
        self.api_func = api_func
        self.limit = limit
        self.sleep = sleep
        self.max_retry = max_retry
        self.retry_sleep = retry_sleep

    def fetch(
            self,
            filters: dict,
            fields: list[str] | None = None,
    ) -> pd.DataFrame:
        offset = 0
        dfs: list[pd.DataFrame] = []

        while True:
            params = dict(filters)
            params["limit"] = self.limit
            params["offset"] = offset
            df = self._fetch_with_retry(params, fields)

            # 拉不到数据，直接结束
            if df is None or df.empty:
                break

            dfs.append(df)

            if len(df) < self.limit:
                break

            print(f'QUERY DATA :{offset}')
            offset += self.limit
            time.sleep(self.sleep)

        if not dfs:
            return pd.DataFrame()

        dfs = [df for df in dfs if df is not None and not df.empty]

        return pd.concat(dfs, ignore_index=True)

    def _fetch_with_retry(self, params: dict, fields: list[str] | None):
        """
        Fetch one page with retry.
        """
        last_exception = None

        for attempt in range(1, self.max_retry + 1):
            try:
                return self.api_func(**params, fields=fields)

            except Exception as e:
                last_exception = e
                if attempt < self.max_retry:
                    time.sleep(self.retry_sleep)
                else:
                    raise RuntimeError(
                        f"Tushare request failed after {self.max_retry} retries, "
                        f"params={params}"
                    ) from e


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
            token=TushareInfo.token,
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
            token=TushareInfo.token,
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
        "etf_type"
    ]

    def __init__(self, *, pro=None, token=None):
        BaseTushareSource.__init__(
            self,
            token=TushareInfo.token,
        )
        self.paginator = TusharePaginator(
            api_func=self.pro.etf_basic,
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
        if start_date or end_date:
            raise ValueError("opt_basic does not support date range")

        filters = {
            "ts_code": ts_code or "",
            "exchange": exchange or "",
            "list_date": date or "",
        }

        return self.paginator.fetch(filters=filters, fields=self.FIELDS)


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
        "amount"
    ]

    def __init__(self, *, pro=None, token=None):
        BaseTushareSource.__init__(
            self,
            token=TushareInfo.token,
        )
        self.paginator = TusharePaginator(
            api_func=self.pro.fund_daily,
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


class TushareEtfFundAdj(BaseDataSource, BaseTushareSource):
    MAX_LIMIT = 2000

    FIELDS = [
        "ts_code",
        "trade_date",
        "adj_factor"
    ]

    def __init__(self, *, pro=None, token=None):
        BaseTushareSource.__init__(
            self,
            token=TushareInfo.token,
        )
        self.paginator = TusharePaginator(
            api_func=self.pro.fund_adj,
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
        if exchange:
            raise ValueError("TushareEtfFundDaily does not support exchange")
        # date 语义优先
        if date is not None:
            start_date = date
            end_date = date

        filters = {
            "ts_code": ts_code or "",
            "start_date": start_date or "",
            "end_date": end_date or "",
        }

        return self.paginator.fetch(filters=filters, fields=self.FIELDS)

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
        "last_ddate"
    ]

    def __init__(self, *, pro=None, token=None):
        BaseTushareSource.__init__(
            self,
            token=TushareInfo.token,
        )
        self.paginator = TusharePaginator(
            api_func=self.pro.fut_basic,
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
        # date ????????????
        if start_date or end_date:
            raise ValueError("TushareFutBasicSource does not support date range")

        filters = {
            "ts_code": ts_code or "",
            "exchange": exchange or "",
            "list_date": date or "",
        }

        return self.paginator.fetch(filters=filters, fields=self.FIELDS)


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
        "oi_chg"
    ]

    def __init__(self, *, pro=None, token=None):
        BaseTushareSource.__init__(
            self,
            token=TushareInfo.token,
        )
        self.paginator = TusharePaginator(
            api_func=self.pro.fut_daily,
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
        # date ????????????
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

if __name__ == "__main__":
    from autotrade.coreutils.config import load_env

    load_env("d:/.env")

    basic = TushareEtfFundAdj()
    res = basic.fetch(ts_code='159238.SZ',exchange='a', start_date='20250104')


    import tushare as ts

    # token秘钥（把给咱们的token复制过来哈）
    token = "f5d21f83664a2e928757d8ae18a8c0a1e58f28e72b0560b196bed1c91672"
    pro = ts.pro_api(token)
    pro._DataApi__token = token  # 保证有这个代码，不然不可以获取
    pro._DataApi__http_url = 'https://jiaoch.site'  # 保证有这个代码，不然不可以获取
    # 测试接口(换成自己的接口）
    res = pro.daily(ts_code='000001.SZ', start_date='20180701', end_date='20180718')
    print(res)
