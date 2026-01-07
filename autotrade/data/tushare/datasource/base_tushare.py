# autotrade/datasource/base_tushare.py
import tushare as ts
from typing import Optional
import time
import pandas as pd
from typing import Callable
import requests
import urllib3

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
                    # 最终失败，记录即可，不抛异常
                    print(
                        f"[WARN] Tushare request failed after {self.max_retry} retries. "
                        f"params={params}. error={e}"
                    )
                    return None

        return None