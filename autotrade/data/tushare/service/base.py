# autotrade/service/base.py
from enum import Enum
import pandas as pd
from abc import ABC


class FetchMode(str, Enum):
    DB_ONLY = "db_only"  # 只查数据库
    SOURCE_ONLY = "source_only"  # 只拉数据源
    DB_THEN_SOURCE = "db_then_source"  # 先查库，不足再拉


class BaseService(ABC):
    source = None
    repo = None

    def get(
            self,
            *,
            mode: FetchMode = FetchMode.DB_THEN_SOURCE,
            persist: bool = True,
            ts_code=None,
            exchange=None,
            date=None,
            start_date=None,
            end_date=None,
    ) -> pd.DataFrame:

        params = dict(
            ts_code=ts_code,
            exchange=exchange,
            date=date,
            start_date=start_date,
            end_date=end_date,
        )

        if mode == FetchMode.DB_ONLY:
            return self._query_db(**params)

        if mode == FetchMode.SOURCE_ONLY:
            df = self._fetch_source(**params)
            if persist:
                self._persist(df)
            return df

        # DB_THEN_SOURCE
        df_db = self._query_db(**params)
        if not df_db.empty:
            return df_db

        df_src = self._fetch_source(**params)
        if persist:
            self._persist(df_src)
        return df_src

    # ===============================
    # 默认行为（可被覆盖）
    # ===============================

    def _query_db(self, **kwargs):
        if not self.repo:
            raise NotImplementedError("Repository not set")
        return self.repo.query(**kwargs)

    def _fetch_source(self, **kwargs):
        if not self.source:
            raise NotImplementedError("DataSource not set")
        return self.source.fetch(**kwargs)

    def _persist(self, df):
        if not self.repo:
            raise NotImplementedError("Repository not set")
        self.repo.insert_ignore(df)
