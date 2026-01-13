# autotrade/service/base.py
from enum import Enum
import pandas as pd
from abc import ABC
from autotrade.coreutils.object import FetchResult
from autotrade.coreutils.constant import FetchStatus, FetchMode
import logging

logger = logging.getLogger("autotrade.service")


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
    ) -> FetchResult:

        params = dict(
            ts_code=ts_code,
            exchange=exchange,
            date=date,
            start_date=start_date,
            end_date=end_date,
        )

        try:
            if mode == FetchMode.DB_ONLY:
                df = self._query_db(**params)
                return FetchResult(FetchStatus.SUCCESS, data=df)

            if mode == FetchMode.SOURCE_ONLY:
                df = self._fetch_source(**params)
                if persist:
                    self._persist(df)
                return FetchResult(FetchStatus.SUCCESS, data=df)

            # DB_THEN_SOURCE
            df_db = self._query_db(**params)
            if not df_db.empty:
                return FetchResult(FetchStatus.SUCCESS, data=df_db)

            df_src = self._fetch_source(**params)
            if persist:
                self._persist(df_src)
            return FetchResult(FetchStatus.SUCCESS, data=df_src)

        except Exception as e:
            logger.error("service get failed", exc_info=e)
            return FetchResult(FetchStatus.FAILED, error=e)

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
