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
            code=None,
            code_list=None,
            exchange=None,
            date=None,
            start_date=None,
            end_date=None,
    ) -> FetchResult:

        params = dict(
            code=code,
            code_list=code_list,
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
            # db 为空直接查询
            if df_db.empty:
                df_src = self._fetch_source(**params)
                if persist:
                    self._persist(df_src)
                return FetchResult(FetchStatus.SUCCESS, data=df_src)

            else:
                # 如果不是用code_list查询又非空，直接返回
                if code_list is None:
                    return FetchResult(FetchStatus.SUCCESS, data=df_db)
                else:
                    # 如果是用code_list查询,结果非空,检查code是否缺失
                    # code字段
                    code_field = self.repo.CODE_FIELD
                    # db获取的code_list
                    db_code_list = set(df_db[code_field])
                    query_code_list = set(code_list)
                    missing_code_list = list(query_code_list-db_code_list)
                    # code 有缺失,查询
                    if len(missing_code_list) > 0:
                        src_params = dict(
                            code_list=missing_code_list,
                            exchange=exchange,
                            date=date,
                            start_date=start_date,
                            end_date=end_date,
                        )
                        df_src = self._fetch_source(**src_params)
                        if persist:
                            self._persist(df_src)
                        df_db = pd.concat([df_db, df_src])

                    return FetchResult(FetchStatus.SUCCESS, data=df_db)

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

