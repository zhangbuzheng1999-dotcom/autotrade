# autotrade/data/tushare/datasource/base.py
from abc import ABC, abstractmethod
import pandas as pd


class BaseDataSource(ABC):
    """
    Base data source with unified query semantics.

    Supported query dimensions:
    - code
    - exchange
    - date
    - start_date
    - end_date
    """

    def fetch(
            self,
            *,
            code: str | None = None,
            code_list: list[str] | None = None,
            exchange: str | None = None,
            date: str | None = None,
            start_date: str | None = None,
            end_date: str | None = None,
    ) -> pd.DataFrame:
        """
        Unified fetch entry.

        This method only defines query semantics.
        Actual mapping is implemented by subclasses.
        """
        return self._fetch_impl(
            code=code,
            code_list=code_list,
            exchange=exchange,
            date=date,
            start_date=start_date,
            end_date=end_date,
        )

    @abstractmethod
    def _fetch_impl(
            self,
            *,
            code: str | None,
            code_list: list[str] | None = None,
            exchange: str | None,
            date: str | None,
            start_date: str | None,
            end_date: str | None,
    ) -> pd.DataFrame:
        """
        Subclass should map unified query semantics
        to concrete API parameters.
        """
        raise NotImplementedError
