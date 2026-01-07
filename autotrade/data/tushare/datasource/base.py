# autotrade/data/tushare/datasource/base.py
from abc import ABC, abstractmethod
import pandas as pd

class BaseDataSource(ABC):
    """
    Base data source with unified query semantics.

    Supported query dimensions:
    - ts_code
    - exchange
    - date
    - start_date
    - end_date
    """

    def fetch(
        self,
        *,
        ts_code: str | None = None,
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
            ts_code=ts_code,
            exchange=exchange,
            date=date,
            start_date=start_date,
            end_date=end_date,
        )

    @abstractmethod
    def _fetch_impl(
        self,
        *,
        ts_code: str | None,
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
