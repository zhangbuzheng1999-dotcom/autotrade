# autotrade/data/ricequant/datasource/futures.py

from __future__ import annotations

import pandas as pd
from rqdatac import all_instruments
from rqdatac import init as rq_init

from autotrade.data.ricequant.base import BaseRQDataSource
from autotrade.data.ricequant.spec.futures import FutureInstrumentSpec


class FutureInstrumentDataSource(BaseRQDataSource):
    """
    all_instruments(type='Future') datasource

    API层固定为 Future，只接受：
        - date
        - market
    """

    _initialized = False

    def __init__(self, spec: FutureInstrumentSpec | None = None):
        if not self.__class__._initialized:
            rq_init()
            self.__class__._initialized = True

        super().__init__(spec or FutureInstrumentSpec())

    def _call_api(self, **api_filters) -> pd.DataFrame:
        return all_instruments(
            type="Future",
            date=api_filters.get("date"),
            market=api_filters.get("market", "cn"),
        )
