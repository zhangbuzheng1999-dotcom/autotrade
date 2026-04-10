from __future__ import annotations

import pandas as pd
from rqdatac import all_instruments
from rqdatac import init as rq_init

from autotrade.data.ricequant.base import BaseRQDataSource
from autotrade.data.ricequant.spec.cn_stock import CNStockInstrumentSpec


class CNStockInstrumentDataSource(BaseRQDataSource):
    """
    all_instruments(type='CS') datasource
    """

    _initialized = False

    def __init__(self, spec: CNStockInstrumentSpec | None = None):
        if not self.__class__._initialized:
            rq_init()
            self.__class__._initialized = True

        super().__init__(spec or CNStockInstrumentSpec())

    def _call_api(self, **api_filters) -> pd.DataFrame:
        return all_instruments(
            type="CS",
            date=api_filters.get("date"),
            market=api_filters.get("market", "cn"),
        )
