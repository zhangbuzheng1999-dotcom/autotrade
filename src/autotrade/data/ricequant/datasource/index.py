# autotrade/data/ricequant/datasource/index.py

from __future__ import annotations

import pandas as pd
from rqdatac import all_instruments, get_price
from rqdatac import init as rq_init

from autotrade.data.ricequant.base import BaseRQDataSource
from autotrade.data.ricequant.spec.index import IndexInstrumentSpec, IndexPriceSpec


class IndexPriceDataSource(BaseRQDataSource):
    _initialized = False

    def __init__(self, spec: IndexPriceSpec | None = None):
        if not self.__class__._initialized:
            rq_init()
            self.__class__._initialized = True

        super().__init__(spec or IndexPriceSpec())

    def _call_api(self, **api_filters) -> pd.DataFrame:
        return get_price(
            order_book_ids=api_filters["order_book_ids"],
            start_date=api_filters.get("start_date"),
            end_date=api_filters.get("end_date"),
            frequency=api_filters.get("frequency", "1d"),
            fields=api_filters.get("fields"),
            adjust_type=api_filters.get("adjust_type", "none"),
            skip_suspended=api_filters.get("skip_suspended", False),
            expect_df=api_filters.get("expect_df", True),
            time_slice=api_filters.get("time_slice"),
            market=api_filters.get("market", "cn"),
        )


class IndexInstrumentDataSource(BaseRQDataSource):
    """
    all_instruments(type='INDX') datasource

    API层固定为 INDX，只接受：
        - date
        - market
    """

    _initialized = False

    def __init__(self, spec: IndexInstrumentSpec | None = None):
        if not self.__class__._initialized:
            rq_init()
            self.__class__._initialized = True

        super().__init__(spec or IndexInstrumentSpec())

    def _call_api(self, **api_filters) -> pd.DataFrame:
        return all_instruments(
            type="INDX",
            date=api_filters.get("date"),
            market=api_filters.get("market", "cn"),
        )
