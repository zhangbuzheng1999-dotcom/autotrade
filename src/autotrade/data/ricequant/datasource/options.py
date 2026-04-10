# autotrade/data/ricequant/datasource/options.py

from __future__ import annotations

import pandas as pd
from rqdatac import all_instruments
from rqdatac import init as rq_init
from rqdatac import options as rq_options

from autotrade.data.ricequant.base import BaseRQDataSource
from autotrade.data.ricequant.spec.options import *


class OptionInstrumentDataSource(BaseRQDataSource):
    """
    all_instruments(type='Option') datasource

    API层固定为 Option，只接受：
        - date
        - market
    """

    _initialized = False

    def __init__(self, spec: OptionInstrumentSpec | None = None):
        if not self.__class__._initialized:
            rq_init()
            self.__class__._initialized = True

        super().__init__(spec or OptionInstrumentSpec())

    def _call_api(self, **api_filters) -> pd.DataFrame:
        return all_instruments(
            type="Option",
            date=api_filters.get("date"),
            market=api_filters.get("market", "cn"),
        )

class OptionGreeksDataSource(BaseRQDataSource):
    """
    options.get_greeks datasource
    """

    _initialized = False

    def __init__(self, spec: OptionGreeksSpec | None = None):
        if not self.__class__._initialized:
            rq_init()
            self.__class__._initialized = True

        super().__init__(spec or OptionGreeksSpec())

    def _call_api(self, **api_filters) -> pd.DataFrame:
        return rq_options.get_greeks(
            order_book_ids=api_filters["order_book_ids"],
            start_date=api_filters["start_date"],
            end_date=api_filters.get("end_date"),
            fields=api_filters.get("fields"),
            model=api_filters.get("model", "implied_forward"),
            price_type=api_filters.get("price_type", "close"),
            frequency=api_filters.get("frequency", "1d"),
            market=api_filters.get("market", "cn"),
        )
