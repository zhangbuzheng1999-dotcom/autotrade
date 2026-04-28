# autotrade/data/ricequant/datasource/common.py

from __future__ import annotations

import pandas as pd
from rqdatac import get_trading_dates
from rqdatac import init as rq_init

from autotrade.data.ricequant.base import BaseRQDataSource
from autotrade.data.ricequant.spec.common import TradingDatesSpec


class TradingDatesDataSource(BaseRQDataSource):
    _initialized = False

    def __init__(self, spec: TradingDatesSpec | None = None):
        if not self.__class__._initialized:
            rq_init()
            self.__class__._initialized = True

        super().__init__(spec or TradingDatesSpec())

    def _call_api(self, **api_filters) -> pd.DataFrame:
        dates = get_trading_dates(
            start_date=api_filters["start_date"],
            end_date=api_filters["end_date"],
            market=api_filters.get("market", "cn"),
        )
        return pd.DataFrame({"trading_date": list(dates)})
