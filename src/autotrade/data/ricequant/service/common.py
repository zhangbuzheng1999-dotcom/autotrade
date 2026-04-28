# autotrade/data/ricequant/service/common.py

from __future__ import annotations

from autotrade.data.ricequant.base import BaseRQService
from autotrade.data.ricequant.datasource.common import TradingDatesDataSource
from autotrade.data.ricequant.repository.common import TradingDatesRepository
from autotrade.data.ricequant.spec.common import TradingDatesSpec



class TradingDatesService(BaseRQService):
    """
    通用交易日服务：
        get_trading_dates(start_date, end_date, market='cn')
    """

    def __init__(
        self,
        *,
        spec: TradingDatesSpec | None = None,
        repo: TradingDatesRepository | None = None,
        source: TradingDatesDataSource | None = None,
    ):
        spec = spec or TradingDatesSpec()
        repo = repo or TradingDatesRepository(spec)
        source = source or TradingDatesDataSource(spec)

        super().__init__(
            spec=spec,
            repo=repo,
            source=source,
        )
