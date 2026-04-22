# autotrade/data/ricequant/service/common.py

from __future__ import annotations

from autotrade.data.ricequant.base import BaseRQService, FetchMode
from autotrade.data.ricequant.spec.common import PriceSpec
from autotrade.data.ricequant.datasource.common import PriceDataSource
from autotrade.data.ricequant.repository.common import PriceRepository
from autotrade.data.ricequant.datasource.common import TradingDatesDataSource
from autotrade.data.ricequant.repository.common import TradingDatesRepository
from autotrade.data.ricequant.spec.common import TradingDatesSpec

class PriceService(BaseRQService):
    """
    Cross-asset common service for rq get_price resource.

    统一对外入口：
        - DB_ONLY
        - SOURCE_ONLY
        - DB_THEN_SOURCE

    当前 price 是 common 下的第一个跨资产资源。
    后续如果 common 里增加别的资源，可以继续在本文件追加：
        - InstrumentListService
        - InstrumentDetailService
        - ...
    """

    def __init__(
        self,
        *,
        spec: PriceSpec | None = None,
        repo: PriceRepository | None = None,
        source: PriceDataSource | None = None,
    ):
        spec = spec or PriceSpec()
        repo = repo or PriceRepository(spec)
        source = source or PriceDataSource(spec)

        super().__init__(
            spec=spec,
            repo=repo,
            source=source,
        )



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
