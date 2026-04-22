# autotrade/data/ricequant/repository/common.py

from __future__ import annotations
from autotrade.data.ricequant.base import (
    BackendRoutingRepository,
    BaseClickHouseRepository,
    BaseRQRepository,
)
from autotrade.data.ricequant.spec.common import PriceSpec,TradingDatesSpec

class MySQLPriceRepository(BaseRQRepository):
    def __init__(self, spec: PriceSpec | None = None):
        super().__init__(spec or PriceSpec())


class ClickHousePriceRepository(BaseClickHouseRepository):
    def __init__(self, spec: PriceSpec | None = None):
        super().__init__(spec or PriceSpec())


class PriceRepository(BackendRoutingRepository):
    """
    Cross-asset common repository for rq get_price resource.

    当前先只封装 PriceSpec。
    后续如果 common 里增加别的跨资产资源，
    可以继续在这里增加类似：
        - InstrumentListRepository
        - InstrumentDetailRepository
        - ...
    """

    def __init__(self, spec: PriceSpec | None = None):
        spec = spec or PriceSpec()
        super().__init__(
            spec,
            mysql_repo_cls=MySQLPriceRepository,
            clickhouse_repo_cls=ClickHousePriceRepository,
        )



class TradingDatesRepository(BaseRQRepository):
    def __init__(self, spec: TradingDatesSpec | None = None):
        super().__init__(spec or TradingDatesSpec())
