# autotrade/data/ricequant/repository/common.py

from __future__ import annotations

from autotrade.data.ricequant.base import BaseRQRepository
from autotrade.data.ricequant.spec.common import PriceSpec


class PriceRepository(BaseRQRepository):
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
        super().__init__(spec or PriceSpec())
