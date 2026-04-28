# autotrade/data/ricequant/repository/common.py

from __future__ import annotations
from autotrade.data.ricequant.base import BaseRQRepository
from autotrade.data.ricequant.spec.common import TradingDatesSpec


class TradingDatesRepository(BaseRQRepository):
    def __init__(self, spec: TradingDatesSpec | None = None):
        super().__init__(spec or TradingDatesSpec())
