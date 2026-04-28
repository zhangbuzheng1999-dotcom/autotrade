from __future__ import annotations

from autotrade.data.ricequant.base import BaseClickHouseRepository, BaseRQRepository
from autotrade.data.ricequant.spec.etf import ETFInstrumentSpec, ETFPriceSpec


class ETFPriceRepository(BaseClickHouseRepository):
    def __init__(self, spec: ETFPriceSpec | None = None):
        super().__init__(spec or ETFPriceSpec())


class ETFInstrumentRepository(BaseRQRepository):
    def __init__(self, spec: ETFInstrumentSpec | None = None):
        super().__init__(spec or ETFInstrumentSpec())
