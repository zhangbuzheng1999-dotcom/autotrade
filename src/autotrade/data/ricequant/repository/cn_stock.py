from __future__ import annotations

from autotrade.data.ricequant.base import BaseClickHouseRepository, BaseRQRepository
from autotrade.data.ricequant.spec.cn_stock import CNStockInstrumentSpec, CNStockPriceSpec


class CNStockPriceRepository(BaseClickHouseRepository):
    def __init__(self, spec: CNStockPriceSpec | None = None):
        super().__init__(spec or CNStockPriceSpec())

class CNStockInstrumentRepository(BaseRQRepository):
    def __init__(self, spec: CNStockInstrumentSpec | None = None):
        super().__init__(spec or CNStockInstrumentSpec())
