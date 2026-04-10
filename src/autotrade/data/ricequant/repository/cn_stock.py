from __future__ import annotations

from autotrade.data.ricequant.base import BaseRQRepository
from autotrade.data.ricequant.spec.cn_stock import CNStockInstrumentSpec


class CNStockInstrumentRepository(BaseRQRepository):
    def __init__(self, spec: CNStockInstrumentSpec | None = None):
        super().__init__(spec or CNStockInstrumentSpec())
