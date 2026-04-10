from __future__ import annotations

from autotrade.data.ricequant.base import BaseRQRepository
from autotrade.data.ricequant.spec.etf import ETFInstrumentSpec


class ETFInstrumentRepository(BaseRQRepository):
    def __init__(self, spec: ETFInstrumentSpec | None = None):
        super().__init__(spec or ETFInstrumentSpec())
