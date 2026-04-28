# autotrade/data/ricequant/repository/index.py

from __future__ import annotations

from autotrade.data.ricequant.base import BaseClickHouseRepository, BaseRQRepository
from autotrade.data.ricequant.spec.index import IndexInstrumentSpec, IndexPriceSpec


class IndexPriceRepository(BaseClickHouseRepository):
    def __init__(self, spec: IndexPriceSpec | None = None):
        super().__init__(spec or IndexPriceSpec())


class IndexInstrumentRepository(BaseRQRepository):
    """
    指数合约基础信息 repository
    """

    def __init__(self, spec: IndexInstrumentSpec | None = None):
        super().__init__(spec or IndexInstrumentSpec())
