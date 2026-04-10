# autotrade/data/ricequant/repository/futures.py

from __future__ import annotations

from autotrade.data.ricequant.base import BaseRQRepository
from autotrade.data.ricequant.spec.futures import FutureInstrumentSpec


class FutureInstrumentRepository(BaseRQRepository):
    """
    期货合约基础信息 repository
    """

    def __init__(self, spec: FutureInstrumentSpec | None = None):
        super().__init__(spec or FutureInstrumentSpec())
