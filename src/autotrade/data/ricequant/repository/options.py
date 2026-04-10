# autotrade/data/ricequant/repository/options.py

from __future__ import annotations

from autotrade.data.ricequant.base import BaseRQRepository
from autotrade.data.ricequant.spec.options import *


class OptionInstrumentRepository(BaseRQRepository):
    """
    期权合约基础信息 repository
    """

    def __init__(self, spec: OptionInstrumentSpec | None = None):
        super().__init__(spec or OptionInstrumentSpec())

class OptionGreeksRepository(BaseRQRepository):
    """
    期权 greek repository
    """

    def __init__(self, spec: OptionGreeksSpec | None = None):
        super().__init__(spec or OptionGreeksSpec())
