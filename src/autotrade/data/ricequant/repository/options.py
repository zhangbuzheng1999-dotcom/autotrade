# autotrade/data/ricequant/repository/options.py

from __future__ import annotations

from autotrade.data.ricequant.base import (
    BackendRoutingRepository,
    BaseClickHouseRepository,
    BaseRQRepository,
)
from autotrade.data.ricequant.spec.options import *


class OptionInstrumentRepository(BaseRQRepository):
    """
    期权合约基础信息 repository
    """

    def __init__(self, spec: OptionInstrumentSpec | None = None):
        super().__init__(spec or OptionInstrumentSpec())

class MySQLOptionGreeksRepository(BaseRQRepository):
    def __init__(self, spec: OptionGreeksSpec | None = None):
        super().__init__(spec or OptionGreeksSpec())


class ClickHouseOptionGreeksRepository(BaseClickHouseRepository):
    def __init__(self, spec: OptionGreeksSpec | None = None):
        super().__init__(spec or OptionGreeksSpec())


class OptionGreeksRepository(BackendRoutingRepository):
    """
    期权 greek repository
    """

    def __init__(self, spec: OptionGreeksSpec | None = None):
        spec = spec or OptionGreeksSpec()
        super().__init__(
            spec,
            mysql_repo_cls=MySQLOptionGreeksRepository,
            clickhouse_repo_cls=ClickHouseOptionGreeksRepository,
        )
