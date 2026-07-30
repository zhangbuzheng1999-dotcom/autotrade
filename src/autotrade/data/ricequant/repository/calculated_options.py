from autotrade.data.ricequant.base import BaseClickHouseRepository
from autotrade.data.ricequant.spec.calculated_options import (
    CalculatedOptionGreeksSpec,
    CalculatedOptionIVXSpec,
)


class CalculatedOptionGreeksRepository(BaseClickHouseRepository):
    def __init__(self, spec: CalculatedOptionGreeksSpec | None = None):
        super().__init__(spec or CalculatedOptionGreeksSpec())


class CalculatedOptionIVXRepository(BaseClickHouseRepository):
    def __init__(self, spec: CalculatedOptionIVXSpec | None = None):
        super().__init__(spec or CalculatedOptionIVXSpec())
