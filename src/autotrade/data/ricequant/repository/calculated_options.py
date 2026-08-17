from autotrade.data.ricequant.base import BaseClickHouseRepository
from autotrade.data.ricequant.spec.calculated_options import (
    CalculatedOptionGreeksSpec,
    CalculatedOptionIVXSpec,
)


class CalculatedOptionGreeksRepository(BaseClickHouseRepository):
    def __init__(self, spec: CalculatedOptionGreeksSpec | None = None):
        super().__init__(spec or CalculatedOptionGreeksSpec())

    def _align_columns(self, df):
        result = df.copy()
        frequency = None
        if "frequency" in result and not result.empty:
            frequency = result["frequency"].iloc[0]
        columns = (
            self.spec.MINUTE_COLUMNS
            if frequency == "1m"
            else self.spec.DAILY_COLUMNS
        )
        for column in columns:
            if column not in result:
                result[column] = None
        return result[columns]


class CalculatedOptionIVXRepository(BaseClickHouseRepository):
    def __init__(self, spec: CalculatedOptionIVXSpec | None = None):
        super().__init__(spec or CalculatedOptionIVXSpec())
