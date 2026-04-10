from __future__ import annotations

from autotrade.data.ricequant.base import BaseRQService, FetchMode, FetchResult
from autotrade.data.ricequant.datasource.etf import ETFInstrumentDataSource
from autotrade.data.ricequant.repository.etf import ETFInstrumentRepository
from autotrade.data.ricequant.service.common import PriceService
from autotrade.data.ricequant.spec.etf import ETFInstrumentSpec


class ETFPriceService(PriceService):
    """
    ETF价格接口：固定 type='ETF'
    """

    FIXED_TYPE = "ETF"

    def get(
        self,
        *,
        mode: FetchMode = FetchMode.DB_THEN_SOURCE,
        persist: bool = True,
        refresh: bool = False,
        **filters,
    ) -> FetchResult:
        filters = dict(filters)
        filters["type"] = self.FIXED_TYPE
        return super().get(
            mode=mode,
            persist=persist,
            refresh=refresh,
            **filters,
        )


class ETFInstrumentService(BaseRQService):
    def __init__(
        self,
        *,
        spec: ETFInstrumentSpec | None = None,
        repo: ETFInstrumentRepository | None = None,
        source: ETFInstrumentDataSource | None = None,
    ):
        spec = spec or ETFInstrumentSpec()
        repo = repo or ETFInstrumentRepository(spec)
        source = source or ETFInstrumentDataSource(spec)

        super().__init__(spec=spec, repo=repo, source=source)
