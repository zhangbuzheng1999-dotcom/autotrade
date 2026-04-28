from __future__ import annotations

from autotrade.data.ricequant.base import BaseRQService
from autotrade.data.ricequant.datasource.etf import ETFInstrumentDataSource, ETFPriceDataSource
from autotrade.data.ricequant.repository.etf import ETFInstrumentRepository, ETFPriceRepository
from autotrade.data.ricequant.spec.etf import ETFInstrumentSpec, ETFPriceSpec


class ETFPriceService(BaseRQService):
    def __init__(
        self,
        *,
        spec: ETFPriceSpec | None = None,
        repo: ETFPriceRepository | None = None,
        source: ETFPriceDataSource | None = None,
    ):
        spec = spec or ETFPriceSpec()
        repo = repo or ETFPriceRepository(spec)
        source = source or ETFPriceDataSource(spec)

        super().__init__(spec=spec, repo=repo, source=source)


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
