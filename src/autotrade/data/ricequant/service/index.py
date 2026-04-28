# autotrade/data/ricequant/service/index.py

from __future__ import annotations

from autotrade.data.ricequant.base import BaseRQService
from autotrade.data.ricequant.datasource.index import IndexInstrumentDataSource, IndexPriceDataSource
from autotrade.data.ricequant.repository.index import IndexInstrumentRepository, IndexPriceRepository
from autotrade.data.ricequant.spec.index import IndexInstrumentSpec, IndexPriceSpec


class IndexPriceService(BaseRQService):
    def __init__(
        self,
        *,
        spec: IndexPriceSpec | None = None,
        repo: IndexPriceRepository | None = None,
        source: IndexPriceDataSource | None = None,
    ):
        spec = spec or IndexPriceSpec()
        repo = repo or IndexPriceRepository(spec)
        source = source or IndexPriceDataSource(spec)

        super().__init__(spec=spec, repo=repo, source=source)


class IndexInstrumentService(BaseRQService):
    """
    指数合约基础信息独立 service
    """

    def __init__(
        self,
        *,
        spec: IndexInstrumentSpec | None = None,
        repo: IndexInstrumentRepository | None = None,
        source: IndexInstrumentDataSource | None = None,
    ):
        spec = spec or IndexInstrumentSpec()
        repo = repo or IndexInstrumentRepository(spec)
        source = source or IndexInstrumentDataSource(spec)

        super().__init__(
            spec=spec,
            repo=repo,
            source=source,
        )



