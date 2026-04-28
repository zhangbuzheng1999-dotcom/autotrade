# autotrade/data/ricequant/service/futures.py
from __future__ import annotations
from autotrade.data.ricequant.base import BaseRQService
from autotrade.data.ricequant.spec.futures import FutureInstrumentSpec, FuturePriceSpec
from autotrade.data.ricequant.repository.futures import FutureInstrumentRepository, FuturePriceRepository
from autotrade.data.ricequant.datasource.futures import FutureInstrumentDataSource, FuturePriceDataSource


class FuturePriceService(BaseRQService):
    def __init__(
        self,
        *,
        spec: FuturePriceSpec | None = None,
        repo: FuturePriceRepository | None = None,
        source: FuturePriceDataSource | None = None,
    ):
        spec = spec or FuturePriceSpec()
        repo = repo or FuturePriceRepository(spec)
        source = source or FuturePriceDataSource(spec)

        super().__init__(spec=spec, repo=repo, source=source)


class FutureInstrumentService(BaseRQService):
    """
    期货合约基础信息独立 service
    """

    def __init__(
        self,
        *,
        spec: FutureInstrumentSpec | None = None,
        repo: FutureInstrumentRepository | None = None,
        source: FutureInstrumentDataSource | None = None,
    ):
        spec = spec or FutureInstrumentSpec()
        repo = repo or FutureInstrumentRepository(spec)
        source = source or FutureInstrumentDataSource(spec)

        super().__init__(
            spec=spec,
            repo=repo,
            source=source,
        )
