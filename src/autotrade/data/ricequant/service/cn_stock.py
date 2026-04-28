from __future__ import annotations
from autotrade.data.ricequant.base import BaseRQService
from autotrade.data.ricequant.datasource.cn_stock import CNStockInstrumentDataSource, CNStockPriceDataSource
from autotrade.data.ricequant.repository.cn_stock import CNStockInstrumentRepository, CNStockPriceRepository
from autotrade.data.ricequant.spec.cn_stock import CNStockInstrumentSpec, CNStockPriceSpec


class CNStockPriceService(BaseRQService):
    def __init__(
        self,
        *,
        spec: CNStockPriceSpec | None = None,
        repo: CNStockPriceRepository | None = None,
        source: CNStockPriceDataSource | None = None,
    ):
        spec = spec or CNStockPriceSpec()
        repo = repo or CNStockPriceRepository(spec)
        source = source or CNStockPriceDataSource(spec)

        super().__init__(spec=spec, repo=repo, source=source)


class CNStockInstrumentService(BaseRQService):
    def __init__(
        self,
        *,
        spec: CNStockInstrumentSpec | None = None,
        repo: CNStockInstrumentRepository | None = None,
        source: CNStockInstrumentDataSource | None = None,
    ):
        spec = spec or CNStockInstrumentSpec()
        repo = repo or CNStockInstrumentRepository(spec)
        source = source or CNStockInstrumentDataSource(spec)

        super().__init__(spec=spec, repo=repo, source=source)
