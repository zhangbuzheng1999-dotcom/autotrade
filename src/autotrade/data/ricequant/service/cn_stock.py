from __future__ import annotations

from autotrade.data.ricequant.base import BaseRQService, FetchMode, FetchResult
from autotrade.data.ricequant.datasource.cn_stock import CNStockInstrumentDataSource
from autotrade.data.ricequant.repository.cn_stock import CNStockInstrumentRepository
from autotrade.data.ricequant.service.common import PriceService
from autotrade.data.ricequant.spec.cn_stock import CNStockInstrumentSpec


class CNStockPriceService(PriceService):
    """
    A股价格接口：固定 type='CS'
    """

    FIXED_TYPE = "CS"

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
