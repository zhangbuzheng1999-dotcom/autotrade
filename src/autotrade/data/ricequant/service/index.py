# autotrade/data/ricequant/service/index.py

from __future__ import annotations

from autotrade.data.ricequant.base import BaseRQService, FetchMode, FetchResult
from autotrade.data.ricequant.datasource.index import IndexInstrumentDataSource
from autotrade.data.ricequant.repository.index import IndexInstrumentRepository
from autotrade.data.ricequant.service.common import PriceService
from autotrade.data.ricequant.spec.index import IndexInstrumentSpec

class IndexPriceService(PriceService):
    """
    指数价格接口：
    复用 common.PriceService，固定 type='INDX'
    """

    FIXED_TYPE = "INDX"

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




