# autotrade/data/ricequant/service/futures.py
from __future__ import annotations
from autotrade.data.ricequant.base import FetchMode, FetchResult
from autotrade.data.ricequant.service.common import PriceService
from autotrade.data.ricequant.spec.futures import FutureInstrumentSpec
from autotrade.data.ricequant.repository.futures import FutureInstrumentRepository
from autotrade.data.ricequant.datasource.futures import FutureInstrumentDataSource
from autotrade.data.ricequant.base import BaseRQService


class FuturePriceService(PriceService):
    """
    Futures price facade.

    复用 common.PriceService，
    对外固定 type='Future'，避免业务层重复传 type。
    """

    FIXED_TYPE = "Future"

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

