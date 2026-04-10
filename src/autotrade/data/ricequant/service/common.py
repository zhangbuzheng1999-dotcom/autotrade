# autotrade/data/ricequant/service/common.py

from __future__ import annotations

from autotrade.data.ricequant.base import BaseRQService,FetchMode
from autotrade.data.ricequant.spec.common import PriceSpec
from autotrade.data.ricequant.datasource.common import PriceDataSource
from autotrade.data.ricequant.repository.common import PriceRepository


class PriceService(BaseRQService):
    """
    Cross-asset common service for rq get_price resource.

    统一对外入口：
        - DB_ONLY
        - SOURCE_ONLY
        - DB_THEN_SOURCE

    当前 price 是 common 下的第一个跨资产资源。
    后续如果 common 里增加别的资源，可以继续在本文件追加：
        - InstrumentListService
        - InstrumentDetailService
        - ...
    """

    def __init__(
        self,
        *,
        spec: PriceSpec | None = None,
        repo: PriceRepository | None = None,
        source: PriceDataSource | None = None,
    ):
        spec = spec or PriceSpec()
        repo = repo or PriceRepository(spec)
        source = source or PriceDataSource(spec)

        super().__init__(
            spec=spec,
            repo=repo,
            source=source,
        )


