# autotrade/data/ricequant/service/options.py

from __future__ import annotations

from autotrade.data.ricequant.base import BaseRQService, FetchMode, FetchResult, FetchStatus
from autotrade.data.ricequant.datasource.options import *
from autotrade.data.ricequant.repository.options import *
from autotrade.data.ricequant.spec.options import *


class OptionPriceService(BaseRQService):
    def __init__(
        self,
        *,
        spec: OptionPriceSpec | None = None,
        repo: OptionPriceRepository | None = None,
        source: OptionPriceDataSource | None = None,
    ):
        spec = spec or OptionPriceSpec()
        repo = repo or OptionPriceRepository(spec)
        source = source or OptionPriceDataSource(spec)

        super().__init__(spec=spec, repo=repo, source=source)


class OptionInstrumentService(BaseRQService):
    """
    期权合约基础信息独立 service
    """

    def __init__(
        self,
        *,
        spec: OptionInstrumentSpec | None = None,
        repo: OptionInstrumentRepository | None = None,
        source: OptionInstrumentDataSource | None = None,
    ):
        spec = spec or OptionInstrumentSpec()
        repo = repo or OptionInstrumentRepository(spec)
        source = source or OptionInstrumentDataSource(spec)

        super().__init__(
            spec=spec,
            repo=repo,
            source=source,
        )


class OptionGreeksService(BaseRQService):
    """
    options.get_greeks service

    说明：
    - SOURCE_ONLY / DB_THEN_SOURCE：严格遵守 API 语义
    - DB_ONLY：允许不传 order_book_ids
    - fields 在 DB_ONLY 下用于结果裁剪
    """

    def __init__(
        self,
        *,
        spec: OptionGreeksSpec | None = None,
        repo: OptionGreeksRepository | None = None,
        source: OptionGreeksDataSource | None = None,
    ):
        spec = spec or OptionGreeksSpec()
        repo = repo or OptionGreeksRepository(spec)
        source = source or OptionGreeksDataSource(spec)

        super().__init__(
            spec=spec,
            repo=repo,
            source=source,
        )

    def get(
        self,
        *,
        mode: FetchMode = FetchMode.DB_THEN_SOURCE,
        persist: bool = True,
        refresh: bool = False,
        **filters,
    ) -> FetchResult:
        result = super().get(
            mode=mode,
            persist=persist,
            refresh=refresh,
            **filters,
        )

        if result.status != FetchStatus.SUCCESS or result.data is None or result.data.empty:
            return result

        fields = filters.get("fields")
        if fields is None:
            return result

        if isinstance(fields, str):
            fields = [fields]

        df = result.data.copy()

        id_cols = [c for c in ["order_book_id", "date", "datetime", "model", "price_type", "frequency", "market"] if c in df.columns]
        value_cols = [c for c in fields if c in df.columns]

        result.data = df[id_cols + value_cols]
        return result
