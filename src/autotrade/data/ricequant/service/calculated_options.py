from __future__ import annotations

from autotrade.data.ricequant.base import (
    BaseRQService,
    FetchMode,
    FetchResult,
    FetchStatus,
)
from autotrade.data.ricequant.datasource.calculated_options import (
    CalculatedOptionGreeksDataSource,
    CalculatedOptionIVXDataSource,
)
from autotrade.data.ricequant.repository.calculated_options import (
    CalculatedOptionGreeksRepository,
    CalculatedOptionIVXRepository,
)
from autotrade.data.ricequant.spec.calculated_options import (
    CalculatedOptionGreeksSpec,
    CalculatedOptionIVXSpec,
)


class CalculatedOptionGreeksService(BaseRQService):
    """Query persisted custom Greeks or calculate a complete source cross-section."""

    def __init__(
        self,
        *,
        spec: CalculatedOptionGreeksSpec | None = None,
        repo: CalculatedOptionGreeksRepository | None = None,
        source: CalculatedOptionGreeksDataSource | None = None,
    ):
        spec = spec or CalculatedOptionGreeksSpec()
        super().__init__(
            spec=spec,
            repo=repo or CalculatedOptionGreeksRepository(spec),
            source=source or CalculatedOptionGreeksDataSource(spec),
        )

    def get(
        self,
        *,
        mode: FetchMode = FetchMode.DB_THEN_SOURCE,
        persist: bool = True,
        refresh: bool = False,
        **filters,
    ) -> FetchResult:
        requested_ids = filters.get("order_book_ids")
        if requested_ids is None and filters.get("order_book_id") is not None:
            requested_ids = [filters["order_book_id"]]
        if isinstance(requested_ids, str):
            requested_ids = [requested_ids]

        result = super().get(
            mode=mode,
            persist=persist,
            refresh=refresh,
            **filters,
        )
        # SOURCE_ONLY calculates/persists the full symbol cross-section and only
        # then applies the caller's result-scope selector.
        if (
            mode != FetchMode.DB_ONLY
            and requested_ids
            and result.status == FetchStatus.SUCCESS
            and result.data is not None
        ):
            result.data = result.data[
                result.data["order_book_id"].isin(requested_ids)
            ].reset_index(drop=True)
        return result


class CalculatedOptionIVXService(BaseRQService):
    """Query persisted IVX values or calculate them from a full source panel."""

    def __init__(
        self,
        *,
        spec: CalculatedOptionIVXSpec | None = None,
        repo: CalculatedOptionIVXRepository | None = None,
        source: CalculatedOptionIVXDataSource | None = None,
    ):
        spec = spec or CalculatedOptionIVXSpec()
        super().__init__(
            spec=spec,
            repo=repo or CalculatedOptionIVXRepository(spec),
            source=source or CalculatedOptionIVXDataSource(spec),
        )
