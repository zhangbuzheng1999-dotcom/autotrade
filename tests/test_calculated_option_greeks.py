from __future__ import annotations

import pandas as pd

from autotrade.data.ricequant.base import FetchMode, FetchResult, FetchStatus
from autotrade.data.ricequant.datasource.calculated_options import (
    CalculatedOptionGreeksDataSource,
)
from autotrade.data.ricequant.service.calculated_options import (
    CalculatedOptionGreeksService,
)
from autotrade.data.ricequant.spec.calculated_options import (
    CalculatedOptionGreeksSpec,
)


class RecordingService:
    def __init__(self, data):
        self.data = data
        self.calls = []

    def get(self, **kwargs):
        self.calls.append(kwargs)
        return FetchResult(FetchStatus.SUCCESS, data=self.data.copy())


def test_source_only_propagates_to_all_input_services():
    instruments = pd.DataFrame(
        {
            "order_book_id": ["OPT-C", "OPT-P"],
            "underlying_order_book_id": ["510050.XSHG", "510050.XSHG"],
            "underlying_symbol": ["510050.XSHG", "510050.XSHG"],
            "listed_date": ["2025-01-01", "2025-01-01"],
            "maturity_date": ["2025-02-01", "2025-02-01"],
            "strike_price": [100.0, 100.0],
            "option_type": ["C", "P"],
        }
    )
    prices = pd.DataFrame(
        {
            "order_book_id": ["OPT-C", "OPT-P"],
            "date": ["2025-01-02", "2025-01-02"],
            "close": [5.0, 4.0],
        }
    )
    instrument_service = RecordingService(instruments)
    price_service = RecordingService(prices)
    future_service = RecordingService(pd.DataFrame())
    source = CalculatedOptionGreeksDataSource(
        option_instrument_service=instrument_service,
        option_price_service=price_service,
        future_price_service=future_service,
    )

    result = source.fetch(
        opt_symbol="510050",
        start_date="2025-01-02",
        end_date="2025-01-02",
    )

    assert len(result) == 2
    assert result["forward_method"].eq("put_call_parity").all()
    assert result["forward_price"].notna().all()
    assert instrument_service.calls[0]["mode"] is FetchMode.SOURCE_ONLY
    assert price_service.calls[0]["mode"] is FetchMode.SOURCE_ONLY
    assert future_service.calls == []


class CapturingRepository:
    def __init__(self):
        self.persisted = None

    def insert_dataframe(self, frame, **filters):
        self.persisted = frame.copy()

    def query(self, **filters):
        return pd.DataFrame()


class FullCrossSectionSource:
    def fetch(self, **filters):
        return pd.DataFrame(
            {
                column: [None, None]
                for column in CalculatedOptionGreeksSpec.COLUMNS
            }
        ).assign(
            order_book_id=["OPT-C", "OPT-P"],
            date=[pd.Timestamp("2025-01-02").date()] * 2,
        )


def test_source_only_persists_full_scope_before_result_filtering():
    repository = CapturingRepository()
    service = CalculatedOptionGreeksService(
        repo=repository,
        source=FullCrossSectionSource(),
    )

    result = service.get(
        mode=FetchMode.SOURCE_ONLY,
        persist=True,
        order_book_ids=["OPT-C"],
        start_date="2025-01-02",
        end_date="2025-01-02",
    )

    assert result.status is FetchStatus.SUCCESS
    assert repository.persisted["order_book_id"].tolist() == ["OPT-C", "OPT-P"]
    assert result.data["order_book_id"].tolist() == ["OPT-C"]
