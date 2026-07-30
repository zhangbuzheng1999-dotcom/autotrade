from __future__ import annotations

import numpy as np
import pandas as pd

from autotrade.analytics.options import calculate_ivx
from autotrade.data.ricequant.base import FetchMode, FetchResult, FetchStatus
from autotrade.data.ricequant.datasource.calculated_options import (
    CalculatedOptionIVXDataSource,
)


class RecordingService:
    def __init__(self, data):
        self.data = data
        self.calls = []

    def get(self, **kwargs):
        self.calls.append(kwargs)
        return FetchResult(FetchStatus.SUCCESS, data=self.data.copy())


def _option_panel():
    rows = []
    for t_days, time_scale in ((20, 1.0), (50, 1.2)):
        for strike, call, put in (
            (90.0, 11.0 * time_scale, 1.0 * time_scale),
            (100.0, 5.0 * time_scale, 5.0 * time_scale),
            (110.0, 1.5 * time_scale, 11.5 * time_scale),
        ):
            for option_type, price in (("C", call), ("P", put)):
                rows.append(
                    {
                        "date": "2025-01-02",
                        "option_price": price,
                        "t_days": t_days,
                        "strike_price": strike,
                        "option_type": option_type,
                        "risk_free_rate": 0.03,
                    }
                )
    return pd.DataFrame(rows)


def test_calculate_ivx_returns_daily_value_and_diagnostics():
    result = calculate_ivx(_option_panel())

    assert len(result) == 1
    assert np.isfinite(result.loc[0, "ivx"])
    assert result.loc[0, "near_t_days"] == 20
    assert result.loc[0, "next_t_days"] == 50
    assert result.loc[0, "option_count"] == 12


def test_ivx_source_only_propagates_to_input_services():
    panel = _option_panel()
    instruments = []
    prices = []
    for index, row in panel.iterrows():
        order_book_id = f"OPT-{index}"
        maturity = pd.Timestamp(row["date"]) + pd.Timedelta(days=int(row["t_days"]))
        instruments.append(
            {
                "order_book_id": order_book_id,
                "underlying_symbol": "TEST",
                "listed_date": "2024-01-01",
                "maturity_date": maturity,
                "strike_price": row["strike_price"],
                "option_type": row["option_type"],
            }
        )
        prices.append(
            {
                "order_book_id": order_book_id,
                "date": row["date"],
                "close": row["option_price"],
            }
        )

    instrument_service = RecordingService(pd.DataFrame(instruments))
    price_service = RecordingService(pd.DataFrame(prices))
    source = CalculatedOptionIVXDataSource(
        option_instrument_service=instrument_service,
        option_price_service=price_service,
    )

    result = source.fetch(
        opt_symbol="TEST",
        start_date="2025-01-02",
        end_date="2025-01-02",
    )

    assert len(result) == 1
    assert np.isfinite(result.loc[0, "ivx"])
    assert result.loc[0, "opt_symbol"] == "TEST"
    assert instrument_service.calls[0]["mode"] is FetchMode.SOURCE_ONLY
    assert instrument_service.calls[0]["persist"] is False
    assert price_service.calls[0]["mode"] is FetchMode.SOURCE_ONLY
    assert price_service.calls[0]["persist"] is False
