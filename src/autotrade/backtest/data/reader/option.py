"""Reader for versioned option analytics strategy data."""

from __future__ import annotations

from typing import Any

from autotrade.backtest.data.reader.base import (
    DataReader,
    float_or_none,
    row_getter,
    string_or_none,
    to_datetime,
)
from autotrade.coreutils.object import OptionAnalyticsData


class OptionAnalyticsReader(DataReader):
    """Convert one row per option and time into ``OptionAnalyticsData``."""

    optional_fields = (
        "underlying_instrument_id",
        "underlying_price",
        "forward_price",
        "risk_free_rate",
        "time_to_expiry",
        "market_iv",
        "surface_iv",
        "delta",
        "gamma",
        "vega",
        "theta",
        "rho",
        "vanna",
        "vomma",
        "charm",
        "value",
    )

    def read(
        self,
        source: Any,
        *,
        model_id: str,
        model_version: str,
        exchange=None,
        metadata: dict[str, Any] | None = None,
    ):
        if not model_id.strip():
            raise ValueError("model_id cannot be empty")
        if not model_version.strip():
            raise ValueError("model_version cannot be empty")

        frame = self.frame(source)
        mapping = self.schema_resolver.resolve(
            frame,
            required=("instrument_id", "time"),
            optional=self.optional_fields,
            schema=self.schema,
        )
        get = row_getter(frame)

        def number(row, field_name):
            return float_or_none(get(row, mapping.get(field_name)))

        def records():
            for row in frame.itertuples(index=False):
                yield OptionAnalyticsData(
                    instrument_id=str(get(row, mapping["instrument_id"])),
                    time=to_datetime(get(row, mapping["time"])),
                    value=number(row, "value"),
                    exchange=exchange,
                    metadata=dict(metadata or {}),
                    underlying_instrument_id=string_or_none(
                        get(row, mapping.get("underlying_instrument_id"))
                    ),
                    underlying_price=number(row, "underlying_price"),
                    forward_price=number(row, "forward_price"),
                    risk_free_rate=number(row, "risk_free_rate"),
                    time_to_expiry=number(row, "time_to_expiry"),
                    market_iv=number(row, "market_iv"),
                    surface_iv=number(row, "surface_iv"),
                    delta=number(row, "delta"),
                    gamma=number(row, "gamma"),
                    vega=number(row, "vega"),
                    theta=number(row, "theta"),
                    rho=number(row, "rho"),
                    vanna=number(row, "vanna"),
                    vomma=number(row, "vomma"),
                    charm=number(row, "charm"),
                    model_id=model_id,
                    model_version=model_version,
                )

        return records()


__all__ = ["OptionAnalyticsReader"]
