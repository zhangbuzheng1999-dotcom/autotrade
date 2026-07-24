"""Readers for standard market-price data."""

from __future__ import annotations

from typing import Any

from autotrade.coreutils.object import CustomData, Tick, TradeBar
from autotrade.backtest.data.reader.base import (
    DataReader,
    finite_float,
    float_or_none,
    float_or_zero,
    row_getter,
    to_datetime,
)


class TradeBarReader(DataReader):
    def read(
        self,
        source: Any,
        *,
        interval=None,
        exchange=None,
        metadata: dict[str, Any] | None = None,
    ):
        frame = self.frame(source)
        mapping = self.schema_resolver.resolve(
            frame,
            required=("symbol", "time", "open", "high", "low", "close"),
            optional=("volume", "turnover", "open_interest"),
            schema=self.schema,
        )
        get = row_getter(frame)

        def records():
            for row in frame.itertuples(index=False):
                yield TradeBar(
                    symbol=str(get(row, mapping["symbol"])),
                    exchange=exchange,
                    time=to_datetime(get(row, mapping["time"])),
                    interval=interval,
                    open=finite_float(get(row, mapping["open"]), "open"),
                    high=finite_float(get(row, mapping["high"]), "high"),
                    low=finite_float(get(row, mapping["low"]), "low"),
                    close=finite_float(get(row, mapping["close"]), "close"),
                    volume=float_or_zero(get(row, mapping.get("volume"))),
                    turnover=float_or_zero(get(row, mapping.get("turnover"))),
                    open_interest=float_or_zero(
                        get(row, mapping.get("open_interest"))
                    ),
                    metadata=dict(metadata or {}),
                )

        return records()


class TickReader(DataReader):
    def read(
        self,
        source: Any,
        *,
        exchange=None,
        tick_type: str = "trade",
        metadata: dict[str, Any] | None = None,
    ):
        frame = self.frame(source)
        mapping = self.schema_resolver.resolve(
            frame,
            required=("symbol", "time"),
            optional=("price", "quantity", "bid", "ask", "bid_size", "ask_size"),
            schema=self.schema,
        )
        get = row_getter(frame)

        def records():
            for row in frame.itertuples(index=False):
                yield Tick(
                    symbol=str(get(row, mapping["symbol"])),
                    exchange=exchange,
                    time=to_datetime(get(row, mapping["time"])),
                    tick_type=tick_type,
                    price=float_or_none(get(row, mapping.get("price"))),
                    quantity=float_or_none(get(row, mapping.get("quantity"))),
                    bid=float_or_none(get(row, mapping.get("bid"))),
                    ask=float_or_none(get(row, mapping.get("ask"))),
                    bid_size=float_or_none(get(row, mapping.get("bid_size"))),
                    ask_size=float_or_none(get(row, mapping.get("ask_size"))),
                    metadata=dict(metadata or {}),
                )

        return records()


class CustomDataReader(DataReader):
    def read(
        self,
        source: Any,
        *,
        exchange=None,
        custom_type: str = "custom",
        metadata: dict[str, Any] | None = None,
    ):
        frame = self.frame(source)
        mapping = self.schema_resolver.resolve(
            frame,
            required=("symbol", "time"),
            optional=("value",),
            schema=self.schema,
        )
        get = row_getter(frame)
        standard_columns = {
            mapping["symbol"],
            mapping["time"],
            mapping.get("value"),
        }

        def records():
            for row in frame.itertuples(index=False):
                payload = {
                    column: get(row, column)
                    for column in frame.columns
                    if column not in standard_columns
                }
                yield CustomData(
                    symbol=str(get(row, mapping["symbol"])),
                    exchange=exchange,
                    time=to_datetime(get(row, mapping["time"])),
                    value=float_or_none(get(row, mapping.get("value"))),
                    custom_type=custom_type,
                    payload=payload,
                    metadata=dict(metadata or {}),
                )

        return records()


__all__ = ["CustomDataReader", "TickReader", "TradeBarReader"]
