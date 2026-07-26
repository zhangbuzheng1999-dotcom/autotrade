"""Internal backtest streaming, synchronization and routing primitives."""

from __future__ import annotations

import heapq
import math
from dataclasses import dataclass, field
from datetime import datetime
from itertools import count
from typing import Iterable, Iterator

from autotrade.coreutils.object import (
    MarketData,
    InstrumentStateData,
    OptionAnalyticsData,
    ValuationUpdate,
)


@dataclass(frozen=True, slots=True)
class _NamedData:
    data_name: str
    record: MarketData | InstrumentStateData


@dataclass(slots=True)
class _DataStream:
    data_name: str
    data: Iterable[MarketData | InstrumentStateData]

    def __iter__(self) -> Iterator[_NamedData]:
        previous_key = None
        for item in self.data:
            if item.time is None:
                raise ValueError(
                    f"bootstrap record {item.instrument_id!r} cannot enter DataStream"
                )
            key = (item.time, item.instrument_id)
            if previous_key is not None and key < previous_key:
                raise ValueError(
                    f"data stream '{self.data_name}' is not ordered by "
                    f"(time, instrument_id): {key!r} follows {previous_key!r}"
                )
            previous_key = key
            yield _NamedData(self.data_name, item)


@dataclass(frozen=True, slots=True)
class _SynchronizedData:
    time: datetime
    records: tuple[_NamedData, ...]


class _DataSynchronizer:
    def sync(
        self,
        streams: Iterable[_DataStream],
    ) -> Iterator[_SynchronizedData]:
        sequence = count()
        heap = []
        for stream in streams:
            iterator = iter(stream)
            try:
                item = next(iterator)
            except StopIteration:
                continue
            heapq.heappush(
                heap,
                (item.record.time, next(sequence), item, iterator),
            )

        while heap:
            when = heap[0][0]
            current: list[_NamedData] = []
            while heap and heap[0][0] == when:
                _, _, item, iterator = heapq.heappop(heap)
                current.append(item)
                try:
                    following = next(iterator)
                except StopIteration:
                    continue
                heapq.heappush(
                    heap,
                    (
                        following.record.time,
                        next(sequence),
                        following,
                        iterator,
                    ),
                )
            yield _SynchronizedData(time=when, records=tuple(current))


@dataclass(frozen=True, slots=True)
class DataRoutingConfig:
    """Explicitly assign named sources to strategy, state and valuation."""

    strategy_data_names: frozenset[str] = field(default_factory=frozenset)
    security_data_names: frozenset[str] = field(default_factory=frozenset)
    valuation_data_names: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        object.__setattr__(self, "strategy_data_names", frozenset(self.strategy_data_names))
        object.__setattr__(self, "security_data_names", frozenset(self.security_data_names))
        object.__setattr__(self, "valuation_data_names", frozenset(self.valuation_data_names))


@dataclass(frozen=True, slots=True)
class _RoutedData:
    strategy: tuple[_NamedData, ...]
    security: tuple[MarketData | InstrumentStateData, ...]
    valuation: tuple[ValuationUpdate, ...]


class _TimeSliceRouter:
    def __init__(self, config: DataRoutingConfig) -> None:
        self.config = config

    def route(
        self,
        records: Iterable[_NamedData],
    ) -> _RoutedData:
        strategy: list[_NamedData] = []
        security: list[MarketData | InstrumentStateData] = []
        valuation: list[ValuationUpdate] = []

        for named in records:
            record = named.record
            if isinstance(record, OptionAnalyticsData):
                if named.data_name not in self.config.strategy_data_names:
                    raise ValueError(
                        f"option analytics source {named.data_name!r} "
                        "must route to strategy"
                    )
                if named.data_name in self.config.security_data_names:
                    raise ValueError(
                        f"option analytics source {named.data_name!r} "
                        "cannot route to security"
                    )
                if named.data_name in self.config.valuation_data_names:
                    raise ValueError(
                        f"option analytics source {named.data_name!r} "
                        "cannot be a valuation source"
                    )
            if isinstance(record, InstrumentStateData):
                if named.data_name not in self.config.security_data_names:
                    raise ValueError(
                        f"instrument state source {named.data_name!r} must route to security"
                    )
                if named.data_name in self.config.valuation_data_names:
                    raise ValueError(
                        f"instrument state source {named.data_name!r} "
                        "cannot be a valuation source"
                    )
            if (
                named.data_name in self.config.strategy_data_names
                and isinstance(record, MarketData)
            ):
                strategy.append(named)
            if named.data_name in self.config.security_data_names:
                security.append(record)
            if (
                named.data_name in self.config.valuation_data_names
                and isinstance(record, MarketData)
            ):
                if record.value is None:
                    continue
                price = float(record.value)
                if not math.isfinite(price):
                    raise ValueError(
                        f"valuation price must be finite for {record.instrument_id!r}, "
                        f"got {record.value!r}"
                    )
                valuation.append(
                    ValuationUpdate(
                        instrument_id=record.instrument_id,
                        time=record.time,
                        price=price,
                        source=record,
                    )
                )

        return _RoutedData(
            strategy=tuple(strategy),
            security=tuple(
                sorted(
                    security,
                    key=lambda item: 0 if isinstance(item, InstrumentStateData) else 1,
                )
            ),
            valuation=tuple(valuation),
        )


__all__ = ["DataRoutingConfig"]
