"""Public facade for building routed TimeSlice streams."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from itertools import chain
import pickle
from pathlib import Path
from typing import Iterable

from autotrade.coreutils.object import (
    MarketData,
    InstrumentStateData,
    Slice,
    TimeSlice,
)
from autotrade.backtest.data.pipeline import (
    DataRoutingConfig,
    _DataStream,
    _DataSynchronizer,
    _RoutedData,
    _TimeSliceRouter,
)


@dataclass(slots=True)
class _DataSource:
    data_name: str
    records: Iterable[MarketData | InstrumentStateData]


class DataManager:
    """Read, synchronize, route and package every runtime data source."""

    def __init__(
        self,
        routing: DataRoutingConfig,
    ) -> None:
        self._router = _TimeSliceRouter(routing)
        self._synchronizer = _DataSynchronizer()
        self._sources: list[_DataSource] = []
        self._data_names: set[str] = set()
        self._consumed = False
        self._materialized: tuple[TimeSlice, ...] | None = None

    def add_data(
        self,
        data_name: str,
        records: Iterable[MarketData | InstrumentStateData],
    ) -> None:
        if self._consumed:
            raise RuntimeError("a consumed DataManager cannot accept new data")
        if not data_name or not data_name.strip():
            raise ValueError("data_name cannot be empty")
        if data_name in self._data_names:
            raise ValueError(f"duplicate data_name {data_name!r}")
        self._validate_routing(data_name)
        self._data_names.add(data_name)
        self._sources.append(_DataSource(data_name, records))

    def stream(self) -> Iterator[TimeSlice]:
        """Return either the lazy one-shot stream or a replayable cached iterator."""
        if self._materialized is not None:
            return iter(self._materialized)
        return self._stream_once()

    def _stream_once(self) -> Iterator[TimeSlice]:
        if self._consumed:
            raise RuntimeError("DataManager streams are single-use")
        configured = (
            self._router.config.strategy_data_names
            | self._router.config.security_data_names
            | self._router.config.valuation_data_names
        )
        unknown = configured - self._data_names
        if unknown:
            raise ValueError(f"routing references unknown data_names: {sorted(unknown)!r}")
        self._consumed = True

        try:
            bootstrap, streams = self._prepare_streams()
            if bootstrap:
                yield self._create_bootstrap(tuple(bootstrap))

            for batch in self._synchronizer.sync(streams):
                yield self._create_time_slice(
                    batch.time,
                    self._router.route(batch.records),
                )
        finally:
            self._sources.clear()

    def materialize(self) -> tuple[TimeSlice, ...]:
        """Build and retain every TimeSlice, releasing registered source streams."""
        if self._materialized is not None:
            return self._materialized
        if self._consumed:
            raise RuntimeError(
                "cannot materialize a DataManager after streaming has started"
            )

        materialized = tuple(self._stream_once())
        self._materialized = materialized
        return materialized

    @property
    def is_materialized(self) -> bool:
        return self._materialized is not None

    @property
    def time_slice_count(self) -> int:
        if self._materialized is None:
            raise RuntimeError("DataManager is not materialized")
        return len(self._materialized)

    def save(self, path: str | Path) -> None:
        """Persist a materialized manager without raw Reader source streams."""
        if self._materialized is None:
            raise RuntimeError("DataManager must be materialized before saving")
        if self._sources:
            raise RuntimeError("materialized DataManager still holds source streams")

        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("wb") as file:
            pickle.dump(self, file, protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    def load(cls, path: str | Path) -> "DataManager":
        """Load and validate a previously materialized manager."""
        with Path(path).open("rb") as file:
            manager = pickle.load(file)
        if not isinstance(manager, cls):
            raise TypeError(
                f"expected {cls.__name__}, got {type(manager).__name__}"
            )
        if manager._materialized is None:
            raise ValueError("saved DataManager is not materialized")
        if manager._sources:
            raise ValueError("saved DataManager unexpectedly contains source streams")
        return manager

    @staticmethod
    def _create_time_slice(
        when: datetime,
        routed: _RoutedData,
    ) -> TimeSlice:
        return TimeSlice(
            time=when,
            slice=Slice.from_named_data(
                when,
                ((item.data_name, item.record) for item in routed.strategy),
            ),
            security_updates=routed.security,
            valuation_updates=routed.valuation,
        )

    @staticmethod
    def _create_bootstrap(
        updates: tuple[InstrumentStateData, ...],
    ) -> TimeSlice:
        return TimeSlice(
            time=None,
            slice=Slice(time=None),
            security_updates=updates,
            is_bootstrap=True,
        )

    def _validate_routing(self, data_name: str) -> None:
        config = self._router.config
        destinations = (
            data_name in config.strategy_data_names,
            data_name in config.security_data_names,
            data_name in config.valuation_data_names,
        )
        if not any(destinations):
            raise ValueError(f"data source {data_name!r} has no routing destination")

    def _prepare_streams(
        self,
    ) -> tuple[list[InstrumentStateData], list[_DataStream]]:
        bootstrap: list[InstrumentStateData] = []
        streams: list[_DataStream] = []
        for source in self._sources:
            iterator = iter(source.records)
            first_timed = None
            for record in iterator:
                if record.time is None:
                    if not isinstance(record, InstrumentStateData):
                        raise TypeError("only instrument state may have time=None")
                    if source.data_name not in self._router.config.security_data_names:
                        raise ValueError(
                            f"bootstrap source {source.data_name!r} must route to security"
                        )
                    bootstrap.append(record)
                    continue
                first_timed = record
                break

            if first_timed is not None:
                records = chain((first_timed,), iterator)
                streams.append(
                    _DataStream(
                        data_name=source.data_name,
                        data=records,
                    )
                )
        return bootstrap, streams


__all__ = ["DataManager"]
