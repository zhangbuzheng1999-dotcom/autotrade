"""Private instrument-frame normalization used by instrument readers.

It normalizes the three columns shared by every instrument state:

``date``
    Effective time of the state. ``NaT`` means bootstrap state and must not be
    sent to the chronological data synchronizer.
``symbol``
    Canonical instrument identifier.
``is_active``
    Whether the instrument is active from this state onward.

All asset-specific columns are preserved for the equity/future/option reader
that invoked it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from autotrade.coreutils.object import (
    EquityStateData,
    FutureStateData,
    InstrumentStateData,
    OptionStateData,
)
from autotrade.backtest.data.reader.base import (
    DataReader,
    datetime_or_none,
    float_or_default,
    float_or_none,
    row_getter,
    string_or_none,
)

@dataclass(frozen=True, slots=True)
class _InstrumentFrameNormalizer:
    """Normalize an instrument-information frame into lifecycle snapshots.

    Existing dated rows are treated as complete active-state snapshots.
    ``list_date`` creates an active row when one does not already exist at that
    time. ``delist_date`` creates an inactive row copied from the latest known
    state at or before that time.

    An undated row without ``list_date`` remains undated.  This is intentional:
    downstream loading code can split ``date.isna()`` rows and use them to
    bootstrap ``SecurityManager`` before the chronological backtest starts.
    """

    symbol_column: str = "symbol"
    date_column: str = "date"
    list_date_column: str = "list_date"
    delist_date_column: str = "delist_date"
    active_column: str = "is_active"

    def expand(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Return an expanded copy with canonical lifecycle columns.

        The returned frame always contains ``date``, ``symbol`` and
        ``is_active`` (or the configured output column names). Input rows are
        never mutated.
        """
        if not isinstance(frame, pd.DataFrame):
            raise TypeError("frame must be a pandas DataFrame")
        if self.symbol_column not in frame.columns:
            raise ValueError(
                f"missing required instrument symbol column {self.symbol_column!r}"
            )

        expanded = frame.copy()
        self._validate_symbols(expanded)
        self._normalize_date_column(expanded, self.date_column)
        self._normalize_optional_date_column(expanded, self.list_date_column)
        self._normalize_optional_date_column(expanded, self.delist_date_column)
        if self.active_column not in expanded.columns:
            expanded[self.active_column] = True
        elif expanded[self.active_column].isna().any():
            raise ValueError("instrument active state cannot be missing")
        else:
            expanded[self.active_column] = expanded[self.active_column].astype(bool)

        output: list[pd.DataFrame] = []
        for _, group in expanded.groupby(
            self.symbol_column,
            sort=False,
            dropna=False,
        ):
            output.append(self._expand_symbol(group.copy()))

        if not output:
            return expanded.reset_index(drop=True)

        result = pd.concat(output, ignore_index=True, sort=False)
        result["_lifecycle_order"] = result[self.active_column].map(
            {True: 0, False: 1}
        )
        result = (
            result.sort_values(
                [self.date_column, self.symbol_column, "_lifecycle_order"],
                kind="mergesort",
                na_position="first",
            )
            .drop(columns="_lifecycle_order")
            .reset_index(drop=True)
        )
        result[self.active_column] = result[self.active_column].astype(bool)
        return result

    def _expand_symbol(self, group: pd.DataFrame) -> pd.DataFrame:
        symbol = str(group[self.symbol_column].iloc[0])
        list_date = self._unique_date(group, self.list_date_column, symbol)
        delist_date = self._unique_date(group, self.delist_date_column, symbol)

        if (
            list_date is not None
            and delist_date is not None
            and list_date > delist_date
        ):
            raise ValueError(
                f"instrument {symbol!r} has list_date after delist_date"
            )

        dated = group[group[self.date_column].notna()].sort_values(
            self.date_column,
            kind="mergesort",
        )
        undated = group[group[self.date_column].isna()]

        if dated.empty and len(undated) > 1:
            raise ValueError(
                f"instrument {symbol!r} has multiple undated states; "
                "provide a date for changing instrument attributes"
            )

        rows = [group]

        if list_date is not None and not self._contains_date(
            group,
            self.date_column,
            list_date,
        ):
            source = self._state_for_listing(dated, undated, list_date, symbol)
            active = source.copy()
            active[self.date_column] = list_date
            active[self.active_column] = True
            rows.append(active.to_frame().T)

        if delist_date is not None:
            source = self._state_for_delisting(
                dated,
                undated,
                delist_date,
                symbol,
            )
            inactive = source.copy()
            inactive[self.date_column] = delist_date
            inactive[self.active_column] = False
            rows.append(inactive.to_frame().T)

        result = pd.concat(rows, ignore_index=True, sort=False)

        # When list_date supplies the effective time for a single undated
        # definition, the synthesized ACTIVE row replaces the bootstrap row.
        if list_date is not None and dated.empty:
            result = result[result[self.date_column].notna()]

        return result

    def _state_for_listing(
        self,
        dated: pd.DataFrame,
        undated: pd.DataFrame,
        list_date: pd.Timestamp,
        symbol: str,
    ) -> pd.Series:
        candidates = dated[dated[self.date_column] <= list_date]
        if not candidates.empty:
            return candidates.iloc[-1]
        if not undated.empty:
            return undated.iloc[-1]
        raise ValueError(
            f"instrument {symbol!r} has no state available at list_date "
            f"{list_date!s}"
        )

    def _state_for_delisting(
        self,
        dated: pd.DataFrame,
        undated: pd.DataFrame,
        delist_date: pd.Timestamp,
        symbol: str,
    ) -> pd.Series:
        candidates = dated[dated[self.date_column] <= delist_date]
        if not candidates.empty:
            return candidates.iloc[-1]
        if not undated.empty:
            return undated.iloc[-1]
        raise ValueError(
            f"instrument {symbol!r} has no state available at delist_date "
            f"{delist_date!s}"
        )

    def _validate_symbols(self, frame: pd.DataFrame) -> None:
        symbols = frame[self.symbol_column]
        if symbols.isna().any():
            raise ValueError("instrument symbol cannot be missing")
        if symbols.astype(str).str.strip().eq("").any():
            raise ValueError("instrument symbol cannot be empty")

    @staticmethod
    def _normalize_date_column(frame: pd.DataFrame, column: str) -> None:
        if column not in frame.columns:
            frame[column] = pd.NaT
            return
        frame[column] = pd.to_datetime(frame[column], errors="raise")

    @staticmethod
    def _normalize_optional_date_column(
        frame: pd.DataFrame,
        column: str,
    ) -> None:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="raise")

    @staticmethod
    def _contains_date(
        frame: pd.DataFrame,
        column: str,
        value: pd.Timestamp,
    ) -> bool:
        return bool(frame[column].eq(value).any())

    @staticmethod
    def _unique_date(
        frame: pd.DataFrame,
        column: str,
        symbol: str,
    ) -> pd.Timestamp | None:
        if column not in frame.columns:
            return None
        values = pd.unique(frame[column].dropna())
        if len(values) > 1:
            raise ValueError(
                f"instrument {symbol!r} has conflicting {column} values"
            )
        if len(values) == 0:
            return None
        return pd.Timestamp(values[0])


class InstrumentStateReader(DataReader):
    """Normalize instrument frames into complete state snapshots."""

    state_type = InstrumentStateData
    extra_fields: tuple[str, ...] = ()

    def read(self, source: Any, *, exchange=None):
        frame = self.frame(source)
        expanded = _InstrumentFrameNormalizer(
            symbol_column=self.schema.get("symbol", "symbol"),
            date_column=self.schema.get("date", "date"),
            list_date_column=self.schema.get("list_date", "list_date"),
            delist_date_column=self.schema.get("delist_date", "delist_date"),
            active_column=self.schema.get("is_active", "is_active"),
        ).expand(frame)

        mapping = self.schema_resolver.resolve(
            expanded,
            required=("symbol", "date", "is_active"),
            optional=(
                "list_date",
                "delist_date",
                "multiplier",
                "margin_rate",
                "commission_rate",
                "long_commission_rate",
                "short_commission_rate",
                *self.extra_fields,
            ),
            schema=self.schema,
        )
        get = row_getter(expanded)
        return tuple(
            self._make_state(exchange, expanded, row, mapping, get)
            for row in expanded.itertuples(index=False)
        )

    def _make_state(self, exchange, frame, row, mapping, get):
        known_columns = set(mapping.values())
        common = dict(
            symbol=str(get(row, mapping["symbol"])),
            time=datetime_or_none(get(row, mapping["date"])),
            is_active=bool(get(row, mapping["is_active"])),
            exchange=exchange,
            multiplier=float_or_default(
                get(row, mapping.get("multiplier")),
                1.0,
            ),
            margin_rate=float_or_default(
                get(row, mapping.get("margin_rate")),
                0.0,
            ),
            commission_rate=float_or_default(
                get(row, mapping.get("commission_rate")),
                0.0,
            ),
            long_commission_rate=float_or_none(
                get(row, mapping.get("long_commission_rate"))
            ),
            short_commission_rate=float_or_none(
                get(row, mapping.get("short_commission_rate"))
            ),
            list_date=datetime_or_none(get(row, mapping.get("list_date"))),
            delist_date=datetime_or_none(get(row, mapping.get("delist_date"))),
            attributes={
                column: get(row, column)
                for column in frame.columns
                if column not in known_columns
            },
        )
        return self._create_state(common, row, get, mapping)

    def _create_state(self, common, row, get, mapping):
        return self.state_type(**common)


class EquityStateReader(InstrumentStateReader):
    state_type = EquityStateData


class FutureStateReader(InstrumentStateReader):
    state_type = FutureStateData
    extra_fields = ("expiry", "root_symbol")

    def _create_state(self, common, row, get, mapping):
        return FutureStateData(
            **common,
            expiry=datetime_or_none(get(row, mapping.get("expiry"))),
            root_symbol=string_or_none(get(row, mapping.get("root_symbol"))),
        )


class OptionStateReader(InstrumentStateReader):
    state_type = OptionStateData
    extra_fields = ("underlying_symbol", "expiry", "strike", "right", "style")

    def _create_state(self, common, row, get, mapping):
        return OptionStateData(
            **common,
            underlying_symbol=string_or_none(
                get(row, mapping.get("underlying_symbol"))
            ),
            expiry=datetime_or_none(get(row, mapping.get("expiry"))),
            strike=float_or_none(get(row, mapping.get("strike"))),
            right=string_or_none(get(row, mapping.get("right"))),
            style=string_or_none(get(row, mapping.get("style"))),
        )


__all__ = [
    "EquityStateReader",
    "FutureStateReader",
    "InstrumentStateReader",
    "OptionStateReader",
]
