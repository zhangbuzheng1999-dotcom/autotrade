"""Shared autotrade backtest reader contract and scalar conversion helpers."""

from __future__ import annotations

import math
from dataclasses import dataclass
from collections.abc import Iterable
from typing import Any

import pandas as pd


FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "instrument_id": (
        "instrument_id",
        "order_book_id",
        "symbol",
        "instrument",
        "code",
        "ticker",
    ),
    "time": ("time", "datetime", "end_time", "timestamp"),
    "open": ("open", "open_price"),
    "high": ("high", "high_price"),
    "low": ("low", "low_price"),
    "close": ("close", "close_price", "last", "last_price"),
    "volume": ("volume", "vol"),
    "turnover": ("turnover", "amount"),
    "open_interest": ("open_interest", "oi"),
    "price": ("price", "last", "last_price"),
    "quantity": ("quantity", "qty", "volume"),
    "bid": ("bid", "bid1", "bid_price", "best_bid"),
    "ask": ("ask", "ask1", "ask_price", "best_ask"),
    "bid_size": ("bid_size", "bid_volume", "bid1_volume"),
    "ask_size": ("ask_size", "ask_volume", "ask1_volume"),
    "value": ("value", "factor_value", "score"),
    "date": ("date", "effective_date", "effective_time"),
    "list_date": ("list_date", "listed_date", "start_date"),
    "delist_date": ("delist_date", "delisted_date", "end_date"),
    "is_active": ("is_active", "active"),
    "multiplier": ("multiplier", "contract_multiplier", "size"),
    "margin_rate": ("margin_rate", "initial_margin_rate"),
    "commission_rate": ("commission_rate", "fee_rate"),
    "long_commission_rate": ("long_commission_rate", "long_rate"),
    "short_commission_rate": ("short_commission_rate", "short_rate"),
    "expiry": ("expiry", "expiry_date", "maturity_date", "maturity"),
    "root_instrument_id": (
        "root_instrument_id",
        "root_symbol",
        "product",
        "underlying",
    ),
    "underlying_instrument_id": (
        "underlying_instrument_id",
        "underlying_symbol",
        "underlying",
        "underlier",
    ),
    "strike": ("strike", "strike_price", "exercise_price"),
    "right": ("right", "option_type", "call_put"),
    "style": ("style", "exercise_style"),
}


@dataclass(slots=True)
class SchemaResolver:
    aliases: dict[str, tuple[str, ...]] | None = None

    def resolve(
        self,
        frame: pd.DataFrame,
        required: Iterable[str],
        schema: dict[str, str] | None = None,
        optional: Iterable[str] = (),
    ) -> dict[str, str]:
        schema = schema or {}
        aliases = self.aliases or FIELD_ALIASES
        columns = set(frame.columns)
        resolved: dict[str, str] = {}

        for field in tuple(required) + tuple(optional):
            if field in schema:
                source = schema[field]
                if source not in columns:
                    raise ValueError(
                        f"schema field '{field}' maps to missing column '{source}'"
                    )
                resolved[field] = source
                continue

            candidates = aliases.get(field, (field,))
            source = next(
                (candidate for candidate in candidates if candidate in columns),
                None,
            )
            if source is not None:
                resolved[field] = source
            elif field in required:
                raise ValueError(
                    f"missing required field '{field}'. Provide schema mapping "
                    f"or one of aliases {candidates}"
                )

        return resolved


class DataReader:
    """Convert one raw source into standardized data objects."""

    def __init__(
        self,
        schema: dict[str, str] | None = None,
        *,
        schema_resolver: SchemaResolver | None = None,
    ) -> None:
        self.schema = dict(schema or {})
        self.schema_resolver = schema_resolver or SchemaResolver()

    def read(self, source: Any, **kwargs):
        raise NotImplementedError

    @staticmethod
    def frame(source: Any) -> pd.DataFrame:
        if isinstance(source, pd.DataFrame):
            return source
        return pd.DataFrame(source)


def row_getter(frame: pd.DataFrame):
    index = {field: idx for idx, field in enumerate(frame.columns)}

    def get(row, column: str | None):
        if column is None:
            return None
        return row[index[column]]

    return get


def to_datetime(value):
    result = pd.Timestamp(value)
    if pd.isna(result):
        raise ValueError("time field cannot be NaT")
    return result.to_pydatetime()


def datetime_or_none(value):
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).to_pydatetime()


def finite_float(value, field_name: str) -> float:
    converted = float(value)
    if not math.isfinite(converted):
        raise ValueError(f"{field_name} must be finite, got {value!r}")
    return converted


def float_or_none(value) -> float | None:
    if value is None or pd.isna(value):
        return None
    return finite_float(value, "numeric field")


def float_or_zero(value) -> float:
    converted = float_or_none(value)
    return 0.0 if converted is None else converted


def float_or_default(value, default: float) -> float:
    converted = float_or_none(value)
    return default if converted is None else converted


def string_or_none(value) -> str | None:
    if value is None or pd.isna(value):
        return None
    return str(value)


__all__ = ["DataReader", "FIELD_ALIASES", "SchemaResolver"]
