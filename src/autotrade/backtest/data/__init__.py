"""Clean end-time data pipeline for autotrade backtests."""

from .data_manager import DataManager
from .reader import (
    CustomDataReader,
    DataReader,
    EquityStateReader,
    FutureStateReader,
    OptionStateReader,
    TickReader,
    TradeBarReader,
)
from .pipeline import DataRoutingConfig
from .reader.base import FIELD_ALIASES, SchemaResolver

__all__ = [
    "CustomDataReader",
    "DataManager",
    "DataReader",
    "DataRoutingConfig",
    "EquityStateReader",
    "FIELD_ALIASES",
    "FutureStateReader",
    "OptionStateReader",
    "SchemaResolver",
    "TickReader",
    "TradeBarReader",
]
