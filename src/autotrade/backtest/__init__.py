"""TimeSlice-driven backtesting components."""

from .backtest_engine import BacktestEngine
from .data import DataManager
from .reporting import BacktestRecorder, BacktestReporting, PerformanceAnalyzer
from .event_engine import (
    EVENT_DATA,
    EVENT_SLICE,
    EVENT_REQUEST,
    EVENT_REQUEST_STATUS,
    BacktestEventEngine,
    Event,
)
from .gateway import (
    AccountLedger,
    BacktestGateway,
    BacktestSettings,
    BarFillModel,
    CommissionModel,
    Fill,
    FillContext,
    FillModel,
    MarginModel,
    MatchingEngine,
)
from autotrade.engine.security_manager import SecurityManager

__all__ = [
    "BacktestEngine",
    "DataManager",
    "BacktestRecorder",
    "BacktestReporting",
    "AccountLedger",
    "CommissionModel",
    "BacktestEventEngine",
    "BacktestGateway",
    "BacktestSettings",
    "BarFillModel",
    "EVENT_DATA",
    "EVENT_SLICE",
    "EVENT_REQUEST",
    "EVENT_REQUEST_STATUS",
    "Event",
    "Fill",
    "FillContext",
    "FillModel",
    "MarginModel",
    "MatchingEngine",
    "PerformanceAnalyzer",
    "SecurityManager",
]
