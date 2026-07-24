"""Coordinator for the shared-security, shared-OMS backtest runtime."""

from __future__ import annotations

from collections.abc import Iterable

from autotrade.coreutils.constant import LogLevel
from autotrade.coreutils.logger import get_logger
from autotrade.coreutils.object import LogData
from autotrade.backtest.performance_analyzer import PerformanceAnalyzer
from autotrade.backtest.backtest_gateway import BacktestSettings, BacktestGateway, BarFillModel, FillModel
from autotrade.engine.oms_engine import OmsBase
from autotrade.engine.clock import BacktestClock
from autotrade.engine.order_router import OrderRouter
from autotrade.engine.timeslice_driver import TimeSliceDriver
from autotrade.engine.security_manager import SecurityManager
from autotrade.coreutils.object import TimeSlice
from autotrade.backtest.backtest_event_engine import (
    EVENT_LOG,
    Event,
    BacktestEventEngine,
)

class BacktestEngine:
    """Coordinate TimeSlice progression across decoupled backtest modules."""

    def __init__(
        self,
        event_engine: BacktestEventEngine | None = None,
        *,
        initial_cash: float = 1_000_000,
        risk_free: float = 0.02,
        annual_days: int = 240,
        engine_id: str = "backtest",
        logger=None,
        settings: BacktestSettings | None = None,
        mkt_order_match_mode: str = "NEXT_BAR_OPEN",
        execution_data_name: str | None = None,
        security_manager: SecurityManager | None = None,
        fill_model: FillModel | None = None,
        oms: OmsBase | None = None,
        performance_analyzer: PerformanceAnalyzer | None = None,
        gateway: BacktestGateway | None = None,
        clock: BacktestClock | None = None,
        order_router: OrderRouter | None = None,
    ):
        if event_engine is None:
            event_engine = BacktestEventEngine()

        self.gateway_name = "backtest"
        self.event_engine = event_engine
        self.initial_cash = initial_cash
        self.risk_free = risk_free
        self.annual_days = annual_days
        self.logger = logger or get_logger(name=engine_id, logfile=f"{engine_id}.log")
        self.current_datetime = None
        self.clock = clock or BacktestClock()
        self.backtest_res: dict = {}
        self.settings = settings or BacktestSettings(
            cheat_on_close=mkt_order_match_mode == "CURRENT_BAR_CLOSE"
        )
        if execution_data_name is not None:
            self.settings.execution_data_name = execution_data_name
        fill_model = fill_model or BarFillModel()
        self.security_manager = security_manager or SecurityManager()
        self.security_manager.bind(self.event_engine)
        self.oms = oms or OmsBase(self.event_engine)
        if self.oms.event_engine is not self.event_engine:
            raise ValueError("oms must use the engine event_engine")
        self.performance_analyzer = performance_analyzer or PerformanceAnalyzer(
            initial_cash=self.initial_cash,
            risk_free=self.risk_free,
            annual_days=self.annual_days,
        )
        self.gateway = gateway or BacktestGateway(
            gateway_name=self.gateway_name,
            event_engine=self.event_engine,
            fill_model=fill_model,
            settings=self.settings,
            security_manager=self.security_manager,
            initial_cash=self.initial_cash,
            clock=self.clock,
        )
        if self.gateway.event_engine is not self.event_engine:
            raise ValueError("gateway must use the engine event_engine")
        if self.gateway.security_manager is not self.security_manager:
            raise ValueError("gateway must use the engine security_manager")
        self.order_router = order_router or OrderRouter(self.event_engine)
        self.timeslice_driver = TimeSliceDriver(
            self.event_engine,
            clock=self.clock,
            simulated_broker=True,
            source=engine_id,
        )
        self.gateway.publish_initial_state()
        self.symbols: list[str] = []
        self.processed_slice_count = 0
        self.register_event()

    def register_event(self):
        """Register engine-owned consumers only.

        Routed order commands are owned by OrderRouter and BacktestGateway.
        """
        self.event_engine.register(EVENT_LOG, self._on_log)

    @property
    def account_daily(self):
        return self.gateway.recorder.account_daily

    @property
    def contract_daily(self):
        return self.gateway.recorder.contract_daily

    @property
    def position_daily(self):
        return self.gateway.recorder.position_daily

    def run(self, time_slices: Iterable[TimeSlice]):
        print("TimeSlice流式回测开始")

        self.processed_slice_count = 0
        for time_slice in time_slices:
            if time_slice.is_bootstrap:
                self.timeslice_driver.process(time_slice)
                continue
            self.current_datetime = time_slice.time
            self.on_time_slice(time_slice)
            self.processed_slice_count += 1

        self.symbols = sorted(self.security_manager.securities)

        if self.account_daily:
            self.backtest_res = self.calculate_statistics()
        else:
            self.backtest_res = {}
        print("TimeSlice回测结束")

    def on_time_slice(self, time_slice: TimeSlice):
        """Advance one deterministic TimeSlice through routed phase commands."""
        self.timeslice_driver.process(time_slice)

    def push_log_event(self, log_data: LogData):
        self.event_engine.put(Event(EVENT_LOG, log_data))

    def _on_log(self, event: Event):
        log_data: LogData = event.data
        prefix = f"BackTestTime:{self.current_datetime} {log_data.msg}"
        if log_data.level == LogLevel.DEBUG:
            self.logger.debug(prefix)
        elif log_data.level == LogLevel.INFO:
            self.logger.info(prefix)
        elif log_data.level == LogLevel.WARNING:
            self.logger.warning(prefix)
        elif log_data.level == LogLevel.ERROR:
            self.logger.error(prefix)

    def calculate_statistics(self):
        return self.performance_analyzer.calculate(self.account_daily)

    def get_trade_log_df(self):
        return self.gateway.recorder.get_trade_log_df(self.oms)

    def get_account_daily_df(self):
        return self.gateway.recorder.get_account_daily_df()

    def performance_plot(self, *args, **kwargs):
        raise RuntimeError(
            "market-data plotting is not recorded by the streaming engine; "
            "attach a dedicated recorder when chart data is required"
        )
