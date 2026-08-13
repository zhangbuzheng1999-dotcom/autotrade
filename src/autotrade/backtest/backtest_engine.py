"""Explicit composition root for the finite, synchronous backtest runtime."""

from __future__ import annotations

from collections.abc import Iterable

from autotrade.backtest.event_engine import BacktestEventEngine
from autotrade.backtest.gateway import (
    BacktestGateway,
    BacktestSettings,
    BarFillModel,
    FillModel,
)
from autotrade.backtest.reporting import BacktestReporting, PerformanceAnalyzer
from autotrade.coreutils.object import TimeSlice
from autotrade.engine.log_engine import LogEngine
from autotrade.engine.oms import OmsBase
from autotrade.engine.order_router import OrderRouter
from autotrade.engine.runtime_engine import (
    RuntimeContext,
    RuntimeEngine,
    build_runtime_components,
)
from autotrade.engine.security_manager import SecurityManager


class BacktestEngine(RuntimeEngine):
    """Assemble shared runtime components around a simulated broker."""

    def __init__(
        self,
        event_engine: BacktestEventEngine | None = None,
        *,
        initial_cash: float = 1_000_000,
        risk_free: float = 0.02,
        annual_days: int = 252,
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
        order_router: OrderRouter | None = None,
        log_engine: LogEngine | None = None,
        reporting: BacktestReporting | None = None,
    ) -> None:
        event_engine = event_engine or BacktestEventEngine()
        context = RuntimeContext(engine_id=engine_id)
        settings = settings or BacktestSettings(
            cheat_on_close=mkt_order_match_mode == "CURRENT_BAR_CLOSE"
        )
        if execution_data_name is not None:
            settings.execution_data_name = execution_data_name

        def create_gateway(engine, securities):
            return gateway or BacktestGateway(
                gateway_name="backtest",
                event_engine=engine,
                fill_model=fill_model or BarFillModel(),
                settings=settings,
                security_manager=securities,
                initial_cash=initial_cash,
            )

        components = build_runtime_components(
            event_engine=event_engine,
            context=context,
            gateway_factory=create_gateway,
            simulated_broker=True,
            security_manager=security_manager,
            oms=oms,
            order_router=order_router,
            log_engine=log_engine,
            logger=logger,
        )
        super().__init__(context=context, components=components)
        gateway = self.gateway
        oms = self.oms
        if gateway.security_manager is not self.security_manager:
            raise ValueError("gateway must use the runtime security_manager")

        analyzer = performance_analyzer or PerformanceAnalyzer(
            initial_cash=initial_cash,
            risk_free=risk_free,
            annual_days=annual_days,
        )
        self.reporting = reporting or BacktestReporting(
            recorder=gateway.recorder,
            analyzer=analyzer,
            oms=oms,
        )
        if self.reporting.recorder is not gateway.recorder:
            raise ValueError("reporting must use the gateway recorder")
        if self.reporting.oms is not oms:
            raise ValueError("reporting must use the runtime oms")
        self.settings = settings
        self.initial_cash = initial_cash
        self.risk_free = risk_free
        self.annual_days = annual_days
        self.backtest_res: dict = {}
        self.symbols: list[str] = []
        self.processed_slice_count = 0
        self.gateway.publish_initial_state()

    @property
    def account_daily(self):
        return self.reporting.recorder.account_daily

    @property
    def contract_daily(self):
        return self.reporting.recorder.contract_daily

    @property
    def position_daily(self):
        return self.reporting.recorder.position_daily

    def run(self, time_slices: Iterable[TimeSlice]) -> dict:
        print("TimeSlice流式回测开始")
        self.processed_slice_count = 0
        for time_slice in time_slices:
            self.timeslice_driver.process(time_slice)
            if not time_slice.is_bootstrap:
                self.processed_slice_count += 1
        self.symbols = sorted(self.security_manager.securities)
        self.backtest_res = self.reporting.calculate()
        print("TimeSlice回测结束")
        return self.backtest_res

    def on_time_slice(self, time_slice: TimeSlice) -> None:
        self.timeslice_driver.process(time_slice)

    def calculate_statistics(self):
        return self.reporting.calculate()

    def get_trade_log_df(self):
        return self.reporting.get_trade_log_df()

    def get_account_daily_df(self):
        return self.reporting.get_account_daily_df()

    def performance_plot(self, *args, **kwargs):
        raise RuntimeError(
            "market-data plotting is not recorded by the streaming engine; "
            "attach a dedicated recorder when chart data is required"
        )


__all__ = ["BacktestEngine"]
