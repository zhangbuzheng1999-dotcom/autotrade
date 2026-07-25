"""Shared runtime assembly, context, lifecycle, and event-driven logging."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from autotrade.coreutils.object import LogData
from autotrade.engine.event_engine import EVENT_LOG, Event, EventEngine
from autotrade.engine.log_engine import LogEngine
from autotrade.engine.oms import OmsBase
from autotrade.engine.order_router import OrderRouter
from autotrade.engine.security_manager import SecurityManager
from autotrade.engine.timeslice_driver import TimeSliceDriver


@dataclass(slots=True)
class RuntimeContext:
    """Observable runtime state; it records time but never drives it."""

    engine_id: str
    current_time: datetime | None = None


@dataclass(slots=True)
class RuntimeComponents:
    """Explicit inventory of components shared by live and backtest runtimes."""

    event_engine: EventEngine
    security_manager: SecurityManager
    oms: OmsBase
    order_router: OrderRouter
    timeslice_driver: TimeSliceDriver
    gateway: Any
    log_engine: "LogEngine"


def build_runtime_components(
    *,
    event_engine: EventEngine,
    context: RuntimeContext,
    gateway_factory: Callable[[EventEngine, SecurityManager], Any],
    simulated_broker: bool,
    security_manager: SecurityManager | None = None,
    oms: OmsBase | None = None,
    order_router: OrderRouter | None = None,
    log_engine: "LogEngine | None" = None,
    logger=None,
) -> RuntimeComponents:
    """Build the common component graph; callers only choose runtime variants."""
    security_manager = security_manager or SecurityManager()
    security_manager.bind(event_engine)
    oms = oms or OmsBase(event_engine)
    order_router = order_router or OrderRouter(event_engine)
    timeslice_driver = TimeSliceDriver(
        event_engine,
        simulated_broker=simulated_broker,
        source=context.engine_id,
        context=context,
    )
    gateway = gateway_factory(event_engine, security_manager)
    log_engine = log_engine or LogEngine(event_engine, context, logger=logger)
    return RuntimeComponents(
        event_engine=event_engine,
        security_manager=security_manager,
        oms=oms,
        order_router=order_router,
        timeslice_driver=timeslice_driver,
        gateway=gateway,
        log_engine=log_engine,
    )


class RuntimeEngine:
    """Common component container and plugin lifecycle for every runtime."""

    def __init__(
        self,
        *,
        context: RuntimeContext,
        components: RuntimeComponents,
    ) -> None:
        self.context = context
        self.components = components
        self._plugins: list[Any] = []
        self._validate_components()

    def _validate_components(self) -> None:
        event_engine = self.components.event_engine
        for name in ("oms", "order_router", "timeslice_driver", "log_engine"):
            component = getattr(self.components, name)
            if component.event_engine is not event_engine:
                raise ValueError(f"{name} must use the runtime event_engine")
        gateway_engine = getattr(self.components.gateway, "event_engine", event_engine)
        if gateway_engine is not event_engine:
            raise ValueError("gateway must use the runtime event_engine")
        if self.components.timeslice_driver.context is not self.context:
            raise ValueError("timeslice_driver must use the runtime context")
        if self.components.log_engine.context is not self.context:
            raise ValueError("log_engine must use the runtime context")

    @property
    def event_engine(self):
        return self.components.event_engine

    @property
    def security_manager(self):
        return self.components.security_manager

    @property
    def oms(self):
        return self.components.oms

    @property
    def order_router(self):
        return self.components.order_router

    @property
    def timeslice_driver(self):
        return self.components.timeslice_driver

    @property
    def gateway(self):
        return self.components.gateway

    @property
    def log_engine(self):
        return self.components.log_engine

    def install(self, plugin: Any) -> Any:
        """Keep a plugin in the runtime lifecycle after it registers itself."""
        if plugin not in self._plugins:
            self._plugins.append(plugin)
        return plugin

    def uninstall(self, plugin: Any) -> None:
        if plugin in self._plugins:
            self._plugins.remove(plugin)
        stop = getattr(plugin, "stop", None) or getattr(plugin, "unregister", None)
        if stop:
            stop()

    def push_log_event(self, log_data: LogData) -> None:
        self.event_engine.put(Event(EVENT_LOG, log_data))


__all__ = [
    "RuntimeComponents",
    "RuntimeContext",
    "RuntimeEngine",
    "build_runtime_components",
]
