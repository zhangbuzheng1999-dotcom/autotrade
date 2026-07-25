"""Explicit composition root for the live trading runtime."""

from __future__ import annotations

from collections.abc import Callable

from autotrade.engine.event_engine import EventEngine
from autotrade.engine.data_manager import LiveDataManager
from autotrade.engine.log_engine import LogEngine
from autotrade.engine.oms import OmsBase
from autotrade.engine.order_router import OrderRouter
from autotrade.engine.runtime_engine import (
    RuntimeContext,
    RuntimeEngine,
    build_runtime_components,
)
from autotrade.engine.security_manager import SecurityManager


class LiveEngine(RuntimeEngine):
    """Assemble shared runtime components around an asynchronous live gateway."""

    def __init__(
        self,
        *,
        gateway=None,
        gateway_factory: Callable[[EventEngine], object] | None = None,
        event_engine: EventEngine | None = None,
        engine_id: str = "live",
        security_manager: SecurityManager | None = None,
        oms: OmsBase | None = None,
        order_router: OrderRouter | None = None,
        log_engine: LogEngine | None = None,
        logger=None,
    ) -> None:
        event_engine = event_engine or EventEngine()
        context = RuntimeContext(engine_id=engine_id)
        if gateway is None and gateway_factory is None:
            raise ValueError("LiveEngine requires gateway or gateway_factory")

        def create_gateway(engine, _security_manager):
            return gateway if gateway is not None else gateway_factory(engine)

        components = build_runtime_components(
            event_engine=event_engine,
            context=context,
            gateway_factory=create_gateway,
            simulated_broker=False,
            security_manager=security_manager,
            oms=oms,
            order_router=order_router,
            log_engine=log_engine,
            logger=logger,
        )
        super().__init__(context=context, components=components)
        self.data_manager = LiveDataManager(
            event_engine,
            driver=self.timeslice_driver,
        )
        bind_execution = getattr(self.gateway, "bind_execution", None)
        if bind_execution is None:
            raise TypeError("live gateway must implement bind_execution()")
        bind_execution()
        self._started = False

    def start(self, setting: dict | None = None) -> None:
        if self._started:
            return
        self.event_engine.start()
        self.gateway.connect(setting or {})
        self._started = True

    def stop(self) -> None:
        if not self._started:
            return
        for plugin in reversed(tuple(self._plugins)):
            self.uninstall(plugin)
        self.gateway.close()
        self.data_manager.unregister()
        self.gateway.unbind_execution()
        self.order_router.unregister()
        self.log_engine.unregister()
        self.event_engine.stop()
        self._started = False


__all__ = ["LiveEngine"]
