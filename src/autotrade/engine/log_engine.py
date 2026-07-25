"""Event-driven logging shared by live and backtest runtimes."""

from __future__ import annotations

from typing import TYPE_CHECKING

from autotrade.coreutils.constant import LogLevel
from autotrade.coreutils.object import LogData
from autotrade.engine.event_engine import EVENT_LOG, Event, EventEngine

if TYPE_CHECKING:
    from autotrade.engine.runtime_engine import RuntimeContext


class LogEngine:
    """Consume EVENT_LOG using the current runtime context."""

    def __init__(
        self,
        event_engine: EventEngine,
        context: "RuntimeContext",
        *,
        logger=None,
    ) -> None:
        self.event_engine = event_engine
        self.context = context
        if logger is None:
            from autotrade.coreutils.logger import get_logger

            logger = get_logger(
                name=context.engine_id,
                logfile=f"{context.engine_id}.log",
            )
        self.logger = logger
        self.event_engine.register(EVENT_LOG, self.process_event)

    def process_event(self, event: Event) -> None:
        log: LogData = event.data
        prefix = (
            f"RuntimeTime:{self.context.current_time} {log.msg}"
            if self.context.current_time is not None
            else log.msg
        )
        method = {
            LogLevel.DEBUG: self.logger.debug,
            LogLevel.INFO: self.logger.info,
            LogLevel.WARNING: self.logger.warning,
            LogLevel.ERROR: self.logger.error,
        }.get(log.level, self.logger.info)
        method(prefix)

    def unregister(self) -> None:
        self.event_engine.unregister(EVENT_LOG, self.process_event)


__all__ = ["LogEngine"]
