"""Shared command router between strategies and execution adapters."""

from __future__ import annotations

from autotrade.coreutils.constant import LogLevel
from autotrade.coreutils.object import LogData
from autotrade.engine.event_engine import (
    COMMAND_ORDER_CANCEL,
    COMMAND_ORDER_MODIFY,
    COMMAND_ORDER_SUBMIT,
    EVENT_LOG,
    Event,
    EventEngine,
    Message,
    MessageKind,
)


class OrderRouter:
    target = "order_router"
    execution_target = "execution"

    def __init__(self, event_engine: EventEngine) -> None:
        self.event_engine = event_engine
        self.active = True
        self._muted_symbols: set[str] = set()
        self.register()

    def register(self) -> None:
        for name, handler in self._routes:
            self.event_engine.register_command(self.target, name, handler)

    def unregister(self) -> None:
        for name, handler in self._routes:
            self.event_engine.unregister_command(self.target, name, handler)

    @property
    def _routes(self):
        return (
            (COMMAND_ORDER_SUBMIT, self._submit),
            (COMMAND_ORDER_CANCEL, self._cancel),
            (COMMAND_ORDER_MODIFY, self._modify),
        )

    def mute(self, symbols, *, enabled: bool = True) -> None:
        if enabled:
            self._muted_symbols.update(symbols)
        else:
            self._muted_symbols.difference_update(symbols)

    def _submit(self, message: Message) -> None:
        request = message.data
        reference = getattr(request, "reference", "")
        internal = bool(
            reference
            and reference.startswith(("ENGINE:", "ROLL:", "RISK:"))
        )
        if not self.active:
            return
        if request.instrument_id in self._muted_symbols and not internal:
            self._log(f"[OrderRouter] blocked: {request.instrument_id} ref={reference}")
            return
        self._forward(message)

    def _cancel(self, message: Message) -> None:
        if self.active:
            self._forward(message)

    def _modify(self, message: Message) -> None:
        if not self.active:
            return
        if message.data.instrument_id in self._muted_symbols:
            self._log(f"[OrderRouter] modify blocked: {message.data.instrument_id}")
            return
        self._forward(message)

    def _forward(self, message: Message) -> None:
        self.event_engine.put(
            Message(
                MessageKind.COMMAND,
                message.name,
                message.data,
                source=self.target,
                target=self.execution_target,
                correlation_id=message.message_id,
            )
        )

    def _log(self, text: str) -> None:
        self.event_engine.put(
            Event(EVENT_LOG, LogData(text, LogLevel.INFO))
        )


__all__ = ["OrderRouter"]
