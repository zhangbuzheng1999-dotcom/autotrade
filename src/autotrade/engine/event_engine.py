"""
Event-driven framework of VeighNa framework.
"""

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from queue import Empty, Queue
from threading import RLock, Thread
from time import sleep, time_ns
from typing import Any
from uuid import uuid4

EVENT_TIMER = "eTimer"
EVENT_TICK = "eTick."  # compatibility only; market data uses EVENT_DATA
EVENT_BAR = "eBar."  # compatibility only; market data uses EVENT_DATA
EVENT_TRADE = "eTrade."
EVENT_ORDER = "eOrder."
EVENT_POSITION = "ePosition."
EVENT_ACCOUNT = "eAccount."
EVENT_CONTRACT = "eContract."  # compatibility only; instrument data uses EVENT_DATA
EVENT_QUOTE = "eQuote."
EVENT_LOG = "eLog"
EVENT_DATA = "event_data"  # one market-data or instrument-state update
EVENT_LIVE_DATA = "event_live_data"  # raw live input before TimeSlice construction
EVENT_SLICE = "event_slice"  # strategy-visible live data or synchronized Slice
EVENT_REQUEST = "event_request"  # data: Request
EVENT_REQUEST_STATUS = "event_request_status"  # data: RequestStatus
EVENT_POSITION_SNAPSHOT = "event_position_snapshot"  # broker reconciliation input
EVENT_ORDER_REQ = "evt.order.req"
EVENT_CANCEL_REQ = "evt.cancel.req"
EVENT_MODIFY_REQ = "evt.modify.req"
EVENT_ROLLOVER = "evt.rollover"
EVENT_COMMAND = "cta_command"

COMMAND_ORDER_SUBMIT = "order.submit"
COMMAND_ORDER_CANCEL = "order.cancel"
COMMAND_ORDER_MODIFY = "order.modify"
COMMAND_MARKET_BEFORE = "market.before"
COMMAND_MARKET_AFTER = "market.after"
COMMAND_ACCOUNT_VALUATION = "account.valuation"

class Event:
    """
    Event object consists of a type string which is used
    by event engine for distributing event, and a data
    object which contains the real data.
    """

    def __init__(self, type: str, data: Any = None) -> None:
        """"""
        self.type: str = type
        self.data: Any = data


class MessageKind(str, Enum):
    COMMAND = "command"
    EVENT = "event"
    RESPONSE = "response"


@dataclass(slots=True)
class Message:
    """Routed message used by live and backtest engines."""

    kind: MessageKind
    name: str
    data: Any
    source: str
    target: str | None = None
    message_id: str | None = None
    correlation_id: str | None = None
    created_at_ns: int | None = None

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("message name must not be empty")
        if not self.source:
            raise ValueError("message source must not be empty")
        if self.kind in {MessageKind.COMMAND, MessageKind.RESPONSE} and not self.target:
            raise ValueError(f"{self.kind.value} message requires a target")
        if self.kind is MessageKind.COMMAND:
            self.message_id = self.message_id or uuid4().hex
            self.created_at_ns = self.created_at_ns or time_ns()
        if self.kind is MessageKind.RESPONSE and not self.correlation_id:
            raise ValueError("response message requires a correlation_id")

    def response(self, *, source: str, data: Any, name: str | None = None) -> "Message":
        if self.kind is not MessageKind.COMMAND:
            raise ValueError("only a command can be answered")
        return Message(
            kind=MessageKind.RESPONSE,
            name=name or self.name,
            data=data,
            source=source,
            target=self.source,
            correlation_id=self.message_id,
        )


class DuplicateHandlerError(ValueError):
    pass


class RouteNotFoundError(LookupError):
    pass


# Defines handler function to be used in event engine.
HandlerType = Callable[[Event], None]


class EventEngine:
    """
    Event engine distributes event object based on its type
    to those handlers registered.

    It also generates timer event by every interval seconds,
    which can be used for timing purpose.
    """

    def __init__(self, interval: int = 1) -> None:
        """
        Timer event is generated every 1 second by default, if
        interval not specified.
        """
        self._interval: int = interval
        self._queue: Queue = Queue()
        self._active: bool = False
        self._thread: Thread = Thread(target=self._run)
        self._timer: Thread = Thread(target=self._run_timer)
        self._handlers: defaultdict = defaultdict(list)
        self._general_handlers: list = []
        self._command_handlers: dict[tuple[str, str], HandlerType] = {}
        self._message_handlers: dict[str, list[HandlerType]] = defaultdict(list)
        self._response_handlers: dict[str, HandlerType] = {}
        self._handlers_lock = RLock()

    def _run(self) -> None:
        """
        Get event from queue and then process it.
        """
        while self._active:
            try:
                event = self._queue.get(block=True, timeout=1)
                self._process(event)
            except Empty:
                pass

    def _process(self, event: Event | Message) -> None:
        """
        First distribute event to those handlers registered listening
        to this type.

        Then distribute event to those general handlers which listens
        to all types.
        """
        if isinstance(event, Message):
            self._process_message(event)
            return
        if event.type in self._handlers:
            [handler(event) for handler in self._handlers[event.type]]

        if self._general_handlers:
            [handler(event) for handler in self._general_handlers]

    def _process_message(self, message: Message) -> None:
        if message.kind is MessageKind.COMMAND:
            handler = self._command_handlers.get((message.target or "", message.name))
            if handler is None:
                raise RouteNotFoundError(
                    f"no command consumer: {message.target}/{message.name}"
                )
            handler(message)
            return
        if message.kind is MessageKind.EVENT:
            for handler in tuple(self._message_handlers.get(message.name, ())):
                handler(message)
            return
        if message.kind is MessageKind.RESPONSE:
            handler = self._response_handlers.get(message.target or "")
            if handler is None:
                raise RouteNotFoundError(f"no response consumer: {message.target}")
            handler(message)
            return
        raise ValueError(f"unsupported message kind: {message.kind!r}")

    def _run_timer(self) -> None:
        """
        Sleep by interval second(s) and then generate a timer event.
        """
        while self._active:
            sleep(self._interval)
            event: Event = Event(EVENT_TIMER)
            self.put(event)

    def start(self) -> None:
        """
        Start event engine to process events and generate timer events.
        """
        self._active = True
        self._thread.start()
        self._timer.start()

    def stop(self) -> None:
        """
        Stop event engine.
        """
        self._active = False
        self._timer.join()
        self._thread.join()

    def put(self, event: Event | Message) -> None:
        """
        Put an event object into event queue.
        """
        self._queue.put(event)

    def register(self, type: str, handler: HandlerType) -> None:
        """
        Register a new handler function for a specific event type. Every
        function can only be registered once for each event type.
        """
        handler_list: list = self._handlers[type]
        if handler not in handler_list:
            handler_list.append(handler)

    def unregister(self, type: str, handler: HandlerType) -> None:
        """
        Unregister an existing handler function from event engine.
        """
        handler_list: list = self._handlers[type]

        if handler in handler_list:
            handler_list.remove(handler)

        if not handler_list:
            self._handlers.pop(type)

    def register_general(self, handler: HandlerType) -> None:
        """
        Register a new handler function for all event types. Every
        function can only be registered once for each event type.
        """
        if handler not in self._general_handlers:
            self._general_handlers.append(handler)

    def unregister_general(self, handler: HandlerType) -> None:
        """
        Unregister an existing general handler function.
        """
        if handler in self._general_handlers:
            self._general_handlers.remove(handler)

    def register_command(self, target: str, name: str, handler: HandlerType) -> None:
        key = (target, name)
        with self._handlers_lock:
            current = self._command_handlers.get(key)
            if current is not None and current != handler:
                raise DuplicateHandlerError(
                    f"command route already has a consumer: {target}/{name}"
                )
            self._command_handlers[key] = handler

    def unregister_command(self, target: str, name: str, handler: HandlerType) -> None:
        key = (target, name)
        with self._handlers_lock:
            if self._command_handlers.get(key) == handler:
                del self._command_handlers[key]

    def subscribe(self, name: str, handler: HandlerType) -> None:
        with self._handlers_lock:
            handlers = self._message_handlers[name]
            if handler not in handlers:
                handlers.append(handler)

    def unsubscribe(self, name: str, handler: HandlerType) -> None:
        with self._handlers_lock:
            handlers = self._message_handlers.get(name, [])
            if handler in handlers:
                handlers.remove(handler)
            if not handlers:
                self._message_handlers.pop(name, None)

    def register_response(self, target: str, handler: HandlerType) -> None:
        with self._handlers_lock:
            current = self._response_handlers.get(target)
            if current is not None and current != handler:
                raise DuplicateHandlerError(
                    f"response target already has a consumer: {target}"
                )
            self._response_handlers[target] = handler

    def unregister_response(self, target: str, handler: HandlerType) -> None:
        with self._handlers_lock:
            if self._response_handlers.get(target) == handler:
                del self._response_handlers[target]



