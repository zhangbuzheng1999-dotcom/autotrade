"""Simulated broker facade for backtest execution and accounting."""

from __future__ import annotations

import math
import uuid
from datetime import datetime

from autotrade.backtest.account_ledger import AccountLedger
from autotrade.backtest.backtest_event_engine import BacktestEventEngine
from autotrade.backtest.backtest_event_publisher import BacktestEventPublisher
from autotrade.backtest.backtest_order_book import (
    RequestRejected,
    SimulatedOrderBook,
)
from autotrade.backtest.backtest_recorder import BacktestRecorder
from autotrade.backtest.commission_model import CommissionModel
from autotrade.backtest.margin_model import MarginModel
from autotrade.backtest.matching_engine import (
    BacktestSettings,
    BarFillModel,
    Fill,
    FillContext,
    FillModel,
    MatchingEngine,
    UnsupportedOrderType,
)
from autotrade.coreutils.constant import LogLevel, OrderStatus
from autotrade.coreutils.object import (
    CancelRequest,
    LogData,
    ModifyRequest,
    OrderData,
    OrderRequest,
    Request,
    RequestState,
    RequestStatus,
    RequestType,
    TimeSlice,
    TradeData,
)
from autotrade.engine.event_engine import (
    COMMAND_ACCOUNT_VALUATION,
    COMMAND_MARKET_AFTER,
    COMMAND_MARKET_BEFORE,
    COMMAND_ORDER_CANCEL,
    COMMAND_ORDER_MODIFY,
    COMMAND_ORDER_SUBMIT,
    EVENT_REQUEST,
    Event,
    Message,
)
from autotrade.engine.security_manager import SecurityManager


class BacktestGateway:
    """Facade that simulates broker order, fill, position, and account reports."""

    def __init__(
        self,
        gateway_name: str,
        *,
        event_engine: BacktestEventEngine,
        fill_model: FillModel | None = None,
        settings: BacktestSettings | None = None,
        security_manager: SecurityManager | None = None,
        initial_cash: float = 1_000_000,
        commission_model: CommissionModel | None = None,
        margin_model: MarginModel | None = None,
        recorder: BacktestRecorder | None = None,
        clock=None,
    ) -> None:
        self.gateway_name = gateway_name
        self.event_engine = event_engine
        self.security_manager = security_manager or SecurityManager()
        self.settings = settings or BacktestSettings()
        self.clock = clock
        self.current_date = datetime.min

        self.publisher = BacktestEventPublisher(event_engine)
        self.order_book = SimulatedOrderBook()
        self.matching_engine = MatchingEngine(
            fill_model=fill_model or BarFillModel(),
            settings=self.settings,
            security_manager=self.security_manager,
        )
        self.account_ledger = AccountLedger(
            initial_cash=initial_cash,
            security_manager=self.security_manager,
            commission_model=commission_model,
            margin_model=margin_model,
        )
        self.recorder = recorder or BacktestRecorder()
        self.register_event()

    def publish_initial_state(self) -> None:
        """Publish broker state after all shared consumers have been installed."""
        self.publisher.account(self.account_ledger.account)

    @property
    def active_orders(self):
        """Compatibility view; order state is owned by SimulatedOrderBook."""
        return self.order_book.active_orders

    @property
    def pending_orders(self):
        return self.order_book.pending_orders

    @property
    def fill_model(self):
        return self.matching_engine.fill_model

    def register_event(self) -> None:
        self.event_engine.register(EVENT_REQUEST, self.process_request_event)
        for name, handler in self._execution_routes:
            self.event_engine.register_command("execution", name, handler)
        for name, handler in self._simulation_routes:
            self.event_engine.register_command("simulated_broker", name, handler)

    def unregister_event(self) -> None:
        self.event_engine.unregister(EVENT_REQUEST, self.process_request_event)
        for name, handler in self._execution_routes:
            self.event_engine.unregister_command("execution", name, handler)
        for name, handler in self._simulation_routes:
            self.event_engine.unregister_command("simulated_broker", name, handler)

    @property
    def _execution_routes(self):
        return (
            (COMMAND_ORDER_SUBMIT, self.process_order_command),
            (COMMAND_ORDER_CANCEL, self.process_cancel_command),
            (COMMAND_ORDER_MODIFY, self.process_modify_command),
        )

    @property
    def _simulation_routes(self):
        return (
            (COMMAND_MARKET_BEFORE, self.process_before_command),
            (COMMAND_MARKET_AFTER, self.process_after_command),
            (COMMAND_ACCOUNT_VALUATION, self.process_valuation_command),
        )

    def process_order_command(self, message: Message) -> None:
        self._process_routed_request(message, RequestType.ORDER)

    def process_cancel_command(self, message: Message) -> None:
        self._process_routed_request(message, RequestType.CANCEL)

    def process_modify_command(self, message: Message) -> None:
        self._process_routed_request(message, RequestType.MODIFY)

    def _process_routed_request(
        self,
        message: Message,
        request_type: RequestType,
    ) -> None:
        self.process_request_event(
            Event(
                EVENT_REQUEST,
                Request(
                    request_type,
                    message.data,
                    request_id=message.message_id,
                    source=message.source,
                ),
            )
        )

    def process_before_command(self, message: Message) -> None:
        self.process_before_data(message.data)

    def process_after_command(self, message: Message) -> None:
        self.process_after_data(message.data)

    def process_valuation_command(self, message: Message) -> None:
        self.process_valuation(message.data)

    def process_request_event(self, event: Event) -> None:
        request = event.data
        if not isinstance(request, Request):
            self.write_log(
                LogData(
                    "[BacktestGateway] EVENT_REQUEST data must be Request",
                    LogLevel.ERROR,
                )
            )
            return
        if request.type not in {
            RequestType.ORDER,
            RequestType.MODIFY,
            RequestType.CANCEL,
        }:
            return
        try:
            order = self._handle_request(request)
        except RequestRejected as exc:
            self._publish_request_status(request, RequestState.REJECTED, str(exc))
            self.write_log(LogData(f"[BacktestGateway] {exc}", LogLevel.ERROR))
            return
        except Exception as exc:
            self._publish_request_status(request, RequestState.FAILED, str(exc))
            self.write_log(LogData(f"[BacktestGateway] 请求处理异常: {exc}", LogLevel.ERROR))
            return

        self._publish_request_status(
            request,
            RequestState.ACCEPTED,
            resource_id=order.orderid,
        )
        self.on_order(order)

    def _handle_request(self, request: Request) -> OrderData:
        if request.type is RequestType.ORDER:
            if not isinstance(request.data, OrderRequest):
                raise RequestRejected("ORDER请求必须携带OrderRequest")
            return self.send_order(request.data)
        if request.type is RequestType.MODIFY:
            if not isinstance(request.data, ModifyRequest):
                raise RequestRejected("MODIFY请求必须携带ModifyRequest")
            return self.modify_order(request.data)
        if request.type is RequestType.CANCEL:
            if not isinstance(request.data, CancelRequest):
                raise RequestRejected("CANCEL请求必须携带CancelRequest")
            return self.cancel_order(request.data)
        raise RequestRejected(f"Gateway不支持请求类型: {request.type.value}")

    def _publish_request_status(
        self,
        request: Request,
        state: RequestState,
        message: str = "",
        *,
        resource_id: str | None = None,
    ) -> None:
        self.publisher.request_status(
            RequestStatus(
                request=request,
                state=state,
                message=message,
                resource_id=resource_id,
                created_at=self.current_date,
            )
        )

    def send_order(self, request: OrderRequest) -> OrderData:
        order = request.create_order_data(uuid.uuid4().hex, self.gateway_name)
        order.datetime = self.current_date
        order.status = OrderStatus.PENDING
        self.security_manager.add(order.symbol)
        self.order_book.submit(order)
        return order

    def cancel_order(self, request: CancelRequest) -> OrderData:
        return self.order_book.cancel(request, self.current_date)

    def modify_order(self, request: ModifyRequest) -> OrderData:
        return self.order_book.modify(request, self.current_date)

    def process_before_data(self, time_slice: TimeSlice) -> None:
        self.current_date = time_slice.time
        for order in self.order_book.activate(time_slice.time):
            self.on_order(order)
        self._match(time_slice)

    def process_after_data(self, time_slice: TimeSlice) -> None:
        self.current_date = time_slice.time
        if not self.settings.cheat_on_close:
            return
        for order in self.order_book.activate(
            time_slice.time,
            include_current=True,
        ):
            self.on_order(order)
        self._match(time_slice, same_time_only=True)

    def process_valuation(self, time_slice: TimeSlice) -> bool:
        """Mark the simulated broker account and record one settlement snapshot."""
        if not time_slice.valuation_updates:
            return False
        self.account_ledger.mark_to_market(time_slice.valuation_updates)
        for position in self.account_ledger.get_all_positions():
            self.publisher.position_snapshot(position)
        self.publisher.account(self.account_ledger.account)
        self.recorder.snapshot(
            time_slice.time,
            self.account_ledger,
            self.security_manager,
        )
        return True

    def _match(
        self,
        time_slice: TimeSlice,
        *,
        same_time_only: bool = False,
    ) -> None:
        try:
            matches = self.matching_engine.match(
                time_slice,
                self.order_book.get_active(),
                same_time_only=same_time_only,
            )
        except UnsupportedOrderType as exc:
            self.write_log(LogData(f"[BacktestGateway] {exc}", LogLevel.ERROR))
            return
        for order, fill in matches:
            self._apply_fill(order, fill)
            self.order_book.remove_active(order)

    def _apply_fill(self, order: OrderData, fill: Fill) -> None:
        remaining = float(order.volume) - float(order.traded)
        if fill.order_id != order.orderid or fill.symbol != order.symbol:
            raise ValueError("fill does not belong to order")
        if not math.isfinite(float(fill.quantity)) or fill.quantity <= 0:
            raise ValueError("fill quantity must be finite and positive")
        if abs(float(fill.quantity) - remaining) > 1e-12:
            raise ValueError(
                "partial fills are not supported yet; fill must equal remaining quantity"
            )
        if not math.isfinite(float(fill.price)) or fill.price <= 0:
            raise ValueError("fill price must be finite and positive")

        order.status = OrderStatus.ALLTRADED
        order.traded = float(order.traded) + float(fill.quantity)
        order.avgFillPrice = fill.price
        order.datetime = fill.time
        trade = TradeData(
            symbol=order.symbol,
            exchange=order.exchange,
            orderid=order.orderid,
            tradeid=uuid.uuid4().hex,
            direction=order.direction,
            offset=order.offset,
            price=fill.price,
            volume=fill.quantity,
            traded=fill.quantity,
            avgFillPrice=fill.price,
            datetime=fill.time,
            status=OrderStatus.ALLTRADED,
            gateway_name=self.gateway_name,
            reference=order.reference,
        )
        position = self.account_ledger.apply_trade(trade)

        self.on_order(order)
        self.on_trade(trade)
        self.publisher.position_snapshot(position)
        self.publisher.account(self.account_ledger.account)

    def get_orders(self) -> list[OrderData]:
        return self.order_book.get_active()

    def on_order(self, order: OrderData) -> None:
        self.publisher.order(order)

    def on_trade(self, trade: TradeData) -> None:
        self.publisher.trade(trade)

    def write_log(self, log_data: LogData) -> None:
        self.publisher.log(log_data)


__all__ = [
    "BacktestGateway",
    "BacktestSettings",
    "BarFillModel",
    "Fill",
    "FillContext",
    "FillModel",
    "RequestRejected",
    "UnsupportedOrderType",
]
