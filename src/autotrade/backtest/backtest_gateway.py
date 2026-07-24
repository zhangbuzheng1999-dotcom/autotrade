"""Slice-aware gateway with FillModel-based matching."""

from __future__ import annotations

import math
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from autotrade.coreutils.constant import Direction, LogLevel, OrderStatus, OrderType
from autotrade.coreutils.object import (
    CancelRequest,
    LogData,
    ModifyRequest,
    OrderData,
    OrderRequest,
    TradeData,
)

from autotrade.backtest.security_manager import SecurityManager
from autotrade.coreutils.object import (
    Request,
    RequestState,
    RequestStatus,
    RequestType,
    Slice,
    TimeSlice,
    TradeBar,
)
from autotrade.backtest.backtest_event_engine import (
    EVENT_LOG,
    EVENT_ORDER,
    EVENT_REQUEST,
    EVENT_REQUEST_STATUS,
    EVENT_TRADE,
    Event,
    BacktestEventEngine,
)

if TYPE_CHECKING:
    from autotrade.coreutils.object import Security


@dataclass(slots=True)
class BacktestSettings:
    cheat_on_close: bool = False
    market_fill_price: str = "next_open"
    stop_limit_same_bar: str = "conservative"
    execution_data_name: str | None = None


@dataclass(slots=True)
class FillContext:
    time: datetime
    order: OrderData
    security: "Security"
    slice: Slice
    securities: SecurityManager
    settings: BacktestSettings


@dataclass(frozen=True, slots=True)
class Fill:
    order_id: str
    symbol: str
    price: float
    quantity: float
    time: datetime


@dataclass(slots=True)
class _GatewayEventPublisher:
    event_engine: BacktestEventEngine

    def on_order(self, order: OrderData) -> None:
        self.event_engine.put(Event(EVENT_ORDER, deepcopy(order)))

    def on_trade(self, trade) -> None:
        self.event_engine.put(Event(EVENT_TRADE, deepcopy(trade)))

    def push_log_event(self, log_data: LogData) -> None:
        self.event_engine.put(Event(EVENT_LOG, log_data))

    def on_request_status(self, status: RequestStatus) -> None:
        self.event_engine.put(Event(EVENT_REQUEST_STATUS, status))


class FillModel:
    """Return at most one full fill for the order's remaining quantity."""

    def fill(self, parameters: FillContext) -> Fill | None:
        order = parameters.order
        if order.type == OrderType.MARKET:
            return self.market_fill(parameters)
        if order.type == OrderType.LIMIT:
            return self.limit_fill(parameters)
        if order.type == OrderType.STP_MKT:
            return self.stop_market_fill(parameters)
        if order.type == OrderType.STP_LMT:
            return self.stop_limit_fill(parameters)
        raise UnsupportedOrderType(order.type)

    def market_fill(self, parameters: FillContext) -> Fill | None:
        return None

    def limit_fill(self, parameters: FillContext) -> Fill | None:
        return None

    def stop_market_fill(self, parameters: FillContext) -> Fill | None:
        return None

    def stop_limit_fill(self, parameters: FillContext) -> Fill | None:
        return None


class BarFillModel(FillModel):
    """Conservative bar-only fill model."""

    def market_fill(self, parameters: FillContext) -> Fill | None:
        order = parameters.order
        bar = _current_trade_bar(parameters)
        if bar is None:
            return None

        same_time = order.datetime == parameters.time
        if same_time and not parameters.settings.cheat_on_close:
            return None

        if same_time and parameters.settings.cheat_on_close:
            price = bar.close
        elif parameters.settings.market_fill_price == "next_close":
            price = bar.close
        else:
            price = bar.open

        return _full_fill(order, price, parameters.time)

    def limit_fill(self, parameters: FillContext) -> Fill | None:
        order = parameters.order
        bar = _current_trade_bar(parameters)
        if bar is None or order.datetime == parameters.time:
            return None
        if order.direction == Direction.LONG and bar.low <= order.price:
            return _full_fill(order, min(bar.open, order.price), parameters.time)
        if order.direction == Direction.SHORT and bar.high >= order.price:
            return _full_fill(order, max(bar.open, order.price), parameters.time)
        return None

    def stop_market_fill(self, parameters: FillContext) -> Fill | None:
        order = parameters.order
        bar = _current_trade_bar(parameters)
        if bar is None or order.datetime == parameters.time:
            return None
        if order.direction == Direction.LONG and bar.high >= order.trigger_price:
            return _full_fill(order, max(order.trigger_price, bar.open), parameters.time)
        if order.direction == Direction.SHORT and bar.low <= order.trigger_price:
            return _full_fill(order, min(order.trigger_price, bar.open), parameters.time)
        return None

    def stop_limit_fill(self, parameters: FillContext) -> Fill | None:
        order = parameters.order
        bar = _current_trade_bar(parameters)
        if bar is None or order.datetime == parameters.time:
            return None

        stop_triggered = bool(getattr(order, "stop_triggered", False))
        triggered_this_bar = False

        if not stop_triggered:
            if order.direction == Direction.LONG and bar.high >= order.trigger_price:
                stop_triggered = True
                triggered_this_bar = True
            elif order.direction == Direction.SHORT and bar.low <= order.trigger_price:
                stop_triggered = True
                triggered_this_bar = True

        if not stop_triggered:
            return None

        setattr(order, "stop_triggered", True)

        if triggered_this_bar and parameters.settings.stop_limit_same_bar == "conservative":
            return None

        return self.limit_fill(parameters)


def _current_trade_bar(parameters: FillContext) -> TradeBar | None:
    return parameters.slice.get_bar(
        parameters.order.symbol,
        data_name=parameters.settings.execution_data_name,
    )


def _full_fill(order: OrderData, price: float, time: datetime) -> Fill | None:
    quantity = max(float(order.volume) - float(order.traded), 0.0)
    if quantity <= 0:
        return None
    return Fill(
        order_id=order.orderid,
        symbol=order.symbol,
        price=price,
        quantity=quantity,
        time=time,
    )


class UnsupportedOrderType(Exception):
    def __init__(self, order_type: OrderType):
        super().__init__(f"Unsupported order type: {order_type}")


class BacktestGateway:
    """Gateway that keeps order state and delegates matching to FillModel."""

    def __init__(
        self,
        gateway_name: str,
        *,
        event_engine: BacktestEventEngine,
        fill_model: FillModel | None = None,
        settings: BacktestSettings | None = None,
        security_manager: SecurityManager | None = None,
    ):
        self.gateway_name = gateway_name
        self.publisher = _GatewayEventPublisher(event_engine)
        self.active_orders: dict[str, dict[str, OrderData]] = {}
        self.pending_orders: dict[str, dict[str, OrderData]] = {}
        self.current_date = datetime.today()
        self.event_engine = event_engine
        self.fill_model = fill_model or BarFillModel()
        self.settings = settings or BacktestSettings()
        self.security_manager = security_manager or SecurityManager()
        self.register_event()

    def register_event(self) -> None:
        self.event_engine.register(EVENT_REQUEST, self.process_request_event)

    def process_request_event(self, event: Event) -> None:
        request = event.data
        if not isinstance(request, Request):
            self.write_log(LogData("[BacktestGateway] EVENT_REQUEST data must be Request", LogLevel.ERROR))
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
        self.publisher.on_request_status(
            RequestStatus(
                request=request,
                state=state,
                message=message,
                resource_id=resource_id,
                created_at=self.current_date,
            )
        )

    def send_order(self, req: OrderRequest) -> OrderData:
        orderid = uuid.uuid4().hex
        order = req.create_order_data(orderid, self.gateway_name)
        order.datetime = self.current_date
        order.status = OrderStatus.PENDING

        self._ensure_symbol(order.symbol)
        self.security_manager.add(order.symbol)
        self.pending_orders[order.symbol][orderid] = order
        return order

    def cancel_order(self, req: CancelRequest) -> OrderData:
        order = self._find_order(req.symbol, req.orderid)
        if order is None:
            raise RequestRejected(f"撤单失败，订单不存在: {req.orderid}")

        if order.status in {OrderStatus.ALLTRADED, OrderStatus.ALLCANCELLED}:
            raise RequestRejected(f"撤单失败，订单不可撤销: {req.orderid}")

        self._pop_order(req.symbol, req.orderid)
        order.status = OrderStatus.ALLCANCELLED
        order.datetime = self.current_date
        return order

    def modify_order(self, req: ModifyRequest) -> OrderData:
        order = self._find_order(req.symbol, req.orderid)
        if order is None:
            raise RequestRejected(f"修改失败，订单不存在: {req.orderid}")

        if order.status in {OrderStatus.ALLTRADED, OrderStatus.ALLCANCELLED, OrderStatus.PARTCANCELLED}:
            raise RequestRejected(f"修改失败，订单不可修改: {req.orderid}")

        if req.qty <= order.traded:
            raise RequestRejected(f"修改失败，新订单总量必须大于已成交数量: {req.orderid}")

        self._pop_order(req.symbol, req.orderid)
        execution_status = order.status
        order.price = req.price
        order.volume = req.qty
        order.trigger_price = req.trigger_price
        order.datetime = self.current_date
        order.status = execution_status
        setattr(order, "stop_triggered", False)
        self.pending_orders[order.symbol][order.orderid] = order
        return order

    def process_before_data(self, time_slice: TimeSlice) -> None:
        """Process resting orders before strategy sees the current Slice."""
        slice_ = time_slice.slice
        self.current_date = time_slice.time
        self._activate_pending_orders(time_slice.time)
        self._scan_active_orders(time_slice)

    def process_after_data(self, time_slice: TimeSlice) -> None:
        """Process current-Slice requests after strategy code returns."""
        self.current_date = time_slice.time
        if not self.settings.cheat_on_close:
            return

        self._activate_pending_orders(time_slice.time, include_current=True)
        self._scan_active_orders(time_slice, same_time_only=True)

    def _activate_pending_orders(self, now: datetime, *, include_current: bool = False) -> None:
        for symbol, orders in list(self.pending_orders.items()):
            self._ensure_symbol(symbol)
            for orderid, order in list(orders.items()):
                if order.datetime is None:
                    should_activate = True
                elif include_current:
                    should_activate = order.datetime <= now
                else:
                    should_activate = order.datetime < now

                if not should_activate:
                    continue

                order.status = (
                    OrderStatus.PARTTRADED if float(order.traded) > 0 else OrderStatus.NOTTRADED
                )
                order.datetime = now if include_current else order.datetime
                self.active_orders[symbol][orderid] = order
                del orders[orderid]
                self.on_order(order)

    def _scan_active_orders(self, time_slice: TimeSlice, *, same_time_only: bool = False) -> None:
        slice_ = time_slice.slice
        execution_data_name = self.settings.execution_data_name
        if execution_data_name is not None and not slice_.bars.get(execution_data_name):
            return
        if execution_data_name is None and not slice_.bars:
            return

        for symbol, orders in list(self.active_orders.items()):
            security = self.security_manager.add(symbol)
            for orderid, order in list(orders.items()):
                if same_time_only and order.datetime != time_slice.time:
                    continue
                if order.status in {OrderStatus.ALLTRADED, OrderStatus.ALLCANCELLED, OrderStatus.REJECTED}:
                    del orders[orderid]
                    continue

                try:
                    fill = self.fill_model.fill(self._fill_parameters(order, security, slice_))
                except UnsupportedOrderType as exc:
                    order.status = OrderStatus.REJECTED
                    order.datetime = time_slice.time
                    self.on_order(order)
                    del orders[orderid]
                    self.write_log(LogData(f"[BacktestGateway] {exc}", LogLevel.ERROR))
                    continue
                if fill is None:
                    if order.type == OrderType.STP_LMT and getattr(order, "stop_triggered", False):
                        self.on_order(order)
                    continue

                self._apply_fill(order, fill)
                if orderid in orders:
                    del orders[orderid]

    def _fill_parameters(self, order: OrderData, security, slice_: Slice) -> FillContext:
        return FillContext(
            time=slice_.time,
            order=order,
            security=security,
            slice=slice_,
            securities=self.security_manager,
            settings=self.settings,
        )

    def _ensure_symbol(self, symbol: str) -> None:
        self.active_orders.setdefault(symbol, {})
        self.pending_orders.setdefault(symbol, {})

    def _find_order(self, symbol: str, orderid: str) -> OrderData | None:
        return (
            self.active_orders.get(symbol, {}).get(orderid)
            or self.pending_orders.get(symbol, {}).get(orderid)
        )

    def _pop_order(self, symbol: str, orderid: str) -> OrderData | None:
        for pool in (self.active_orders, self.pending_orders):
            orders = pool.get(symbol, {})
            if orderid in orders:
                return orders.pop(orderid)
        return None

    def _apply_fill(self, order: OrderData, fill: Fill) -> None:
        """Apply one full fill while preserving a future partial-fill boundary."""
        remaining = float(order.volume) - float(order.traded)
        if fill.order_id != order.orderid or fill.symbol != order.symbol:
            raise ValueError("fill does not belong to order")
        if not math.isfinite(float(fill.quantity)) or fill.quantity <= 0:
            raise ValueError("fill quantity must be finite and positive")
        if abs(float(fill.quantity) - remaining) > 1e-12:
            raise ValueError("partial fills are not supported yet; fill must equal remaining quantity")
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
        self.on_trade(trade)
        self.on_order(order)

    def get_orders(self) -> list[OrderData]:
        return [order for orders in self.active_orders.values() for order in orders.values()]

    def on_order(self, order: OrderData) -> None:
        self.publisher.on_order(order)

    def on_trade(self, trade: TradeData) -> None:
        self.publisher.on_trade(trade)

    def write_log(self, log_data: LogData) -> None:
        self.publisher.push_log_event(log_data)


class RequestRejected(Exception):
    """Expected business rejection while processing a request."""
