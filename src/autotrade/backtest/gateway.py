"""Simulated broker facade and all of its private simulation components."""

from __future__ import annotations

import math
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from autotrade.backtest.event_engine import BacktestEventEngine
from autotrade.backtest.reporting import BacktestRecorder
from autotrade.coreutils.constant import Direction, LogLevel, OrderStatus, OrderType
from autotrade.coreutils.object import (
    AccountData,
    CancelRequest,
    LogData,
    ModifyRequest,
    OrderData,
    OrderRequest,
    PositionData,
    Request,
    RequestState,
    RequestStatus,
    RequestType,
    Slice,
    TimeSlice,
    TradeBar,
    TradeData,
    ValuationUpdate,
)
from autotrade.engine.event_engine import (
    COMMAND_ACCOUNT_VALUATION,
    COMMAND_MARKET_AFTER,
    COMMAND_MARKET_BEFORE,
    COMMAND_ORDER_CANCEL,
    COMMAND_ORDER_MODIFY,
    COMMAND_ORDER_SUBMIT,
    EVENT_ACCOUNT,
    EVENT_LOG,
    EVENT_ORDER,
    EVENT_POSITION_SNAPSHOT,
    EVENT_REQUEST,
    EVENT_REQUEST_STATUS,
    EVENT_TRADE,
    Event,
    Message,
)
from autotrade.engine.security_manager import SecurityManager

if TYPE_CHECKING:
    from autotrade.coreutils.object import Security


class CommissionModel:
    """Calculate commission from a fill and current Security settings."""

    def calculate(self, *, direction, price, volume, security) -> float:
        if security is None:
            return 0.0
        rate = (
            security.long_commission_rate
            if direction == Direction.LONG
            else security.short_commission_rate
        )
        if rate is None:
            rate = security.commission_rate
        turnover = abs(float(volume)) * float(price) * float(security.multiplier)
        return turnover * float(rate)


class MarginModel:
    """Calculate margin without owning account state."""

    def calculate(self, *, position, mark_price, security) -> float:
        if security is None:
            return 0.0
        return (
            abs(float(position.volume))
            * float(mark_price)
            * float(security.multiplier)
            * float(security.margin_rate)
        )


@dataclass(slots=True)
class BacktestEventPublisher:
    """Publish isolated simulated-broker results through the shared protocol."""

    event_engine: BacktestEventEngine

    def order(self, data: OrderData) -> None:
        self.event_engine.put(Event(EVENT_ORDER, deepcopy(data)))

    def trade(self, data: TradeData) -> None:
        self.event_engine.put(Event(EVENT_TRADE, deepcopy(data)))

    def position_snapshot(self, data: PositionData) -> None:
        self.event_engine.put(Event(EVENT_POSITION_SNAPSHOT, deepcopy(data)))

    def account(self, data: AccountData) -> None:
        self.event_engine.put(Event(EVENT_ACCOUNT, deepcopy(data)))

    def request_status(self, data: RequestStatus) -> None:
        self.event_engine.put(Event(EVENT_REQUEST_STATUS, deepcopy(data)))

    def log(self, data: LogData) -> None:
        self.event_engine.put(Event(EVENT_LOG, data))


class RequestRejected(Exception):
    """Expected business rejection while processing a request."""


class SimulatedOrderBook:
    """Own pending and active order state inside the simulated broker."""

    def __init__(self) -> None:
        self.active_orders: dict[str, dict[str, OrderData]] = {}
        self.pending_orders: dict[str, dict[str, OrderData]] = {}

    def submit(self, order: OrderData) -> None:
        self._ensure_instrument(order.instrument_id)
        self.pending_orders[order.instrument_id][order.orderid] = order

    def cancel(self, request: CancelRequest, now: datetime) -> OrderData:
        order = self.find(request.instrument_id, request.orderid)
        if order is None:
            raise RequestRejected(f"撤单失败，订单不存在: {request.orderid}")
        if order.status in {OrderStatus.ALLTRADED, OrderStatus.ALLCANCELLED}:
            raise RequestRejected(f"撤单失败，订单不可撤销: {request.orderid}")
        self.pop(request.instrument_id, request.orderid)
        order.status = OrderStatus.ALLCANCELLED
        order.datetime = now
        return order

    def modify(self, request: ModifyRequest, now: datetime) -> OrderData:
        order = self.find(request.instrument_id, request.orderid)
        if order is None:
            raise RequestRejected(f"修改失败，订单不存在: {request.orderid}")
        if order.status in {
            OrderStatus.ALLTRADED,
            OrderStatus.ALLCANCELLED,
            OrderStatus.PARTCANCELLED,
        }:
            raise RequestRejected(f"修改失败，订单不可修改: {request.orderid}")
        if request.qty <= order.traded:
            raise RequestRejected(
                f"修改失败，新订单总量必须大于已成交数量: {request.orderid}"
            )
        self.pop(request.instrument_id, request.orderid)
        execution_status = order.status
        order.price = request.price
        order.volume = request.qty
        order.trigger_price = request.trigger_price
        order.datetime = now
        order.status = execution_status
        setattr(order, "stop_triggered", False)
        self.submit(order)
        return order

    def activate(self, now: datetime, *, include_current: bool = False):
        activated = []
        for instrument_id, orders in list(self.pending_orders.items()):
            self._ensure_instrument(instrument_id)
            for orderid, order in list(orders.items()):
                should_activate = (
                    order.datetime is None
                    or order.datetime < now
                    or (include_current and order.datetime <= now)
                )
                if not should_activate:
                    continue
                order.status = (
                    OrderStatus.PARTTRADED
                    if float(order.traded) > 0
                    else OrderStatus.NOTTRADED
                )
                if include_current:
                    order.datetime = now
                self.active_orders[instrument_id][orderid] = order
                del orders[orderid]
                activated.append(order)
        return activated

    def iter_active(self):
        for orders in self.active_orders.values():
            yield from list(orders.values())

    def remove_active(self, order: OrderData) -> None:
        self.active_orders.get(order.instrument_id, {}).pop(order.orderid, None)

    def find(self, instrument_id: str, orderid: str) -> OrderData | None:
        return (
            self.active_orders.get(instrument_id, {}).get(orderid)
            or self.pending_orders.get(instrument_id, {}).get(orderid)
        )

    def pop(self, instrument_id: str, orderid: str) -> OrderData | None:
        for pool in (self.active_orders, self.pending_orders):
            orders = pool.get(instrument_id, {})
            if orderid in orders:
                return orders.pop(orderid)
        return None

    def get_active(self) -> list[OrderData]:
        return list(self.iter_active())

    def _ensure_instrument(self, instrument_id: str) -> None:
        self.active_orders.setdefault(instrument_id, {})
        self.pending_orders.setdefault(instrument_id, {})


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
    instrument_id: str
    price: float
    quantity: float
    time: datetime


class FillModel:
    """Replaceable simulated-exchange fill policy."""

    def fill(self, parameters: FillContext) -> Fill | None:
        handlers = {
            OrderType.MARKET: self.market_fill,
            OrderType.LIMIT: self.limit_fill,
            OrderType.STP_MKT: self.stop_market_fill,
            OrderType.STP_LMT: self.stop_limit_fill,
        }
        handler = handlers.get(parameters.order.type)
        if handler is None:
            raise UnsupportedOrderType(parameters.order.type)
        return handler(parameters)

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
        if (
            triggered_this_bar
            and parameters.settings.stop_limit_same_bar == "conservative"
        ):
            return None
        return self.limit_fill(parameters)


class MatchingEngine:
    """Calculate fills without mutating broker, OMS, or account state."""

    def __init__(self, *, fill_model, settings, security_manager) -> None:
        self.fill_model = fill_model
        self.settings = settings
        self.security_manager = security_manager

    def match(self, time_slice, orders, *, same_time_only: bool = False):
        slice_ = time_slice.slice
        data_name = self.settings.execution_data_name
        if data_name is not None and not slice_.bars.get(data_name):
            return []
        if data_name is None and not slice_.bars:
            return []
        matches = []
        for order in orders:
            if same_time_only and order.datetime != time_slice.time:
                continue
            security = self.security_manager.add(order.instrument_id)
            fill = self.fill_model.fill(
                FillContext(
                    time=slice_.time,
                    order=order,
                    security=security,
                    slice=slice_,
                    securities=self.security_manager,
                    settings=self.settings,
                )
            )
            if fill is not None:
                matches.append((order, fill))
        return matches


class UnsupportedOrderType(Exception):
    def __init__(self, order_type: OrderType):
        super().__init__(f"Unsupported order type: {order_type}")


def _current_trade_bar(parameters: FillContext) -> TradeBar | None:
    return parameters.slice.get_bar(
        parameters.order.instrument_id,
        data_name=parameters.settings.execution_data_name,
    )


def _full_fill(order: OrderData, price: float, time: datetime) -> Fill | None:
    quantity = max(float(order.volume) - float(order.traded), 0.0)
    if quantity <= 0:
        return None
    return Fill(order.orderid, order.instrument_id, price, quantity, time)


class AccountLedger:
    """Authoritative simulated-broker cash, position, margin and PnL ledger."""

    account_id = "BACKTEST"

    def __init__(
        self,
        *,
        initial_cash: float,
        security_manager: SecurityManager,
        commission_model: CommissionModel | None = None,
        margin_model: MarginModel | None = None,
    ) -> None:
        self.security_manager = security_manager
        self.commission_model = commission_model or CommissionModel()
        self.margin_model = margin_model or MarginModel()
        self.positions: dict[str, PositionData] = {}
        self.mark_prices: dict[str, float] = {}
        self.unrealized_pnl_by_symbol: dict[str, float] = {}
        self.realized_pnl_by_symbol: dict[str, float] = {}
        self.turnover_by_symbol: dict[str, float] = {}
        self.commission_by_symbol: dict[str, float] = {}
        self.account = AccountData(
            accountid=self.account_id,
            gateway_name="BACKTEST",
        )
        self.account.cash = float(initial_cash)
        self.account.available = float(initial_cash)
        self.account.equity = float(initial_cash)

    def apply_trade(self, trade: TradeData) -> PositionData:
        previous = deepcopy(self.positions.get(trade.instrument_id))
        position = self._project_position(trade)
        security = self.security_manager.get(trade.instrument_id)
        multiplier = float(security.multiplier if security is not None else 1)
        turnover = abs(float(trade.volume)) * float(trade.price) * multiplier
        commission = self.commission_model.calculate(
            direction=trade.direction,
            price=trade.price,
            volume=trade.volume,
            security=security,
        )
        old_volume = self._signed_volume(previous)
        delta = (
            abs(float(trade.volume))
            if trade.direction == Direction.LONG
            else -abs(float(trade.volume))
        )
        close_quantity = min(abs(old_volume), abs(delta)) if old_volume * delta < 0 else 0
        realized_pnl = 0.0
        if close_quantity and previous is not None:
            sign = 1 if old_volume > 0 else -1
            realized_pnl = (
                (float(trade.price) - float(previous.price))
                * close_quantity
                * multiplier
                * sign
            )
        self.account.cash += realized_pnl - commission
        self.account.realized_pnl += realized_pnl
        self.realized_pnl_by_symbol[trade.instrument_id] = (
            self.realized_pnl_by_symbol.get(trade.instrument_id, 0.0) + realized_pnl
        )
        self.turnover_by_symbol[trade.instrument_id] = (
            self.turnover_by_symbol.get(trade.instrument_id, 0.0) + turnover
        )
        self.commission_by_symbol[trade.instrument_id] = (
            self.commission_by_symbol.get(trade.instrument_id, 0.0) + commission
        )
        self.mark_prices[trade.instrument_id] = float(trade.price)
        if position.volume == 0:
            self.unrealized_pnl_by_symbol.pop(trade.instrument_id, None)
        self._refresh_position_margins()
        self._refresh_portfolio()
        return deepcopy(position)

    def mark_to_market(self, updates: tuple[ValuationUpdate, ...]) -> None:
        for update in updates:
            self.mark_prices[update.instrument_id] = float(update.price)
        self._mark_positions()
        self._refresh_position_margins()
        self._refresh_portfolio()

    def get_all_positions(self) -> list[PositionData]:
        return [deepcopy(position) for position in self.positions.values()]

    def _project_position(self, trade: TradeData) -> PositionData:
        current = self.positions.get(trade.instrument_id)
        old_volume = self._signed_volume(current)
        old_price = 0.0 if current is None else float(current.price)
        delta = abs(float(trade.volume)) if trade.direction == Direction.LONG else -abs(float(trade.volume))
        new_volume = old_volume + delta
        if old_volume == 0 or old_volume * delta > 0:
            new_price = (
                float(trade.price)
                if old_volume == 0
                else (
                    old_price * abs(old_volume) + float(trade.price) * abs(delta)
                ) / abs(new_volume)
            )
        elif new_volume == 0:
            new_price = 0.0
        elif old_volume * new_volume > 0:
            new_price = old_price
        else:
            new_price = float(trade.price)
        position = PositionData(
            gateway_name=trade.gateway_name,
            instrument_id=trade.instrument_id,
            exchange=trade.exchange,
            direction=Direction.NET,
            volume=new_volume,
            price=new_price,
            margin=0,
        )
        if new_volume == 0:
            self.positions.pop(trade.instrument_id, None)
        else:
            self.positions[trade.instrument_id] = position
        return position

    def _mark_positions(self) -> None:
        for instrument_id in set(self.unrealized_pnl_by_symbol) - set(self.positions):
            del self.unrealized_pnl_by_symbol[instrument_id]
        for instrument_id, position in self.positions.items():
            security = self.security_manager.get(instrument_id)
            multiplier = float(security.multiplier if security is not None else 1)
            mark_price = self.mark_prices.get(instrument_id, float(position.price))
            self.unrealized_pnl_by_symbol[instrument_id] = (
                (mark_price - float(position.price))
                * float(position.volume)
                * multiplier
            )

    def _refresh_position_margins(self) -> None:
        for instrument_id, position in self.positions.items():
            position.margin = self.margin_model.calculate(
                position=position,
                mark_price=self.mark_prices.get(instrument_id, float(position.price)),
                security=self.security_manager.get(instrument_id),
            )

    def _refresh_portfolio(self) -> None:
        self.account.margin = sum(float(p.margin) for p in self.positions.values())
        self.account.unrealized_pnl = sum(self.unrealized_pnl_by_symbol.values())
        self.account.equity = self.account.cash + self.account.unrealized_pnl
        self.account.available = self.account.equity - self.account.margin

    @staticmethod
    def _signed_volume(position: PositionData | None) -> float:
        if position is None:
            return 0.0
        volume = float(position.volume)
        return -volume if position.direction == Direction.SHORT and volume > 0 else volume


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
    ) -> None:
        self.gateway_name = gateway_name
        self.event_engine = event_engine
        self.security_manager = security_manager or SecurityManager()
        self.settings = settings or BacktestSettings()
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
        self.security_manager.add(order.instrument_id)
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
        if fill.order_id != order.orderid or fill.instrument_id != order.instrument_id:
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
            instrument_id=order.instrument_id,
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
        self.account_ledger.apply_trade(trade)

        self.on_order(order)
        self.on_trade(trade)
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
    "AccountLedger",
    "BacktestGateway",
    "BacktestSettings",
    "BacktestEventPublisher",
    "BarFillModel",
    "CommissionModel",
    "Fill",
    "FillContext",
    "FillModel",
    "MarginModel",
    "MatchingEngine",
    "RequestRejected",
    "SimulatedOrderBook",
    "UnsupportedOrderType",
]
