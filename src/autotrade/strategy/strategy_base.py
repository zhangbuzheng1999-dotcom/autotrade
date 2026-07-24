# -*- coding: utf-8 -*-
from __future__ import annotations
from collections import defaultdict, deque
from typing import Dict, List, Tuple, Optional
from autotrade.coreutils.constant import Interval, Exchange, Direction, OrderType, OrderStatus
from autotrade.coreutils.object import BarData, OrderData, TradeData, OrderRequest, ModifyRequest, CancelRequest, PositionData, \
    LogData, Request, RequestStatus, RequestType, Slice, Tick, TickData, TradeBar
from autotrade.engine.event_engine import (
    COMMAND_ORDER_CANCEL,
    COMMAND_ORDER_MODIFY,
    COMMAND_ORDER_SUBMIT,
    EVENT_LOG,
    EVENT_ORDER,
    EVENT_POSITION,
    EVENT_REQUEST,
    EVENT_REQUEST_STATUS,
    EVENT_SLICE,
    EVENT_TRADE,
    Event,
    EventEngine,
    Message,
    MessageKind,
)

# ===================== 常量/工具 =====================

EVENT_RECONCILE = "eReconcile"
ENTRY_REFS = {"entry"}
STOP_REF = "stop_order"

SUBMIT_LIKE = {OrderStatus.SUBMITTING, OrderStatus.PENDING, OrderStatus.MODIFIED}
CANCEL_LIKE = {OrderStatus.ALLCANCELLED, OrderStatus.PARTCANCELLED}
FILLED_LIKE = {OrderStatus.ALLTRADED, OrderStatus.PARTTRADED}


class TargetOrder:
    __slots__ = ("reference", "symbol", "direction", "price", "trigger_price", "volume", "type")

    def __init__(self, reference, symbol, direction, price, trigger_price, volume, type_):
        self.reference = reference
        self.symbol = symbol
        self.direction = direction
        self.price = price
        self.trigger_price = trigger_price
        self.volume = int(volume)
        self.type = type_


# ===================== 策略 =====================

class StrategyBase:
    """
    专业版（vn.py 事件引擎统一对齐）：
    - on_order/on_trade/on_15m/on_2h/on_tick 只“置脏并投递 EVENT_RECONCILE”
    - 统一在 _on_reconcile 里 build_plan + execute（串行、安全、可排空合并）
    - 含：in-flight cancel 幂等、版本/幂等键、按键限频、tick 幂等判断、seed-stop 瞬间保护
    """

    def __init__(
            self,
            event_engine: EventEngine,
            security_manager=None,
    ):

        self.me = event_engine
        self.security_manager = security_manager

        self.exchange = Exchange.HKFE

        # —— 专业必备 ——
        self._canceling: set[str] = set()  # in-flight cancel 幂等
        self._realign_pending: bool = False  # 置脏标志
        self._reconciling: bool = False  # 防重入
        self._last_action_at = defaultdict(float)  # 按 (symbol, ref) 限频

        # —— 事件注册：统一对齐入口 ——
    def initialize(self):
        self.register_event()

    def unregister_event(self):
        self.me.unregister(EVENT_ORDER, self.process_order_event)
        self.me.unregister(EVENT_TRADE, self.process_trade_event)
        self.me.unregister(EVENT_POSITION, self.process_position_event)
        self.me.unregister(EVENT_SLICE, self.process_data_event)
        self.me.unregister(
            EVENT_REQUEST_STATUS,
            self.process_request_status_event,
        )

    def start(self):
        self.initialize()

    def stop(self):
        self.unregister_event()

    # ===================== Engine 直接回调：只入队 =====================
    def register_event(self):
        """注册事件监听"""
        self.me.register(EVENT_ORDER, self.process_order_event)
        self.me.register(EVENT_TRADE, self.process_trade_event)
        self.me.register(EVENT_POSITION, self.process_position_event)
        self.me.register(EVENT_SLICE, self.process_data_event)
        self.me.register(EVENT_REQUEST_STATUS, self.process_request_status_event)

    def process_order_event(self, event: Event):
        order: OrderData = event.data
        self.on_order(order)

    def process_position_event(self, event: Event):
        position: PositionData = event.data
        self.on_position(position)

    def process_trade_event(self, event: Event):
        trade: TradeData = event.data
        self.on_trade(trade)

    def process_tick_event(self, event: Event):
        tick: TickData = event.data
        self.on_tick(tick)

    def process_bar_event(self, event: Event):
        bar: BarData = event.data
        self.on_bar(bar)

    def process_data_event(self, event: Event):
        self.on_data(event.data)

    def process_request_status_event(self, event: Event):
        self.on_request_status(event.data)

    def push_log_event(self, log_data: LogData):
        self.me.put(Event(EVENT_LOG, log_data))

    def push_order_request(self, order_req: OrderRequest):
        return self._send_order_command(COMMAND_ORDER_SUBMIT, order_req)

    def push_cancel_request(self, cancel_req: CancelRequest):
        return self._send_order_command(COMMAND_ORDER_CANCEL, cancel_req)

    def push_modify_request(self, modify_req: ModifyRequest):
        return self._send_order_command(COMMAND_ORDER_MODIFY, modify_req)

    def _send_order_command(self, name: str, data) -> str:
        message = Message(
            MessageKind.COMMAND,
            name,
            data,
            source=f"strategy.{type(self).__name__}",
            target="order_router",
        )
        self.me.put(message)
        return message.message_id

    def push_request(self, request: Request) -> str:
        self.me.put(Event(EVENT_REQUEST, request))
        return request.request_id

    # ===================== Engine 直接回调：只入队 =====================
    def on_order(self, order: OrderData):
        pass

    def on_trade(self, trade: TradeData):
        pass

    def on_position(self, position: PositionData):
        pass

    # 你现有的 on_bar 多路分发
    def on_bar(self, bar: BarData):
        pass

    def on_tick(self, tick: TickData):
        pass

    def on_data(self, slice_: Slice):
        """Dispatch one standard Slice in both live and backtest runtimes."""
        if not isinstance(slice_, Slice):
            raise TypeError(
                f"StrategyBase expects Slice, got {type(slice_).__name__}"
            )
        for data_name in getattr(slice_, "ticks", {}).values():
            for ticks in data_name.values():
                for tick in ticks:
                    self.on_tick(tick)
        for bar in getattr(slice_, "bar_list", []):
            self.on_bar(bar)

    def on_request_status(self, status: RequestStatus):
        pass

    @property
    def securities(self):
        return self.security_manager

    # ===================== 统一对齐入口（事件线程串行） =====================

    def _request_realign(self):
        if not self._realign_pending:
            self._realign_pending = True
            self._on_reconcile(Event(EVENT_RECONCILE))

    def _on_reconcile(self, _ev: Event):
        if self._reconciling:
            return
        self._reconciling = True
        try:
            while True:
                if not self._realign_pending:
                    break
                self._realign_pending = False

                plan = self._build_plan()
                self._execute(plan)
        finally:
            self._reconciling = False

    # ===================== 计划生成 =====================
    def _build_plan(self):
        plan: List[Tuple[str, OrderRequest | ModifyRequest | CancelRequest]] = []

        desired_entry = self._compute_desired_entry()

        # entry place/modify
        for desire_order in desired_entry.items():
            pass

        return plan

    # —— 目标：entry ——
    def _compute_desired_entry(self):
        targets: Dict[str, TargetOrder] = {}
        return targets

    # ===================== clOrdId / Request 构造 =====================
    def _mk_place_req(self, tgt: TargetOrder) -> OrderRequest:
        return OrderRequest(
            symbol=tgt.symbol, exchange=self.exchange,
            direction=tgt.direction, type=tgt.type,
            price=tgt.price, volume=abs(tgt.volume), trigger_price=tgt.trigger_price,
            reference=tgt.reference
        )

    def _mk_modify_req(self, live: OrderData, tgt: TargetOrder, ) -> ModifyRequest:
        return ModifyRequest(
            orderid=live.orderid, symbol=tgt.symbol, exchange=self.exchange,
            qty=abs(tgt.volume),
            price=tgt.price, trigger_price=tgt.price
        )

    def _mk_cancel_req(self, live: OrderData) -> CancelRequest:
        return CancelRequest(orderid=live.orderid, symbol=self.vt_symbol, exchange=self.exchange)

    # ===================== 执行（按键限频 + 赛跑幂等） =====================
    def _execute(self, plan: List[Tuple[str, ModifyRequest | OrderRequest | CancelRequest]]):
        for act, req in plan:
            if act == "place":
                self.push_order_request(req)
            elif act == "modify":
                self.push_modify_request(req)
            elif act == "cancel":
                self.push_cancel_request(req)

    def write_log(self, log_data: LogData):
        self.me.put(Event(EVENT_LOG, log_data))
