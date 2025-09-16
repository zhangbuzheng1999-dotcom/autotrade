# -*- coding: utf-8 -*-
from __future__ import annotations
from collections import defaultdict, deque
from typing import Dict, List, Tuple, Optional
from backtest.backtest_event_engine import Event, BacktestEventEngine
from coreutils.constant import Interval, Exchange, Direction, OrderType, OrderStatus
from coreutils.object import BarData, OrderData, TradeData, OrderRequest, ModifyRequest, CancelRequest, PositionData, \
    LogData, TickData
from engine.event_engine import EventEngine, EVENT_TICK, EVENT_ORDER, EVENT_TRADE, EVENT_BAR, EVENT_LOG, \
    EVENT_ORDER_REQ, EVENT_CANCEL_REQ, EVENT_MODIFY_REQ, EVENT_POSITION
from backtest.backtest_event_engine import BacktestEventEngine

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

    def __init__(self, event_engine: EventEngine | BacktestEventEngine):

        self.me = event_engine

        self.ee = BacktestEventEngine()

        self.exchange = Exchange.HKFE

        # —— 专业必备 ——
        self._canceling: set[str] = set()  # in-flight cancel 幂等
        self._realign_pending: bool = False  # 置脏标志
        self._reconciling: bool = False  # 防重入
        self._last_action_at = defaultdict(float)  # 按 (symbol, ref) 限频

        # —— 事件注册：统一对齐入口 ——
        self.ee.register(EVENT_RECONCILE, self._on_reconcile)
        # （如需要，也可注册 EVENT_ORDER/EVENT_TRADE 到 on_order/on_trade）
        self.ee.start()

    def initialize(self):
        self.register_event()

    # ===================== Engine 直接回调：只入队 =====================
    def register_event(self):
        """注册事件监听"""
        self.me.register(EVENT_TICK, self.process_tick_event)
        self.me.register(EVENT_ORDER, self.process_order_event)
        self.me.register(EVENT_TRADE, self.process_trade_event)
        self.me.register(EVENT_POSITION, self.process_position_event)
        self.me.register(EVENT_BAR, self.process_bar_event)

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

    def push_log_event(self, log_data: LogData):
        self.me.put(Event(EVENT_LOG, log_data))

    def push_order_request(self, order_req: OrderRequest):
        self.me.put(Event(EVENT_ORDER_REQ, order_req))

    def push_cancel_request(self, cancel_req: CancelRequest):
        self.me.put(Event(EVENT_CANCEL_REQ, cancel_req))

    def push_modify_request(self, modify_req: ModifyRequest):
        self.me.put(Event(EVENT_MODIFY_REQ, modify_req))

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

    # ===================== 统一对齐入口（事件线程串行） =====================

    def _request_realign(self):
        if not self._realign_pending:
            self._realign_pending = True
            self.ee.put(Event(EVENT_RECONCILE))

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

    # ===================== 执行（按键限频 + 赛跑幂等） =====================

    def _execute(self, plan: List[Tuple[str, ModifyRequest | OrderRequest | CancelRequest]]):
        for act, req in plan:
            if act == "place":
                self.me.put(Event(EVENT_ORDER_REQ, req))
            elif act == "modify":
                self.me.put(Event(EVENT_MODIFY_REQ, req))
            elif act == "cancel":
                self.me.put(Event(EVENT_CANCEL_REQ, req))

    def write_log(self, log_data: LogData):
        self.me.put(Event(EVENT_LOG, log_data))
