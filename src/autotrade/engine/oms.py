"""Shared order, trade, position, account, and quote state."""

from datetime import datetime
from copy import deepcopy
from autotrade.coreutils.constant import Direction
from autotrade.engine.event_engine import Event, EventEngine
from autotrade.engine.event_engine import (
                                    EVENT_ORDER,
                                    EVENT_TRADE,
                                    EVENT_POSITION,
                                    EVENT_POSITION_SNAPSHOT,
                                    EVENT_ACCOUNT,
                                    EVENT_QUOTE
                                )
from autotrade.coreutils.object import (
    QuoteData,
    OrderData,
    TradeData,
    PositionData,
    AccountData,
)
import pandas as pd

class OmsBase:
    """
    通用 OMS 基类：
    - 维护最新行情、订单、成交、仓位、账户等状态快照。
    - 通过事件引擎接收更新。
    """

    def __init__(self, event_engine: EventEngine | None = None):
        self.event_engine = event_engine or EventEngine()

        # 交易执行和账户状态
        self.orders: dict[str, OrderData] = {}
        self.trades: dict[str, TradeData] = {}
        self.positions: dict[str, PositionData] = {}
        self.accounts: dict[str, AccountData] = {}
        self.quotes: dict[str, QuoteData] = {}

        self.active_orders: dict[str, OrderData] = {}
        self.active_quotes: dict[str, QuoteData] = {}

        # 注册事件
        self.register_event()

    def register_event(self):
        """注册事件监听"""
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        self.event_engine.register(
            EVENT_POSITION_SNAPSHOT,
            self.process_position_snapshot_event,
        )
        self.event_engine.register(EVENT_ACCOUNT, self.process_account_event)
        self.event_engine.register(EVENT_QUOTE, self.process_quote_event)

    def unregister_event(self):
        self.event_engine.unregister(EVENT_ORDER, self.process_order_event)
        self.event_engine.unregister(EVENT_TRADE, self.process_trade_event)
        self.event_engine.unregister(
            EVENT_POSITION_SNAPSHOT,
            self.process_position_snapshot_event,
        )
        self.event_engine.unregister(EVENT_ACCOUNT, self.process_account_event)
        self.event_engine.unregister(EVENT_QUOTE, self.process_quote_event)

    def start(self):
        """Lifecycle compatibility hook."""

    def stop(self):
        self.unregister_event()

    # ========== 事件处理 ==========
    def process_order_event(self, event: Event):
        order: OrderData = event.data
        self.orders[order.orderid] = order
        if order.is_active():
            self.active_orders[order.orderid] = order
        else:
            self.active_orders.pop(order.orderid, None)

    def process_trade_event(self, event: Event):
        trade: TradeData = event.data
        if trade.tradeid in self.trades:
            return
        self.trades[trade.tradeid] = trade
        previous = deepcopy(self.positions.get(trade.instrument_id))
        position = self._apply_trade_to_position(trade)
        if position is not None:
            self._after_trade_applied(trade, previous, position)
            self.event_engine.put(Event(EVENT_POSITION, deepcopy(position)))

    def _after_trade_applied(
        self,
        trade: TradeData,
        previous: PositionData | None,
        position: PositionData,
    ) -> None:
        """Subclass hook executed after the common position projection."""

    def _apply_trade_to_position(self, trade: TradeData) -> PositionData | None:
        """Project confirmed fills into one signed net position."""
        if trade.direction not in {Direction.LONG, Direction.SHORT}:
            return None

        current = self.positions.get(trade.instrument_id)
        old_volume = 0.0
        old_price = 0.0
        if current is not None:
            old_volume = float(current.volume)
            if current.direction == Direction.SHORT and old_volume > 0:
                old_volume = -old_volume
            old_price = float(current.price)

        delta = (
            abs(float(trade.volume))
            if trade.direction == Direction.LONG
            else -abs(float(trade.volume))
        )
        new_volume = old_volume + delta

        if old_volume == 0 or old_volume * delta > 0:
            new_price = (
                float(trade.price)
                if old_volume == 0
                else (
                    old_price * abs(old_volume)
                    + float(trade.price) * abs(delta)
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

    def process_position_snapshot_event(self, event: Event):
        """Accept broker snapshots only through the reconciliation input."""
        position: PositionData = deepcopy(event.data)
        if position.volume == 0:
            self.positions.pop(position.instrument_id, None)
        else:
            self.positions[position.instrument_id] = position
        self.event_engine.put(Event(EVENT_POSITION, deepcopy(position)))

    def reconcile_positions(
        self,
        positions: list[PositionData],
        *,
        replace: bool = True,
    ) -> None:
        """Reconcile startup/reconnect broker positions, then publish results."""
        if replace:
            self.positions.clear()
        for position in positions:
            self.process_position_snapshot_event(
                Event(EVENT_POSITION_SNAPSHOT, position)
            )

    def process_account_event(self, event: Event):
        account: AccountData = event.data
        self.accounts[account.accountid] = account

    def process_quote_event(self, event: Event):
        quote: QuoteData = event.data
        self.quotes[quote.quoteid] = quote
        if quote.is_active():
            self.active_quotes[quote.quoteid] = quote
        else:
            self.active_quotes.pop(quote.quoteid, None)

    # ========== 查询接口 ==========
    def get_order(self, orderid: str):
        return self.orders.get(orderid)

    def get_trade(self, tradeid: str):
        return self.trades.get(tradeid)

    def get_position(self, instrument_id: str):
        return self.positions.get(instrument_id)

    def get_account(self, accountid: str):
        return self.accounts.get(accountid)

    def get_quote(self, quoteid: str):
        return self.quotes.get(quoteid)

    def get_all_orders(self):
        return list(self.orders.values())

    def get_all_trades(self):
        return list(self.trades.values())

    @property
    def trade_log(self) -> list[TradeData]:
        """Ordered view of authoritative trades for live and backtest callers."""
        return self.get_all_trades()

    def get_all_positions(self):
        return list(self.positions.values())

    def get_all_accounts(self):
        return list(self.accounts.values())

    def get_all_quotes(self):
        return list(self.quotes.values())

    def get_all_active_orders(self):
        return list(self.active_orders.values())

    def get_all_active_quotes(self):
        return list(self.active_quotes.values())

    def filter_orders(
        self,
        limit: int | None = None,
        start_date: str | int | float | datetime | None = None,
        end_date: str | int | float | datetime | None = None,
    ) -> list[OrderData]:
        """
        从 self.orders 里筛选订单
        - limit: 返回的订单数量上限（取最后N条），None 表示不限制
        - start_date: 开始时间（字符串/时间戳/datetime）
        - end_date: 结束时间（字符串/时间戳/datetime）
        """

        def to_dt(val):
            """安全转换为 datetime"""
            if val is None:
                return None
            return pd.to_datetime(val)

        start_dt = to_dt(start_date)
        end_dt = to_dt(end_date)

        # 所有订单（values 是 OrderData）
        orders = list(self.orders.values())

        # 按 datetime 排序（确保时间顺序）
        orders.sort(key=lambda o: o.datetime or datetime.min)

        # 时间过滤
        filtered = []
        for o in orders:
            if not o.datetime:  # 没有时间的订单直接跳过
                continue
            if start_dt and o.datetime < start_dt:
                continue
            if end_dt and o.datetime > end_dt:
                continue
            filtered.append(o)

        # 应用 limit
        if limit is not None:
            filtered = filtered[-limit:]

        return filtered


class OmsMhi(OmsBase):
    def __init__(self, event_engine: EventEngine | None = None):
        super().__init__(event_engine)

    def process_position_snapshot_event(self, event: Event):
        position: PositionData = event.data
        if position.instrument_id not in self.positions.keys():
            self.positions[position.instrument_id] = position
        else:
            old_position = self.positions[position.instrument_id]
            delta_volume = abs(position.volume) if position.direction == Direction.LONG else -abs(position.volume)
            old_volume = abs(old_position.volume) if old_position.direction == Direction.LONG else -abs(old_position.volume)
            new_position = old_volume + delta_volume
            if new_position == 0:
                self.positions.pop(position.instrument_id)
            elif new_position < 0:
                old_position.volume = abs(new_position)
                old_position.direction = Direction.SHORT
            else:
                old_position.volume = abs(new_position)
                old_position.direction = Direction.LONG
        current = self.positions.get(position.instrument_id)
        if current is None:
            current = PositionData(
                gateway_name=position.gateway_name,
                instrument_id=position.instrument_id,
                exchange=position.exchange,
                direction=Direction.NET,
                volume=0,
                price=0,
                margin=0,
            )
        self.event_engine.put(Event(EVENT_POSITION, deepcopy(current)))

