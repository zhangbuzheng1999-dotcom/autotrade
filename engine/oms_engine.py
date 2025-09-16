from datetime import datetime
from coreutils.constant import Direction,OrderStatus
from engine.event_engine import Event, EventEngine
from engine.event_engine import (
                                    EVENT_TICK,
                                    EVENT_ORDER,
                                    EVENT_TRADE,
                                    EVENT_POSITION,
                                    EVENT_ACCOUNT,
                                    EVENT_CONTRACT,
                                    EVENT_QUOTE
                                )
from coreutils.object import (
    CancelRequest,
    LogData,
    OrderRequest,
    QuoteData,
    QuoteRequest,
    SubscribeRequest,
    HistoryRequest,
    OrderData,
    BarData,
    TickData,
    TradeData,
    PositionData,
    AccountData,
    ContractData,
    Exchange,
    Product
)
import pandas as pd

class OmsBase:
    """
    通用 OMS 基类：
    - 维护最新行情、订单、成交、仓位、账户等状态快照。
    - 通过事件引擎接收更新。
    """

    def __init__(self, event_engine: EventEngine = EventEngine()):
        self.event_engine = event_engine

        # 数据缓存
        self.ticks: dict[str, TickData] = {}
        self.orders: dict[str, OrderData] = {}
        self.trades: dict[str, TradeData] = {}
        self.positions: dict[str, PositionData] = {}
        self.accounts: dict[str, AccountData] = {}
        self.contracts: dict[str, ContractData] = {}
        self.quotes: dict[str, QuoteData] = {}

        self.active_orders: dict[str, OrderData] = {}
        self.active_quotes: dict[str, QuoteData] = {}

        # 注册事件
        self.register_event()

    def register_event(self):
        """注册事件监听"""
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        self.event_engine.register(EVENT_POSITION, self.process_position_event)
        self.event_engine.register(EVENT_ACCOUNT, self.process_account_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_contract_event)
        self.event_engine.register(EVENT_QUOTE, self.process_quote_event)

    # ========== 事件处理 ==========
    def process_tick_event(self, event: Event):
        tick: TickData = event.data
        self.ticks[tick.vt_symbol] = tick

    def process_order_event(self, event: Event):
        order: OrderData = event.data
        self.orders[order.vt_orderid] = order
        if order.is_active():
            self.active_orders[order.vt_orderid] = order
        elif order.vt_orderid in self.active_orders:
            self.active_orders.pop(order.vt_orderid)

    def process_trade_event(self, event: Event):
        trade: TradeData = event.data
        self.trades[trade.vt_tradeid] = trade

    def process_position_event(self, event: Event):
        position: PositionData = event.data
        self.positions[position.symbol] = position

    def process_account_event(self, event: Event):
        account: AccountData = event.data
        self.accounts[account.vt_accountid] = account

    def process_contract_event(self, event: Event):
        contract: ContractData = event.data
        self.contracts[contract.vt_symbol] = contract

    def process_quote_event(self, event: Event):
        quote: QuoteData = event.data
        self.quotes[quote.vt_quoteid] = quote
        if quote.is_active():
            self.active_quotes[quote.vt_quoteid] = quote
        elif quote.vt_quoteid in self.active_quotes:
            self.active_quotes.pop(quote.vt_quoteid)

    # ========== 查询接口 ==========
    def get_tick(self, vt_symbol: str):
        return self.ticks.get(vt_symbol)

    def get_order(self, vt_orderid: str):
        return self.orders.get(vt_orderid)

    def get_trade(self, vt_tradeid: str):
        return self.trades.get(vt_tradeid)

    def get_position(self, symbol: str):
        return self.positions.get(symbol)

    def get_account(self, vt_accountid: str):
        return self.accounts.get(vt_accountid)

    def get_contract(self, vt_symbol: str):
        return self.contracts.get(vt_symbol)

    def get_quote(self, vt_quoteid: str):
        return self.quotes.get(vt_quoteid)

    def get_all_ticks(self):
        return list(self.ticks.values())

    def get_all_orders(self):
        return list(self.orders.values())

    def get_all_trades(self):
        return list(self.trades.values())

    def get_all_positions(self):
        return list(self.positions.values())

    def get_all_accounts(self):
        return list(self.accounts.values())

    def get_all_contracts(self):
        return list(self.contracts.values())

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
    def __init__(self, event_engine: EventEngine = EventEngine()):
        super().__init__(event_engine)
    def process_position_event(self, event: Event):
        position: PositionData = event.data
        if position.symbol not in self.positions.keys():
            self.positions[position.symbol] = position
        else:
            old_position = self.positions[position.symbol]
            delta_volume = abs(position.volume) if position.direction == Direction.LONG else -abs(position.volume)
            old_volume = abs(old_position.volume) if old_position.direction == Direction.LONG else -abs(old_position.volume)
            new_position = old_volume + delta_volume
            if new_position == 0:
                self.positions.pop(position.symbol)
            elif new_position < 0:
                old_position.volume = abs(new_position)
                old_position.direction = Direction.SHORT
            else:
                old_position.volume = abs(new_position)
                old_position.direction = Direction.LONG

