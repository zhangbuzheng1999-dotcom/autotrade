from datetime import datetime
import uuid

from sqlalchemy.sql.functions import current_date

from coreutils.object import (ModifyRequest, CancelRequest,
                              OrderRequest,
                              OrderData,
                              BarData,
                              TradeData, PositionData)

from coreutils.constant import Direction, OrderType, OrderStatus

class BacktestGateway:
    """
    模拟交易所模块，负责：
    1. 接收订单请求（OrderRequest），生成订单数据并分类存储。
    2. 根据 on_bar 驱动逻辑，处理订单激活（条件单触发）和撮合成交。
    3. 通过回调通知 Gateway：订单状态变化、成交回报。

    特点：
    - 支持多标的，每个 symbol 独立维护订单簿。
    - 不涉及资金计算（资金管理在 BacktestEngine）。
    - 不处理滑点、手续费逻辑（可扩展）。
    - 核心是订单生命周期管理 + 撮合逻辑。
    """

    def __init__(self, gateway_name: str, backtest_engine):
        """
        初始化模拟交易所
        gateway_name: Gateway 名称（用于标识来源）
        on_order: 回调函数 -> 用于通知订单状态变化
        on_trade: 回调函数 -> 用于通知成交回报
        """
        self.gateway_name = gateway_name

        # 激活订单：{symbol: {orderid: OrderData}}
        self.active_orders: dict[str, dict[str, OrderData]] = {}

        # 待激活订单：{symbol: {orderid: OrderData}}
        self.inactive_orders: dict[str, dict[str, OrderData]] = {}

        # 回测引擎
        self.backtest_engine = backtest_engine

        # 当前时间
        self.current_date = datetime.today()

    def send_order(self, req: OrderRequest) -> str:
        """
        接收 OrderRequest 并生成 OrderData，分类存入订单簿。
        分类规则：
        - 限价单(LIMIT)、市价单(MARKET)：立即进入 active_orders。
        - 条件单(STP_LMT、STP_MKT)：进入 inactive_orders。
        """
        orderid = uuid.uuid4().hex[:8]
        order = req.create_order_data(orderid, self.gateway_name)
        order.datetime = self.current_date
        symbol = order.symbol

        if symbol not in self.active_orders:
            self.active_orders[symbol] = {}
        if symbol not in self.inactive_orders:
            self.inactive_orders[symbol] = {}

        if order.type in [OrderType.LIMIT, OrderType.MARKET]:
            order.status = OrderStatus.SUBMITTING
            self.active_orders[symbol][orderid] = order
        elif order.type in [OrderType.STP_LMT, OrderType.STP_MKT]:
            order.status = OrderStatus.PENDING
            self.inactive_orders[symbol][orderid] = order

        self.on_order(order)
        return orderid

    def cancel_order(self, req: CancelRequest):
        """撤销订单，支持多标的"""
        symbol = req.symbol
        oid = req.orderid
        order = None

        if oid in self.active_orders.get(symbol, {}):
            order = self.active_orders[symbol].pop(oid)
        elif oid in self.inactive_orders.get(symbol, {}):
            order = self.inactive_orders[symbol].pop(oid)

        if order and order.status not in [OrderStatus.ALLTRADED, OrderStatus.ALLCANCELLED]:
            order.status = OrderStatus.ALLCANCELLED
            order.datetime = self.current_date
            self.on_order(order)

    def modify_order(self, req: ModifyRequest):
        """修改订单：按 symbol 定位"""
        symbol = req.symbol
        oid = req.orderid
        order = self.active_orders.get(symbol, {}).get(oid) or self.inactive_orders.get(symbol, {}).get(oid)

        if not order:
            reference = f"[警告] 修改失败，订单 {oid} 不存在,req:{req},current_date:{self.current_date}"
            print(reference)
            order = OrderData(symbol=req.symbol, exchange=req.exchange, orderid=req.orderid, type=OrderType.MARKET,
                              direction=Direction.LONG, volume=req.qty, price=req.price, gateway_name=self.gateway_name,
                              status=OrderStatus.REJECTED, traded=0, avgFillPrice=0,
                              broker_orderid=None, reference=reference)
            self.on_order(order)
            return

        if order.status in [OrderStatus.ALLTRADED, OrderStatus.ALLCANCELLED, OrderStatus.PARTCANCELLED]:
            reference = f"[警告] 修改失败，订单 {oid} 状态={order.status}"
            order = OrderData(symbol=req.symbol, exchange=req.exchange, orderid=req.orderid, type=OrderType.MARKET,
                              direction=Direction.LONG, volume=req.qty, price=req.price, gateway_name=self.gateway_name,
                              status=OrderStatus.REJECTED, traded=0, avgFillPrice=0,
                              broker_orderid=None, reference=reference)
            self.on_order(order)
            return

        if req.qty < order.traded:
            reference = f"[警告] 修改失败，新数量 {req.qty} 小于已成交数量 {order.traded}"
            order = OrderData(symbol=req.symbol, exchange=req.exchange, orderid=req.orderid, type=OrderType.MARKET,
                              direction=Direction.LONG, volume=req.qty, price=req.price, gateway_name=self.gateway_name,
                              status=OrderStatus.REJECTED, traded=0, avgFillPrice=0,
                              broker_orderid=None, reference=reference)
            self.on_order(order)
            return

        order.price = req.price
        order.volume = req.qty
        order.trigger_price = req.trigger_price
        order.status = OrderStatus.MODIFIED
        order.datetime = self.current_date

        self.on_order(order)

    def on_bar(self, bar: BarData):
        """
        撮合逻辑：
        - 只处理 bar.symbol 对应的订单。
        """
        symbol = bar.symbol
        self.current_date = bar.datetime
        # Step 1: 激活止损单
        for oid, order in list(self.inactive_orders.get(symbol, {}).items()):
            if self._stop_trigger(order, bar):
                order.status = OrderStatus.PENDING
                order.datetime = self.current_date
                # 记录触发在哪根bar
                order.triggered_bar = bar.datetime

                self.active_orders[symbol][oid] = order
                del self.inactive_orders[symbol][oid]
                self.on_order(order)

        # Step 2: 撮合激活订单
        for oid, order in list(self.active_orders.get(symbol, {}).items()):
            if order.status in [OrderStatus.ALLTRADED, OrderStatus.ALLCANCELLED]:
                continue

            if order.type in [OrderType.MARKET, OrderType.STP_MKT]:
                if order.direction == Direction.LONG:
                    close_price = max(order.trigger_price, bar.open_price)
                else:
                    close_price = min(order.trigger_price, bar.open_price)

                self._fill_order(order, close_price)
                del self.active_orders[symbol][oid]

            elif order.type in [OrderType.ABS_LMT]:
                if self._can_fill_absolute(order, bar):
                    self._fill_order(order, order.price)
                    del self.active_orders[symbol][oid]

            elif order.type in [OrderType.LIMIT, OrderType.STP_LMT]:
                if self._can_fill(order, bar):
                    # 如果是 STP_LMT 并且刚刚在这根 bar 被触发
                    if order.type == OrderType.STP_LMT and getattr(order, "triggered_bar", None) == bar.datetime:
                        # 当根 bar 内只能盘中成交 → 给限价
                        self._fill_order(order, order.price)
                    else:
                        # 普通限价逻辑（允许开盘成交）
                        self._fill_order(order, self._get_fill_price(order, bar))
                    del self.active_orders[symbol][oid]

    def _stop_trigger(self, order: OrderData, bar: BarData) -> bool:
        return (order.direction == Direction.LONG and bar.high_price >= order.trigger_price) or \
            (order.direction == Direction.SHORT and bar.low_price <= order.trigger_price)

    def _can_fill(self, order: OrderData, bar: BarData) -> bool:
        return (order.direction == Direction.LONG and bar.low_price <= order.price) or \
            (order.direction == Direction.SHORT and
             bar.high_price >= order.price)

    def _get_fill_price(self, order: OrderData, bar: BarData) -> float:
        if order.direction == Direction.LONG:
            # 开盘小于等于限价 → 开盘成交
            if bar.open_price <= order.price:
                return bar.open_price
            # 否则，盘中触发成交 → 限价
            return order.price
        else:  # SHORT
            # 开盘大于等于限价 → 开盘成交
            if bar.open_price >= order.price:
                return bar.open_price
            # 否则，盘中触发成交 → 限价
            return order.price

    def _can_fill_absolute(self, order: OrderData, bar: BarData) -> bool:
        return bar.low_price <= order.price <= bar.high_price

    def _fill_order(self, order: OrderData, price: float):
        order.status = OrderStatus.ALLTRADED
        order.traded = order.volume
        order.avgFillPrice = price
        order.datetime = self.current_date

        trade = TradeData(
            symbol=order.symbol,
            exchange=order.exchange,
            orderid=order.orderid,
            tradeid=str(uuid.uuid4())[:8],
            direction=order.direction,
            offset=order.offset,
            price=price,
            volume=order.volume,
            traded=order.volume,
            avgFillPrice=price,
            datetime=order.datetime,
            status=OrderStatus.ALLTRADED,
            gateway_name=self.gateway_name, reference=order.reference
        )

        self.on_trade(trade)
        self.on_order(order)
        position_data = PositionData(symbol=order.symbol,
                                     exchange=order.exchange,
                                     direction=order.direction, volume=order.volume, gateway_name=self.gateway_name)
        self.on_position(position_data)
    def get_orders(self) -> list[OrderData]:
        """返回所有未完成订单"""
        orders = []
        for sym_orders in self.active_orders.values():
            orders.extend(sym_orders.values())
        for sym_orders in self.inactive_orders.values():
            orders.extend(sym_orders.values())
        return orders

    def on_order(self, order: OrderData) -> None:
        self.backtest_engine.on_order(order)

    def on_trade(self, trade: TradeData) -> None:
        self.backtest_engine.on_trade(trade)

    def on_position(self, position: PositionData) -> None:
        self.backtest_engine.on_position(position)
