import pytest
from datetime import datetime
from coreutils.object import BarData, OrderRequest, CancelRequest, ModifyRequest
from coreutils.constant import Direction, OrderType, Offset, Status, Exchange, Interval
from backtest.backtest_gateway import BacktestGateway


class DummyEngine:
    """模拟回测引擎，用于捕获 Gateway 回调"""
    def __init__(self):
        self.orders = []
        self.trades = []

    def on_order(self, order):
        self.orders.append(order)

    def on_trade(self, trade):
        self.trades.append(trade)


@pytest.fixture
def gateway():
    engine = DummyEngine()
    return BacktestGateway(gateway_name="test_gateway", backtest_engine=engine), engine


def make_order_request(symbol, price, volume, direction, order_type, trigger_price=None):
    req = OrderRequest(
        symbol=symbol,
        exchange=Exchange.SHFE,
        direction=direction,
        type=order_type,
        offset=Offset.OPEN,
        price=price,
        volume=volume
    )
    req.trigger_price = trigger_price
    return req


def make_bar(symbol, open_price, high_price, low_price, close_price):
    return BarData(
        symbol=symbol,
        exchange=Exchange.SHFE,
        datetime=datetime.now(),
        interval=Interval.MINUTE,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        close_price=close_price,
        gateway_name="test_gateway"
    )


def assert_close(a, b, tol=1e-6):
    assert abs(a - b) <= tol, f"Expected {b}, got {a}"


# ========== 基础测试 ==========
def test_market_order_fill_long(gateway):
    gw, engine = gateway
    req = make_order_request("RB99", price=0, volume=2, direction=Direction.LONG, order_type=OrderType.MARKET)
    orderid = gw.send_order(req)

    bar = make_bar("RB99", open_price=3500, high_price=3520, low_price=3490, close_price=3510)
    gw.on_bar(bar)

    # 检查成交
    assert len(engine.trades) == 1
    trade = engine.trades[0]
    assert_close(trade.price, 3500)
    assert trade.volume == 2
    assert engine.orders[-1].status == Status.ALLTRADED
    assert orderid not in gw.active_orders["RB99"]


def test_market_order_fill_short(gateway):
    gw, engine = gateway
    req = make_order_request("RB99", price=0, volume=1, direction=Direction.SHORT, order_type=OrderType.MARKET)
    gw.send_order(req)

    bar = make_bar("RB99", open_price=3550, high_price=3560, low_price=3540, close_price=3555)
    gw.on_bar(bar)

    trade = engine.trades[0]
    assert_close(trade.price, 3550)
    assert trade.volume == 1
    assert trade.direction == Direction.SHORT


# ========== 限价单 ==========
def test_limit_order_fill(gateway):
    gw, engine = gateway
    req = make_order_request("RB99", price=3500, volume=1, direction=Direction.LONG, order_type=OrderType.LIMIT)
    gw.send_order(req)

    bar = make_bar("RB99", open_price=3490, high_price=3510, low_price=3480, close_price=3505)
    gw.on_bar(bar)

    trade = engine.trades[0]
    # 成交价：max(order.price, bar.open_price)=max(3500, 3490)=3500
    assert_close(trade.price, 3500)
    assert trade.volume == 1


def test_limit_order_gap_open(gateway):
    gw, engine = gateway
    req = make_order_request("RB99", price=3500, volume=1, direction=Direction.LONG, order_type=OrderType.LIMIT)
    gw.send_order(req)

    bar = make_bar("RB99", open_price=3600, high_price=3620, low_price=3590, close_price=3610)
    gw.on_bar(bar)

    trade = engine.trades[0]
    # gap open → 成交价=开盘价3600
    assert_close(trade.price, 3600)


# ========== 止损单 ==========
def test_stop_limit_order_activation(gateway):
    gw, engine = gateway
    req = make_order_request("RB99", price=3560, volume=1, direction=Direction.LONG,
                              order_type=OrderType.STP_LMT, trigger_price=3550)
    gw.send_order(req)
    assert len(gw.inactive_orders["RB99"]) == 1

    # bar 触发 trigger_price
    bar = make_bar("RB99", open_price=3540, high_price=3560, low_price=3530, close_price=3555)
    gw.on_bar(bar)

    assert len(gw.inactive_orders["RB99"]) == 0
    trade = engine.trades[0]
    assert_close(trade.price, 3560)


# ========== 撤单 ==========
def test_cancel_order_active(gateway):
    gw, engine = gateway
    req = make_order_request("RB99", price=3500, volume=1, direction=Direction.LONG, order_type=OrderType.LIMIT)
    orderid = gw.send_order(req)

    cancel_req = CancelRequest(symbol="RB99", orderid=orderid,exchange=Exchange.SHFE)
    gw.cancel_order(cancel_req)

    assert engine.orders[-1].status == Status.ALLCANCELLED
    assert orderid not in gw.active_orders["RB99"]


def test_cancel_order_inactive(gateway):
    gw, engine = gateway
    req = make_order_request("RB99", price=3560, volume=1, direction=Direction.LONG,
                              order_type=OrderType.STP_LMT, trigger_price=3550)
    orderid = gw.send_order(req)
    cancel_req = CancelRequest(symbol="RB99", orderid=orderid,exchange=Exchange.SHFE)
    gw.cancel_order(cancel_req)

    assert engine.orders[-1].status == Status.ALLCANCELLED
    assert orderid not in gw.inactive_orders["RB99"]


# ========== 修改 ==========
def test_modify_order(gateway):
    gw, engine = gateway
    req = make_order_request("RB99", price=3500, volume=1, direction=Direction.LONG, order_type=OrderType.LIMIT)
    orderid = gw.send_order(req)

    modify_req = ModifyRequest(symbol="RB99", orderid=orderid, price=3490, qty=2, exchange=Exchange.SHFE)
    gw.modify_order(modify_req)

    assert engine.orders[-1].status == Status.MODIFIED
    modified_order = gw.active_orders["RB99"][orderid]
    assert_close(modified_order.price, 3490)
    assert modified_order.volume == 2


# ========== 多标的 ==========
def test_multi_symbols(gateway):
    gw, engine = gateway
    # 两个 symbol 下限价单
    req1 = make_order_request("RB99", price=3500, volume=1, direction=Direction.LONG, order_type=OrderType.LIMIT)
    gw.send_order(req1)
    req2 = make_order_request("CU99", price=70000, volume=1, direction=Direction.SHORT, order_type=OrderType.LIMIT)
    gw.send_order(req2)

    # 撮合 RB99
    bar_rb = make_bar("RB99", open_price=3500, high_price=3510, low_price=3490, close_price=3505)
    gw.on_bar(bar_rb)

    # 撮合 CU99
    bar_cu = make_bar("CU99", open_price=70050, high_price=70100, low_price=69900, close_price=70010)
    gw.on_bar(bar_cu)

    assert len(engine.trades) == 2
    prices = [t.price for t in engine.trades]
    assert 3500 in prices
    assert 70050 in prices
