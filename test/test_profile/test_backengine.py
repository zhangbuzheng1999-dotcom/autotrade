import pandas as pd
from datetime import datetime, timedelta
from coreutils.constant import Direction, OrderType, Interval, Exchange
from coreutils.object import OrderRequest, BarData
from backtest.backtest_engine import BacktestEngine

# 策略类
class TestStrategy:
    def __init__(self, engine):
        self.engine = engine

    def on_bar(self, bar: BarData):
        actions = {
            datetime(2023, 1, 1): ("LONG", 2),
            datetime(2023, 1, 2): ("LONG", 1),
            datetime(2023, 1, 3): ("SHORT", 1),
            datetime(2023, 1, 4): ("SHORT", 1),
            datetime(2023, 1, 5): ("SHORT", 3),
            datetime(2023, 1, 6): ("SHORT", 1),
            datetime(2023, 1, 7): ("LONG", 1),
            datetime(2023, 1, 8): ("LONG", 5),
            datetime(2023, 1, 9): ("SHORT", 4),
        }
        if bar.datetime in actions:
            direction, vol = actions[bar.datetime]
            req = OrderRequest(symbol="RB99.SHFE", price=bar.open_price, volume=vol,
                               direction=Direction[direction], type=OrderType.MARKET, exchange=Exchange.SHFE)
            self.engine.gateway.send_order(req)

    def on_trade(self, trade):
        pass

    def on_order(self, order):
        pass


def test_backtest_engine():
    # 生成10天的bar数据
    dates = [datetime(2023, 1, 1) + timedelta(days=i) for i in range(10)]
    open_prices = [3500 + i * 10 for i in range(10)]
    high_prices = [p + 10 for p in open_prices]
    low_prices = [p - 10 for p in open_prices]
    close_prices = [p for p in open_prices]
    symbol = "RB99.SHFE"
    ktype = Interval.DAILY
    exchange = Exchange.SHFE

    df = pd.DataFrame({
        "trade_date": dates,
        "open": open_prices,
        "high": high_prices,
        "low": low_prices,
        "close": close_prices,
        "symbol": [symbol] * 10,
        "ktype": [ktype] * 10
    })

    engine = BacktestEngine(daily_update_interval=Interval.DAILY)
    contract_params = {
        "RB99.SHFE": {"size": 10, "margin_rate": 0.1, "long_rate": 0.0002, "short_rate": 0.0002}
    }
    engine.set_contracts(contract_params)
    engine.add_strategy(TestStrategy)
    engine.load_data({symbol: df})
    engine.run()

    result = engine.account_daily

    expected = [
        (999986.0, 7000.0, 0.0, 0.0),
        (999978.98, 10530.0, 0.0, 200.0),
        (1000138.61, 7040.0, 166.67, 333.33),
        (1000398.21, 3530.0, 433.33, 266.67),
        (1000743.64, 7080.0, 800.0, 0.0),
        (1000736.54, 10650.0, 800.0, -200.0),
        (1000562.75, 7120.0, 633.33, -333.33),
        (999993.72, 10710.0, 100.0, 0.0),
        (1000265.08, 3580.0, 400.0, 0.0),
        (1000265.08, 3580.0, 400.0, -100.0),
    ]

    for i, (timestamp, account) in enumerate(result.items()):
        cash = round(account["cash"], 2)
        margin = round(account["margin"], 2)
        realized = round(account["realized_pnl"], 2)
        unrealized = round(account["unrealized_pnl"], 2)
        exp_cash, exp_margin, exp_realized, exp_unrealized = expected[i]

        assert abs(cash - exp_cash) < 0.5
        assert abs(margin - exp_margin) < 1
        assert abs(realized - exp_realized) < 1
        assert abs(unrealized - exp_unrealized) < 1
