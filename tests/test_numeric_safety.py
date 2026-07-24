import math
import unittest
from datetime import datetime, timedelta

import pandas as pd

from autotrade.coreutils.constant import Direction, Exchange, OrderType
from autotrade.coreutils.object import OrderRequest

from autotrade.backtest.accounting_manager import AccountingManager
from autotrade.backtest.backtest_gateway import Fill
from autotrade.backtest.backtest_engine import BacktestEngine
from autotrade.backtest.security_manager import SecurityManager
from autotrade.coreutils.object import Slice, TradeBar
from autotrade.backtest.data.pipeline import (
    DataRoutingConfig,
    _NamedData,
    _TimeSliceRouter,
)
from autotrade.backtest.data.reader import TradeBarReader
from autotrade.backtest.backtest_event_engine import BacktestEventEngine


class NumericSafetyTests(unittest.TestCase):
    def test_trade_bar_rejects_non_finite_price(self):
        with self.assertRaisesRegex(ValueError, "open must be finite"):
            TradeBar(
                symbol="A",
                time=datetime(2024, 1, 1),
                open=math.nan,
                high=1,
                low=1,
                close=1,
            )

    def test_reader_rejects_infinite_price(self):
        frame = pd.DataFrame(
            [{"symbol": "A", "time": datetime(2024, 1, 1),
              "open": 1, "high": math.inf, "low": 1, "close": 1}]
        )
        with self.assertRaisesRegex(ValueError, "high must be finite"):
            list(TradeBarReader().read(frame))

    def test_gateway_rejects_non_finite_fill(self):
        engine = BacktestEngine(logger=_QuietLogger())
        request = OrderRequest(
            symbol="A",
            exchange=Exchange.SSE,
            direction=Direction.LONG,
            type=OrderType.MARKET,
            volume=1,
        )
        order = engine.gateway.send_order(request)
        fill = Fill(order.orderid, order.symbol, math.nan, 1, datetime(2024, 1, 1))

        with self.assertRaisesRegex(ValueError, "fill price must be finite"):
            engine.gateway._apply_fill(order, fill)

        self.assertFalse(engine.oms.trade_log)

    def test_mark_to_market_rejects_non_finite_value(self):
        data = TradeBar(
            symbol="A",
            time=datetime(2024, 1, 1),
            open=1,
            high=1,
            low=1,
            close=1,
        )
        data.value = math.nan
        router = _TimeSliceRouter(
            DataRoutingConfig(valuation_data_names={"mark"})
        )

        with self.assertRaisesRegex(ValueError, "valuation price must be finite"):
            router.route([_NamedData("mark", data)])

    def test_intraday_annual_return_is_nan_instead_of_overflowing(self):
        result = AccountingManager._calculate_annual_return(100, 200, 60)
        self.assertTrue(math.isnan(result))

    def test_extreme_annual_return_becomes_infinity_instead_of_overflowing(self):
        result = AccountingManager._calculate_annual_return(
            1,
            1e100,
            timedelta(days=1).total_seconds(),
        )
        self.assertEqual(result, math.inf)

    def test_extreme_annual_loss_does_not_underflow_ratio(self):
        result = AccountingManager._calculate_annual_return(
            1e308,
            1e-308,
            timedelta(days=365).total_seconds(),
        )
        self.assertEqual(result, -1.0)


class SliceDataTests(unittest.TestCase):
    def test_empty_slice_has_no_data(self):
        slice_ = Slice(time=datetime(2024, 1, 1))

        self.assertFalse(slice_.has_data)
        self.assertFalse(slice_.contains_data("1m"))

    def test_slice_reports_named_data(self):
        bar = TradeBar(
            symbol="A",
            time=datetime(2024, 1, 1),
            open=1,
            high=1,
            low=1,
            close=1,
        )
        slice_ = Slice.from_named_data(bar.time, [("1m", bar)])

        self.assertTrue(slice_.has_data)
        self.assertTrue(slice_.contains_data("1m"))
        self.assertFalse(slice_.contains_data("5m"))


class _QuietLogger:
    def debug(self, _message):
        pass

    def info(self, _message):
        pass

    def warning(self, _message):
        pass

    def error(self, _message):
        pass


if __name__ == "__main__":
    unittest.main()
