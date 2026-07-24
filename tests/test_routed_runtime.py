import unittest
from datetime import datetime

from autotrade.backtest.backtest_event_engine import BacktestEventEngine
from autotrade.backtest.backtest_engine import BacktestEngine
from autotrade.coreutils.constant import Direction, Exchange, OrderType
from autotrade.coreutils.object import (
    OrderRequest,
    Slice,
    TickData,
    TimeSlice,
    TradeBar,
    ValuationUpdate,
)
from autotrade.engine.event_engine import (
    COMMAND_ACCOUNT_VALUATION,
    COMMAND_MARKET_AFTER,
    COMMAND_MARKET_BEFORE,
    COMMAND_ORDER_SUBMIT,
    EVENT_DATA,
    EVENT_LIVE_DATA,
    EVENT_SLICE,
    Event,
    Message,
    MessageKind,
)
from autotrade.engine.live_timeslice_builder import LiveTimeSliceBuilder
from autotrade.engine.order_router import OrderRouter
from autotrade.engine.security_manager import SecurityManager
from autotrade.engine.timeslice_driver import TimeSliceDriver


class RoutedRuntimeTests(unittest.TestCase):
    def test_command_routes_to_exact_target(self):
        engine = BacktestEventEngine()
        received = []
        engine.register_command(
            "execution",
            COMMAND_ORDER_SUBMIT,
            lambda message: received.append(message.data),
        )

        engine.put(
            Message(
                MessageKind.COMMAND,
                COMMAND_ORDER_SUBMIT,
                "order-1",
                source="order_router",
                target="execution",
            )
        )

        self.assertEqual(received, ["order-1"])

    def test_timeslice_driver_preserves_backtest_phase_order(self):
        engine = BacktestEventEngine()
        driver = TimeSliceDriver(engine, simulated_broker=True)
        trace = []
        engine.register(EVENT_DATA, lambda event: trace.append("security"))
        engine.register_command(
            "simulated_broker",
            COMMAND_MARKET_BEFORE,
            lambda message: trace.append("before"),
        )
        engine.register(EVENT_SLICE, lambda event: trace.append("strategy"))
        engine.register_command(
            "simulated_broker",
            COMMAND_MARKET_AFTER,
            lambda message: trace.append("after"),
        )
        engine.register_command(
            "simulated_broker",
            COMMAND_ACCOUNT_VALUATION,
            lambda message: trace.append("valuation"),
        )
        when = datetime(2024, 1, 1)
        bar = TradeBar(
            symbol="A",
            time=when,
            open=1,
            high=1,
            low=1,
            close=1,
        )

        driver.process(
            TimeSlice(
                time=when,
                slice=Slice.from_named_data(when, [("1m", bar)]),
                security_updates=(bar,),
                valuation_updates=(
                    ValuationUpdate("A", when, 1, bar),
                ),
            )
        )

        self.assertEqual(
            trace,
            ["security", "before", "strategy", "after", "valuation"],
        )

    def test_live_input_updates_security_before_strategy_slice(self):
        engine = BacktestEventEngine()
        securities = SecurityManager(engine)
        LiveTimeSliceBuilder(engine)
        observed_prices = []
        engine.register(
            EVENT_SLICE,
            lambda event: observed_prices.append(
                (
                    securities["A"].price,
                    event.data.ticks["live"]["A"][0].price,
                )
            ),
        )
        tick = TickData(
            gateway_name="LIVE",
            symbol="A",
            exchange=Exchange.SSE,
            datetime=datetime(2024, 1, 1),
            last_price=10,
        )

        engine.put(Event(EVENT_LIVE_DATA, tick))

        self.assertEqual(observed_prices, [(10, 10)])

    def test_live_builder_and_backtest_feed_share_timeslice_driver_contract(self):
        engine = BacktestEventEngine()
        received = []

        class RecordingDriver:
            def process(self, time_slice):
                received.append(time_slice)

        builder = LiveTimeSliceBuilder(engine, driver=RecordingDriver())
        tick = TickData(
            gateway_name="LIVE",
            symbol="A",
            exchange=Exchange.SSE,
            datetime=datetime(2024, 1, 1),
            last_price=10,
        )

        built = builder.push(tick)

        self.assertIs(received[0], built)
        self.assertIsInstance(received[0], TimeSlice)
        self.assertIsInstance(received[0].slice, Slice)

    def test_security_manager_does_not_forward_raw_data_to_strategy(self):
        engine = BacktestEventEngine()
        SecurityManager(engine)
        slices = []
        engine.register(EVENT_SLICE, lambda event: slices.append(event.data))
        bar = TradeBar(
            symbol="A",
            time=datetime(2024, 1, 1),
            open=1,
            high=1,
            low=1,
            close=1,
        )

        engine.put(Event(EVENT_DATA, bar))

        self.assertEqual(slices, [])

    def test_order_router_forwards_shared_command_to_execution(self):
        engine = BacktestEventEngine()
        OrderRouter(engine)
        received = []
        engine.register_command(
            "execution",
            COMMAND_ORDER_SUBMIT,
            lambda message: received.append(message.data),
        )
        request = OrderRequest(
            symbol="A",
            exchange=Exchange.SSE,
            direction=Direction.LONG,
            type=OrderType.MARKET,
            volume=1,
        )

        engine.put(
            Message(
                MessageKind.COMMAND,
                COMMAND_ORDER_SUBMIT,
                request,
                source="strategy.test",
                target="order_router",
            )
        )

        self.assertEqual(received, [request])

    def test_backtest_gateway_consumes_same_order_command(self):
        engine = BacktestEngine(logger=_QuietLogger())
        request = OrderRequest(
            symbol="A",
            exchange=Exchange.SSE,
            direction=Direction.LONG,
            type=OrderType.MARKET,
            volume=1,
        )

        engine.event_engine.put(
            Message(
                MessageKind.COMMAND,
                COMMAND_ORDER_SUBMIT,
                request,
                source="strategy.test",
                target="order_router",
            )
        )

        self.assertEqual(len(engine.oms.get_all_orders()), 1)
        self.assertEqual(len(engine.gateway.pending_orders["A"]), 1)


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
