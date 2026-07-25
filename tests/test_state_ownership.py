import unittest
from datetime import datetime

from autotrade.coreutils.constant import Direction, Exchange, OrderType, Product
from autotrade.coreutils.object import (
    ContractData,
    OrderRequest,
    PositionData,
    Slice,
    TickData,
    TimeSlice,
    TradeBar,
    TradeData,
    ValuationUpdate,
)
from autotrade.engine.event_engine import (
    EVENT_DATA,
    EVENT_POSITION,
    EVENT_POSITION_SNAPSHOT,
    EVENT_TRADE,
    Event,
)
from autotrade.backtest.event_engine import BacktestEventEngine
from autotrade.backtest.backtest_engine import BacktestEngine
from autotrade.backtest.gateway import Fill
from autotrade.backtest.gateway import AccountLedger
from autotrade.backtest.gateway import SimulatedOrderBook
from autotrade.backtest.gateway import MatchingEngine
from autotrade.engine.oms import OmsBase
from autotrade.engine.security_manager import SecurityManager


class StateOwnershipTests(unittest.TestCase):
    def setUp(self):
        self.events = BacktestEventEngine()
        self.securities = SecurityManager(self.events)
        self.oms = OmsBase(self.events)

    def test_security_manager_alone_consumes_market_and_contract_data(self):
        contract = ContractData(
            gateway_name="SIM",
            instrument_id="RB99",
            exchange=Exchange.SHFE,
            name="rebar",
            product=Product.FUTURES,
            size=10,
            pricetick=1,
        )
        tick = TickData(
            gateway_name="SIM",
            instrument_id="RB99",
            exchange=Exchange.SHFE,
            datetime=datetime(2024, 1, 1),
            last_price=3500,
            bid_price_1=3499,
            ask_price_1=3501,
        )

        self.events.put(Event(EVENT_DATA, contract))
        self.events.put(Event(EVENT_DATA, tick))

        security = self.securities["RB99"]
        self.assertEqual(security.multiplier, 10)
        self.assertEqual(security.price, 3500)
        self.assertFalse(hasattr(self.oms, "ticks"))
        self.assertFalse(hasattr(self.oms, "contracts"))

    def test_oms_projects_trade_and_publishes_position(self):
        positions = []
        self.events.register(
            EVENT_POSITION,
            lambda event: positions.append(event.data),
        )
        trade = TradeData(
            gateway_name="SIM",
            instrument_id="RB99",
            exchange=Exchange.SHFE,
            orderid="O1",
            tradeid="T1",
            direction=Direction.LONG,
            price=3500,
            volume=2,
            datetime=datetime(2024, 1, 1),
        )

        self.events.put(Event(EVENT_TRADE, trade))

        self.assertEqual(self.oms.get_position("RB99").volume, 2)
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0].volume, 2)

    def test_broker_position_snapshot_is_reconciliation_input(self):
        published = []
        self.events.register(
            EVENT_POSITION,
            lambda event: published.append(event.data),
        )
        snapshot = PositionData(
            gateway_name="SIM",
            instrument_id="RB99",
            exchange=Exchange.SHFE,
            direction=Direction.NET,
            volume=3,
            price=3490,
        )

        self.events.put(Event(EVENT_POSITION_SNAPSHOT, snapshot))

        self.assertEqual(self.oms.get_position("RB99").volume, 3)
        self.assertEqual(len(published), 1)

    def test_backtest_uses_shared_oms_and_composed_simulated_broker(self):
        engine = BacktestEngine(logger=_QuietLogger())

        self.assertIs(type(engine.oms), OmsBase)
        self.assertIsInstance(engine.gateway.order_book, SimulatedOrderBook)
        self.assertIsInstance(engine.gateway.matching_engine, MatchingEngine)
        self.assertIsInstance(engine.gateway.account_ledger, AccountLedger)
        self.assertIsNotNone(
            engine.oms.get_account(engine.gateway.account_ledger.account.accountid)
        )

    def test_backtest_fill_and_valuation_do_not_replay_position_snapshots(self):
        engine = BacktestEngine(logger=_QuietLogger())
        position_events = []
        snapshot_events = []
        engine.event_engine.register(
            EVENT_POSITION,
            lambda event: position_events.append(event.data),
        )
        engine.event_engine.register(
            EVENT_POSITION_SNAPSHOT,
            lambda event: snapshot_events.append(event.data),
        )
        request = OrderRequest(
            instrument_id="A",
            exchange=Exchange.SSE,
            direction=Direction.LONG,
            type=OrderType.MARKET,
            volume=1,
        )
        order = engine.gateway.send_order(request)
        when = datetime(2024, 1, 1)
        engine.gateway._apply_fill(
            order,
            Fill(order.orderid, "A", 10, 1, when),
        )

        bar = TradeBar(
            instrument_id="A",
            exchange=Exchange.SSE,
            time=when,
            open=10,
            high=10,
            low=10,
            close=10,
        )
        engine.gateway.process_valuation(
            TimeSlice(
                time=when,
                slice=Slice(time=when),
                valuation_updates=(
                    ValuationUpdate("A", when, 10, bar),
                ),
            )
        )

        self.assertEqual(len(position_events), 1)
        self.assertFalse(snapshot_events)
        self.assertEqual(engine.oms.get_position("A").volume, 1)
        self.assertEqual(engine.gateway.account_ledger.positions["A"].volume, 1)


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
