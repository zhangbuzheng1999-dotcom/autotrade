import unittest
from datetime import datetime

from autotrade.coreutils.constant import Direction, Exchange, Product
from autotrade.coreutils.object import ContractData, PositionData, TickData, TradeData
from autotrade.engine.event_engine import (
    EVENT_DATA,
    EVENT_POSITION,
    EVENT_POSITION_SNAPSHOT,
    EVENT_TRADE,
    Event,
)
from autotrade.backtest.backtest_event_engine import BacktestEventEngine
from autotrade.engine.oms_engine import OmsBase
from autotrade.engine.security_manager import SecurityManager


class StateOwnershipTests(unittest.TestCase):
    def setUp(self):
        self.events = BacktestEventEngine()
        self.securities = SecurityManager(self.events)
        self.oms = OmsBase(self.events)

    def test_security_manager_alone_consumes_market_and_contract_data(self):
        contract = ContractData(
            gateway_name="SIM",
            symbol="RB99",
            exchange=Exchange.SHFE,
            name="rebar",
            product=Product.FUTURES,
            size=10,
            pricetick=1,
        )
        tick = TickData(
            gateway_name="SIM",
            symbol="RB99",
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
            symbol="RB99",
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
            symbol="RB99",
            exchange=Exchange.SHFE,
            direction=Direction.NET,
            volume=3,
            price=3490,
        )

        self.events.put(Event(EVENT_POSITION_SNAPSHOT, snapshot))

        self.assertEqual(self.oms.get_position("RB99").volume, 3)
        self.assertEqual(len(published), 1)


if __name__ == "__main__":
    unittest.main()
