import unittest
from datetime import datetime

import pandas as pd

from autotrade.coreutils.constant import Exchange

from autotrade.engine.security_manager import SecurityManager
from autotrade.coreutils.object import EquitySecurity, FutureContract
from autotrade.backtest.data import (
    DataManager,
    DataRoutingConfig,
    EquityStateReader,
    FutureStateReader,
    TradeBarReader,
)


class DataPipelineTests(unittest.TestCase):
    def test_bootstrap_then_routed_market_slice_uses_same_object_reference(self):
        equities = pd.DataFrame(
            [{"instrument_id": "AAA", "multiplier": 2, "margin_rate": 0.1}]
        )
        bars = pd.DataFrame(
            [{
                "instrument_id": "AAA",
                "time": "2024-01-02 09:31:00",
                "open": 10,
                "high": 12,
                "low": 9,
                "close": 11,
            }]
        )
        manager = DataManager(
            DataRoutingConfig(
                strategy_data_names={"1m"},
                security_data_names={"equities", "1m"},
                valuation_data_names={"1m"},
            )
        )
        manager.add_data(
            "equities",
            EquityStateReader().read(equities, exchange=Exchange.SSE),
        )
        manager.add_data(
            "1m",
            TradeBarReader().read(bars, exchange=Exchange.SSE),
        )

        slices = list(manager.stream())

        self.assertEqual(len(slices), 2)
        bootstrap, market = slices
        self.assertTrue(bootstrap.is_bootstrap)
        self.assertIsNone(bootstrap.time)
        self.assertFalse(market.is_bootstrap)
        bar = market.slice.get_bar("AAA", "1m")
        self.assertIs(bar, market.security_updates[0])
        self.assertIs(bar, market.valuation_updates[0].source)

        securities = SecurityManager()
        securities.on_timeslice(bootstrap)
        securities.on_timeslice(market)
        security = securities["AAA"]
        self.assertIsInstance(security, EquitySecurity)
        self.assertEqual(security.multiplier, 2)
        self.assertEqual(security.close, 11)

    def test_lifecycle_and_attribute_change_are_timed_state_updates(self):
        futures = pd.DataFrame([
            {
                "date": "2024-01-01",
                "instrument_id": "IF2401",
                "list_date": "2024-01-01",
                "delist_date": "2024-01-20",
                "multiplier": 300,
                "expiry": "2024-01-19",
            },
            {
                "date": "2024-01-10",
                "instrument_id": "IF2401",
                "list_date": "2024-01-01",
                "delist_date": "2024-01-20",
                "multiplier": 200,
                "expiry": "2024-01-19",
            },
        ])
        manager = DataManager(
            DataRoutingConfig(security_data_names={"futures"})
        )
        manager.add_data(
            "futures",
            FutureStateReader().read(futures, exchange=Exchange.CFFEX),
        )

        slices = list(manager.stream())

        self.assertEqual(
            [item.time for item in slices],
            [
                datetime(2024, 1, 1),
                datetime(2024, 1, 10),
                datetime(2024, 1, 20),
            ],
        )
        securities = SecurityManager()
        for item in slices:
            securities.on_timeslice(item)
        security = securities["IF2401"]
        self.assertIsInstance(security, FutureContract)
        self.assertEqual(security.multiplier, 200)
        self.assertFalse(security.is_active)
        self.assertFalse(security.is_tradable)


if __name__ == "__main__":
    unittest.main()
