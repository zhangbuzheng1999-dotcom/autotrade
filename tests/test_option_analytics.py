import unittest
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from autotrade.backtest.data import (
    DataManager,
    DataRoutingConfig,
    OptionAnalyticsReader,
)
from autotrade.backtest.event_engine import BacktestEventEngine
from autotrade.coreutils.constant import Exchange
from autotrade.coreutils.object import (
    InstrumentStateData,
    OptionAnalyticsData,
    OptionContract,
    OptionStateData,
    Slice,
    TradeBar,
)
from autotrade.engine.security_manager import SecurityManager
from autotrade.strategy.option_strategy import (
    OptionPanelAssembler,
    OptionStrategy,
)


@dataclass(slots=True)
class ExtendedOptionAnalyticsData(OptionAnalyticsData):
    custom_greek: float | None = None


class OptionAnalyticsTests(unittest.TestCase):
    def test_reader_routes_analytics_only_to_strategy_slice(self):
        frame = pd.DataFrame(
            [
                {
                    "order_book_id": "MO2401-C-5000",
                    "date": "2024-01-02",
                    "underlying_order_book_id": "000852.XSHG",
                    "underlying_close": 5000,
                    "forward_price": 5010,
                    "r": 0.02,
                    "time_to_expiry": 20 / 365,
                    "iv": 0.22,
                    "delta": 0.51,
                    "vomma": 10.5,
                }
            ]
        )
        reader = OptionAnalyticsReader(
            schema={
                "underlying_instrument_id": "underlying_order_book_id",
            }
        )
        manager = DataManager(
            DataRoutingConfig(
                strategy_data_names={"mo_analytics"},
            )
        )
        manager.add_data(
            "mo_analytics",
            reader.read(
                frame,
                model_id="black76_grid",
                model_version="v1",
                exchange=Exchange.CFFEX,
            ),
        )

        time_slice = next(manager.stream())
        analytics = time_slice.slice.option_analytics["mo_analytics"][
            "MO2401-C-5000"
        ]

        self.assertEqual(analytics.surface_iv, 0.22)
        self.assertEqual(analytics.delta, 0.51)
        self.assertEqual(analytics.vomma, 10.5)
        self.assertEqual(analytics.model_id, "black76_grid")
        self.assertFalse(time_slice.security_updates)
        self.assertFalse(time_slice.valuation_updates)

    def test_router_rejects_analytics_as_security_state(self):
        frame = pd.DataFrame(
            [
                {
                    "instrument_id": "MO-C",
                    "time": "2024-01-02",
                    "delta": 0.5,
                }
            ]
        )
        manager = DataManager(
            DataRoutingConfig(
                strategy_data_names={"analytics"},
                security_data_names={"analytics"},
            )
        )
        manager.add_data(
            "analytics",
            OptionAnalyticsReader().read(
                frame,
                model_id="black76",
                model_version="v1",
            ),
        )

        with self.assertRaisesRegex(
            ValueError,
            "cannot route to security",
        ):
            next(manager.stream())

    def test_analytics_requires_model_identity_and_finite_values(self):
        with self.assertRaisesRegex(ValueError, "model_id"):
            OptionAnalyticsData(
                instrument_id="MO-C",
                time=datetime(2024, 1, 2),
                model_version="v1",
            )
        with self.assertRaisesRegex(ValueError, "delta must be finite"):
            OptionAnalyticsData(
                instrument_id="MO-C",
                time=datetime(2024, 1, 2),
                delta=float("nan"),
                model_id="black76",
                model_version="v1",
            )

    def test_assembler_joins_multiple_underlyings_without_scanning_securities(self):
        when = datetime(2024, 1, 2)
        securities = SecurityManager()
        securities.on_data(
            OptionStateData(
                instrument_id="MO-C",
                time=None,
                is_active=True,
                underlying_instrument_id="000852.XSHG",
                strike=5000,
                right="C",
            )
        )
        securities.on_data(
            OptionStateData(
                instrument_id="IO-P",
                time=None,
                is_active=True,
                underlying_instrument_id="000300.XSHG",
                strike=3500,
                right="P",
            )
        )
        securities.on_data(
            InstrumentStateData(
                instrument_id="IF2401",
                time=None,
                is_active=True,
            )
        )
        for instrument_id, close in (("MO-C", 120), ("IO-P", 80)):
            securities.on_data(
                TradeBar(
                    instrument_id=instrument_id,
                    time=when,
                    open=close,
                    high=close,
                    low=close,
                    close=close,
                )
            )
        analytics = {
            "MO-C": OptionAnalyticsData(
                instrument_id="MO-C",
                time=when,
                delta=0.5,
                model_id="black76",
                model_version="v1",
            ),
            "IO-P": OptionAnalyticsData(
                instrument_id="IO-P",
                time=when,
                delta=-0.4,
                model_id="black76",
                model_version="v1",
            ),
        }

        panel = OptionPanelAssembler.build(securities, analytics)

        self.assertIsNotNone(panel)
        self.assertFalse(hasattr(panel, "time"))
        self.assertEqual(set(panel.contracts), {"MO-C", "IO-P"})
        self.assertIsInstance(panel.contracts["MO-C"].security, OptionContract)
        frame = panel.to_frame()
        self.assertEqual(frame.loc["MO-C", "close"], 120)
        self.assertEqual(frame.loc["IO-P", "delta"], -0.4)
        self.assertNotIn("IF2401", frame.index)

    def test_assembler_only_joins_and_does_not_own_time_semantics(self):
        securities = SecurityManager()
        for instrument_id in ("MO-C", "IO-P"):
            securities.on_data(
                OptionStateData(
                    instrument_id=instrument_id,
                    time=None,
                    is_active=True,
                )
            )
        analytics = {
            "MO-C": OptionAnalyticsData(
                instrument_id="MO-C",
                time=datetime(2024, 1, 2),
                model_id="black76",
                model_version="v1",
            ),
            "IO-P": OptionAnalyticsData(
                instrument_id="IO-P",
                time=datetime(2024, 1, 3),
                model_id="black76",
                model_version="v1",
            ),
        }

        panel = OptionPanelAssembler.build(securities, analytics)

        self.assertEqual(set(panel.contracts), {"MO-C", "IO-P"})
        self.assertFalse(hasattr(panel, "time"))

    def test_to_frame_discovers_security_and_analytics_fields(self):
        securities = SecurityManager()
        securities.on_data(
            OptionStateData(
                instrument_id="MO-C",
                time=None,
                is_active=True,
                margin_rate=0.12,
            )
        )
        analytics = {
            "MO-C": ExtendedOptionAnalyticsData(
                instrument_id="MO-C",
                time=datetime(2024, 1, 2),
                custom_greek=3.5,
                model_id="black76",
                model_version="v2",
            )
        }

        panel = OptionPanelAssembler.build(securities, analytics)
        frame = panel.to_frame()

        self.assertEqual(frame.loc["MO-C", "margin_rate"], 0.12)
        self.assertEqual(frame.loc["MO-C", "custom_greek"], 3.5)
        self.assertIn("price", frame.columns)

    def test_option_strategy_only_assembles_when_configured_analytics_arrive(self):
        when = datetime(2024, 1, 2)
        events = BacktestEventEngine()
        securities = SecurityManager(events)
        securities.on_data(
            OptionStateData(
                instrument_id="MO-C",
                time=None,
                is_active=True,
                strike=5000,
                right="C",
            )
        )
        strategy = _CapturingOptionStrategy(events, securities)

        strategy.on_data(Slice(time=when))
        self.assertEqual(strategy.panels, [])

        analytics = OptionAnalyticsData(
            instrument_id="MO-C",
            time=when,
            delta=0.5,
            model_id="black76",
            model_version="v1",
        )
        strategy.on_data(
            Slice.from_named_data(
                when,
                [("mo_analytics", analytics)],
            )
        )

        self.assertEqual(len(strategy.panels), 1)
        self.assertIn("MO-C", strategy.panels[0].contracts)

    def test_legacy_option_contract_greeks_and_option_chain_are_removed(self):
        contract = OptionContract(
            instrument_id="MO-C",
            time=datetime(2024, 1, 2),
        )
        self.assertFalse(hasattr(contract, "iv"))
        self.assertFalse(hasattr(contract, "delta"))
        self.assertFalse(hasattr(Slice(time=None), "option_chains"))


class _CapturingOptionStrategy(OptionStrategy):
    def __init__(self, event_engine, security_manager):
        super().__init__(
            event_engine,
            security_manager,
            option_analytics_data_name="mo_analytics",
        )
        self.panels = []

    def on_option_panel(self, panel, slice_):
        self.panels.append(panel)


if __name__ == "__main__":
    unittest.main()
