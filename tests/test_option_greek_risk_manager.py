from datetime import datetime, timedelta
from types import SimpleNamespace

from autotrade.coreutils.object import OptionContract
from autotrade.option import (
    GreekRiskManager,
    OptionBacktestAnalyzer,
)


def _manager():
    security_manager = SimpleNamespace(
        securities={
            "linear": SimpleNamespace(price=100.0, close=100.0, multiplier=10.0)
        },
        get=lambda instrument_id: security_manager.securities.get(instrument_id),
    )
    oms = SimpleNamespace(
        positions={"linear": SimpleNamespace(volume=2.0, direction=None)},
        get_position=lambda instrument_id: oms.positions.get(instrument_id),
    )
    return GreekRiskManager(security_manager, oms), security_manager


def test_current_exposure_uses_latest_risk_record():
    manager, _ = _manager()
    analytics = SimpleNamespace(instrument_id="linear", delta=1.0)
    manager.update(analytics)
    state = manager.get("linear")

    assert state.security.price == 100.0
    assert state.analytics is analytics
    assert state.delta == 1.0
    assert state.unit_delta_exposure == 10.0
    assert state.unit_dollar_delta_1pct == 10.0
    assert manager.asset_exposure("linear").delta == 20.0
    assert manager.portfolio_exposure().delta == 20.0


def test_backtest_analyzer_uses_lagged_position_and_frozen_price():
    manager, securities = _manager()
    analyzer = OptionBacktestAnalyzer(manager)
    start = datetime(2024, 1, 1)
    manager.update(SimpleNamespace(instrument_id="linear", delta=1.0))
    analyzer.record(start)

    securities.securities["linear"].price = 105.0
    manager.update(SimpleNamespace(instrument_id="linear", delta=1.0))
    analyzer.record(start + timedelta(days=1), commission=3.0)

    attribution = analyzer.attributions[0]
    assert attribution.actual_pnl == 97.0
    assert attribution.greek_pnl["delta"] == 100.0
    assert attribution.approximate_pnl == 97.0


def test_backtest_analyzer_does_not_default_missing_option_delta():
    manager, securities = _manager()
    securities.securities["linear"] = OptionContract(
        instrument_id="linear", time=datetime(2024, 1, 1), value=10.0, multiplier=10.0
    )
    analyzer = OptionBacktestAnalyzer(manager)
    start = datetime(2024, 1, 1)
    analyzer.record(start)

    securities.securities["linear"].value = 11.0
    analyzer.record(start + timedelta(days=1))

    attribution = analyzer.attributions[0]
    assert not attribution.valid
    assert "linear:option_delta" in attribution.missing
    assert attribution.approximate_pnl is None
