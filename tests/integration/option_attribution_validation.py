"""Full-universe option Greek-attribution integration validation.

This is deliberately a *coverage strategy*, rather than a trade idea.  Once
per trading day it buys one contract of every currently available option that
is not already held; it closes a holding when fewer than three calendar days
remain to expiry.  Consequently every listed option has a continuous marked
P&L / Greek-P&L history over its investable lifetime.

The module is executable because it needs the local RQ data set.  It is kept
out of the ordinary unit-test suite: a complete MO run is an integration test
and writes several large, useful audit tables.
"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

from autotrade.backtest import BacktestEngine, BacktestEventEngine
from autotrade.backtest.data import (
    DataManager,
    DataRoutingConfig,
    OptionAnalyticsReader,
    OptionStateReader,
    TradeBarReader,
)
from autotrade.backtest.gateway import BacktestGateway, BacktestSettings
from autotrade.backtest.reporting import PerformanceAnalyzer
from autotrade.coreutils.constant import Direction, Exchange, Interval, OrderType
from autotrade.coreutils.object import OrderRequest
from autotrade.engine.event_engine import EVENT_SLICE
from autotrade.engine.security_manager import SecurityManager
from autotrade.option import GreekRiskManager, OptionBacktestAnalyzer, OptionBacktestReporting, OptionStrategy


DATA_ROOT = Path("/home/buzheng/Desktop/data/autotrade_rq")
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "results_all_option_attribution"

OPTION_INSTRUMENT_FIELDS = {
    "order_book_id": "instrument_id",
    "underlying_order_book_id": "underlying_instrument_id",
    "maturity_date": "expiry",
    "strike_price": "strike",
    "option_type": "right",
    "exercise_type": "style",
    "contract_multiplier": "multiplier",
}
OPTION_PRICE_FIELDS = {
    "order_book_id": "instrument_id",
    "date": "time",
    "total_turnover": "turnover",
}
OPTION_ANALYTICS_FIELDS = {
    "order_book_id": "instrument_id",
    "date": "time",
    "underlying_order_book_id": "underlying_instrument_id",
    "iv": "surface_iv",
}


def _rename(frame: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    return frame.rename(columns={old: new for old, new in mapping.items() if old in frame})


def _signed_volume(position) -> float:
    if position is None:
        return 0.0
    return float(position.volume) * (1.0 if position.direction == Direction.LONG else -1.0)


def load_option_frames(
    asset: str,
    *,
    start: str | None = None,
    end: str | None = None,
) -> tuple[dict[str, pd.DataFrame], Exchange]:
    """Load only the instrument, price and Greek data required by the test."""
    suffix = asset.upper()
    instruments = _rename(pd.read_pickle(DATA_ROOT / f"optioninstrument_{suffix}.pkl"), OPTION_INSTRUMENT_FIELDS)
    market = _rename(pd.read_pickle(DATA_ROOT / f"optionprice_{suffix}.pkl"), OPTION_PRICE_FIELDS)
    analytics = _rename(pd.read_pickle(DATA_ROOT / f"calculatedoptionGreeks_{suffix}.pkl"), OPTION_ANALYTICS_FIELDS)
    for frame in (market, analytics):
        frame["time"] = pd.to_datetime(frame["time"])
        if start is not None:
            frame.drop(frame.index[frame["time"] < pd.Timestamp(start)], inplace=True)
        if end is not None:
            frame.drop(frame.index[frame["time"] > pd.Timestamp(end)], inplace=True)

    # A mark without same-day analytics cannot be attributed, and analytics
    # without a mark cannot be traded or valued.  The inner join defines the
    # audit universe explicitly instead of silently forward-filling either.
    common = market.loc[:, ["time", "instrument_id"]].merge(
        analytics.loc[:, ["time", "instrument_id"]], how="inner",
        on=["time", "instrument_id"], validate="one_to_one",
    )
    market = market.merge(common, how="inner", on=["time", "instrument_id"])
    analytics = analytics.merge(common, how="inner", on=["time", "instrument_id"])
    ids = set(common["instrument_id"])
    instruments = instruments[instruments["instrument_id"].isin(ids)].copy()
    analytics["time_to_expiry"] = pd.to_numeric(analytics["t_days"], errors="coerce") / 365.0
    analytics["underlying_price"] = analytics["forward_price"]
    for frame in (market, analytics):
        frame.sort_values(["time", "instrument_id"], inplace=True, ignore_index=True)
    return {"option_instruments": instruments, "option_market": market, "option_analytics": analytics}, Exchange.CFFEX


def build_data_manager(frames: dict[str, pd.DataFrame], exchange: Exchange) -> DataManager:
    manager = DataManager(DataRoutingConfig(
        strategy_data_names={"option_market", "option_analytics"},
        security_data_names={"option_instruments", "option_market"},
        valuation_data_names={"option_market"},
    ))
    manager.add_data("option_instruments", OptionStateReader().read(frames["option_instruments"], exchange=exchange))
    manager.add_data("option_market", TradeBarReader().read(frames["option_market"], interval=Interval.K_DAY, exchange=exchange))
    manager.add_data("option_analytics", OptionAnalyticsReader().read(
        frames["option_analytics"], model_id="black97", model_version="cfutures_v1", exchange=exchange,
    ))
    return manager


class AllOptionRollStrategy(OptionStrategy):
    """Buy each eligible contract once and close it below the expiry threshold."""

    def __init__(self, *args, oms, quantity: int = 1, exit_days: int = 3, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.oms = oms
        self.quantity = int(quantity)
        self.exit_days = int(exit_days)
        self._last_decision_date = None
        self._retired: set[str] = set()

    def on_option_panel(self, panel, slice_) -> None:
        decision_date = pd.Timestamp(slice_.time).date()
        if decision_date == self._last_decision_date:
            return
        self._last_decision_date = decision_date

        held = {
            instrument_id: _signed_volume(self.oms.get_position(instrument_id))
            for instrument_id in panel.contracts
        }
        # Exit first.  A closed contract is retired permanently so it cannot
        # re-enter during its final two calendar days if it still has a quote.
        for instrument_id, quantity in held.items():
            if not quantity:
                continue
            expiry = panel.contracts[instrument_id].security.expiry
            days_left = (pd.Timestamp(expiry).date() - decision_date).days
            if days_left < self.exit_days:
                self._retired.add(instrument_id)
                self._submit(instrument_id, Direction.SHORT if quantity > 0 else Direction.LONG, abs(quantity), "expiry_exit")

        for instrument_id, view in panel.contracts.items():
            if held[instrument_id] or instrument_id in self._retired:
                continue
            expiry = view.security.expiry
            days_left = (pd.Timestamp(expiry).date() - decision_date).days
            # Deliberately do *not* screen on Greek completeness: this is a
            # data-quality test of the attribution system.  An unavailable
            # Delta/forward later appears as ``valid=False`` and a concrete
            # ``missing`` reason in the instrument-level PnL table.
            if days_left < self.exit_days:
                continue
            self._submit(instrument_id, Direction.LONG, self.quantity, "universe_entry")

    def _submit(self, instrument_id: str, direction: Direction, volume: float, reference: str) -> None:
        self.push_order_request(OrderRequest(
            instrument_id=instrument_id, exchange=self.security_manager.get(instrument_id).exchange,
            direction=direction, type=OrderType.MARKET, volume=volume, reference=reference,
        ))


class AttributionValidationGateway(BacktestGateway):
    """Snapshot active holdings at every valuation; trade events retain zeros."""

    option_analyzer: OptionBacktestAnalyzer | None = None

    def process_valuation(self, time_slice):
        recorded = super().process_valuation(time_slice)
        if recorded and self.option_analyzer is not None:
            active_ids = [
                instrument_id for instrument_id, position in self.option_analyzer.manager.oms.positions.items()
                if _signed_volume(position)
            ]
            self.option_analyzer.record(time_slice.slice.time, instrument_ids=active_ids)
        return recorded


def run_validation(
    asset: str = "MO",
    *,
    start: str | None = None,
    end: str | None = None,
) -> tuple[OptionBacktestReporting, pd.DataFrame]:
    frames, exchange = load_option_frames(asset, start=start, end=end)
    events = BacktestEventEngine()
    settings = BacktestSettings(cheat_on_close=True, market_fill_price="next_open", execution_data_name=None)
    securities = SecurityManager()
    securities.bind(events)
    initial_cash = 1_000_000_000.0
    gateway = AttributionValidationGateway("backtest", event_engine=events, settings=settings, security_manager=securities, initial_cash=initial_cash)
    engine = BacktestEngine(event_engine=events, initial_cash=initial_cash, risk_free=0.0, annual_days=252, settings=settings, security_manager=securities, gateway=gateway)
    risk_manager = GreekRiskManager(engine.security_manager, engine.oms, option_factor_price="forward")
    events.register(EVENT_SLICE, lambda event: risk_manager.on_slice(event.data))
    option_analyzer = OptionBacktestAnalyzer(risk_manager)
    option_analyzer.subscribe_trade_events(events)
    gateway.option_analyzer = option_analyzer
    reporting = OptionBacktestReporting(recorder=gateway.recorder, analyzer=PerformanceAnalyzer(initial_cash=initial_cash, risk_free=0.0, annual_days=252), oms=engine.oms, option_analyzer=option_analyzer)
    engine.reporting = reporting
    strategy = AllOptionRollStrategy(
        events, engine.security_manager, oms=engine.oms, quantity=1, exit_days=3,
    )
    strategy.initialize()
    engine.install(strategy)
    engine.run(build_data_manager(frames, exchange).stream())

    instrument_pnl = reporting.get_instrument_greek_pnl_df().reset_index()
    valid = instrument_pnl[instrument_pnl["valid"]].copy()
    summary = pd.DataFrame([{
        "asset": asset.upper(),
        "traded_instruments": int(len({trade.instrument_id for trade in engine.oms.trade_log})),
        "trade_count": len(engine.oms.trade_log),
        "attribution_intervals": len(instrument_pnl),
        "valid_intervals": len(valid),
        "invalid_intervals": int((~instrument_pnl["valid"]).sum()),
        "actual_pnl": float(instrument_pnl["actual_pnl"].sum()),
        "valid_actual_pnl": float(valid["actual_pnl"].sum()),
        "approximate_pnl": float(valid["approximate_pnl"].sum()),
        "residual_pnl": float(valid["residual_pnl"].sum()),
        "net_residual_over_abs_actual": float(valid["residual_pnl"].sum() / valid["actual_pnl"].abs().sum()) if not valid.empty and valid["actual_pnl"].abs().sum() else float("nan"),
        "absolute_residual_over_absolute_actual": float(valid["residual_pnl"].abs().sum() / valid["actual_pnl"].abs().sum()) if not valid.empty and valid["actual_pnl"].abs().sum() else float("nan"),
    }])
    return reporting, summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--asset", default="MO")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--start", help="inclusive YYYY-MM-DD; use this for a bounded audit run")
    parser.add_argument("--end", help="inclusive YYYY-MM-DD; use this for a bounded audit run")
    args = parser.parse_args()
    reporting, summary = run_validation(args.asset, start=args.start, end=args.end)
    output = args.output_dir / args.asset.upper()
    output.mkdir(parents=True, exist_ok=True)
    summary.to_csv(output / "validation_summary.csv", index=False)
    # Parquet preserves MultiIndex and is practical for the full universe;
    # these are precisely the four report tables requested for later analysis.
    reporting.get_position_cash_greeks_df(include_flat=True).to_parquet(output / "position_cash_greeks.parquet")
    reporting.get_instrument_greek_pnl_df().to_parquet(output / "instrument_greek_pnl.parquet")
    reporting.get_portfolio_cash_greeks_df().to_parquet(output / "portfolio_cash_greeks.parquet")
    reporting.get_portfolio_greek_pnl_df().to_parquet(output / "portfolio_greek_pnl.parquet")
    reporting.get_portfolio_greek_pnl_analysis_df().to_csv(output / "greek_pnl_analysis.csv")
    print(summary.to_string(index=False))
    print(f"wrote {output}")


if __name__ == "__main__":
    main()
