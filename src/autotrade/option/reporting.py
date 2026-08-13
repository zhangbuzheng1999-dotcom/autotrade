"""Option-specific tables layered on top of standard backtest reporting."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd

from autotrade.backtest.reporting import BacktestReporting
from autotrade.coreutils.object import OptionContract

from .backtest_analysis import OptionBacktestAnalyzer
from .greek_risk_manager import GREEKS, _risk_field


class OptionBacktestReporting(BacktestReporting):
    """Add frozen option structure, portfolio risk and PnL-attribution tables.

    ``OptionBacktestAnalyzer.record`` remains deliberately explicit: callers
    record it at the same valuation points used by the strategy.  This class
    only turns those immutable snapshots into report-friendly DataFrames.
    """

    def __init__(self, *, option_analyzer: OptionBacktestAnalyzer, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.option_analyzer = option_analyzer

    def calculate(self, *, print_result: bool = True) -> dict:
        result = dict(super().calculate(print_result=print_result))
        result["option_strategy"] = self.get_option_strategy_summary()
        self.result = result
        return result

    def get_option_position_df(self, *, include_flat: bool = False) -> pd.DataFrame:
        """Return option contract terms, positions, price and model inputs by snapshot."""
        records: list[dict[str, Any]] = []
        for instrument_id, snapshots in self.option_analyzer.instrument_snapshots.items():
            for snapshot in snapshots:
                state = snapshot.state
                security = state.security
                if not isinstance(security, OptionContract):
                    continue
                quantity = snapshot.position
                if not include_flat and not quantity:
                    continue
                analytics = state.analytics
                records.append({
                    "asof": snapshot.asof,
                    "instrument_id": instrument_id,
                    "quantity": quantity,
                    "multiplier": state.multiplier,
                    "price": state.price,
                    "market_value": None if state.price is None or state.multiplier is None else quantity * state.multiplier * state.price,
                    "underlying_instrument_id": security.underlying_instrument_id,
                    "expiry": security.expiry,
                    "strike": security.strike,
                    "right": security.right,
                    "style": security.style,
                    "forward_price": state.forward_price,
                    "underlying_price": _risk_field(analytics, "underlying_price"),
                    "time_to_expiry": _risk_field(analytics, "time_to_expiry"),
                    "market_iv": _risk_field(analytics, "market_iv"),
                    "surface_iv": _risk_field(analytics, "surface_iv"),
                    "risk_free_rate": _risk_field(analytics, "risk_free_rate"),
                    # Exposure, rather than per-contract raw model Greeks.
                    **{
                        greek: None if state.greek(greek) is None or state.multiplier is None
                        else quantity * state.multiplier * state.greek(greek)
                        for greek in GREEKS
                    },
                    "delta_notional": None if state.delta is None or state.multiplier is None or state.driver_price is None
                    else quantity * state.multiplier * state.delta * state.driver_price,
                    "delta_pnl_1pct": None if state.delta is None or state.multiplier is None or state.driver_price is None
                    else quantity * state.multiplier * state.delta * state.driver_price * 0.01,
                })
        return self._frame(records, self._position_columns()).set_index(
            ["asof", "instrument_id"], drop=True,
        )

    def get_position_greeks_df(self, *, include_flat: bool = False) -> pd.DataFrame:
        """MultiIndex ``(date, instrument_id)`` position Greek exposures."""
        return self.get_option_position_df(include_flat=include_flat)

    def get_portfolio_greek_df(self) -> pd.DataFrame:
        """Aggregate only facts occurring at the same event timestamp."""
        return self.get_portfolio_greeks_df()

    def get_portfolio_greeks_df(self) -> pd.DataFrame:
        """Daily portfolio Greek exposure, aggregated from position-level facts."""
        positions = self.get_position_greeks_df()
        if positions.empty:
            return pd.DataFrame(
                columns=(*GREEKS, "delta_notional", "delta_pnl_1pct"),
                index=pd.Index([], name="date"),
            )
        columns = (*GREEKS, "delta_notional", "delta_pnl_1pct")
        totals = positions.loc[:, list(columns)].groupby(level="asof").sum(min_count=1)
        totals.index.name = "date"
        return totals

    def get_greek_attribution_df(self) -> pd.DataFrame:
        """Compatibility alias for reporter-side, exact-timestamp PnL aggregation."""
        return self.get_portfolio_greek_pnl_df().reset_index()

    def get_instrument_greek_pnl_df(self) -> pd.DataFrame:
        """MultiIndex ``(date, instrument_id)`` Greek PnL, dated at interval end."""
        records = [
            {
                "date": attribution.end,
                "start": attribution.start,
                "instrument_id": attribution.instrument_id,
                "actual_pnl": attribution.actual_pnl,
                **{greek: attribution.greek_pnl.get(greek) for greek in GREEKS},
                "approximate_pnl": attribution.approximate_pnl,
                "residual_pnl": attribution.residual_pnl,
                "valid": attribution.valid,
                "missing": attribution.missing,
            }
            for attribution in self.option_analyzer.instrument_attributions
        ]
        return self._frame(
            records,
            ("date", "start", "instrument_id", "actual_pnl", *GREEKS, "approximate_pnl", "residual_pnl", "valid", "missing"),
        ).set_index(["date", "instrument_id"], drop=True)

    def get_portfolio_greek_pnl_df(self) -> pd.DataFrame:
        """Daily portfolio Greek PnL aggregated from instrument attributions."""
        pnl = self.get_instrument_greek_pnl_df()
        columns = ("actual_pnl", *GREEKS, "approximate_pnl", "residual_pnl")
        if pnl.empty:
            return pd.DataFrame(columns=columns, index=pd.Index([], name="date"))
        totals = pnl.loc[:, list(columns)].groupby(level="date").sum(min_count=1)
        totals["valid"] = pnl["valid"].groupby(level="date").all()
        totals["missing"] = pnl["missing"].groupby(level="date").agg(
            lambda values: tuple(sorted({item for group in values for item in group})),
        )
        return totals

    def get_portfolio_greek_pnl_analysis_df(self) -> pd.DataFrame:
        """Summarize start-of-period risk exposures and their Greek PnL.

        Each PnL interval is paired with its own *start* snapshot, not the
        post-trade/end snapshot.  This matters because attribution is based on
        lagged positions and Greeks.  No as-of fill is performed: an interval
        without its exact start risk snapshot is excluded, and a whole end
        timestamp is excluded if any constituent attribution is invalid.
        """
        positions = self.get_position_greeks_df(include_flat=True).reset_index()
        instrument_pnl = self.get_instrument_greek_pnl_df().reset_index()
        columns = (
            "observation_count", "avg_start_exposure", "avg_abs_start_exposure",
            "max_abs_start_exposure", "total_pnl", "avg_pnl", "pnl_std",
            "positive_pnl_ratio", "pnl_per_avg_abs_start_exposure",
            "avg_delta_notional", "avg_delta_pnl_1pct",
        )
        if positions.empty or instrument_pnl.empty:
            return pd.DataFrame(columns=columns, index=pd.Index([], name="greek"))

        valid_dates = instrument_pnl.groupby("date")["valid"].all()
        valid_pnl = instrument_pnl[
            instrument_pnl["date"].isin(valid_dates[valid_dates].index)
        ]
        paired = valid_pnl.merge(
            positions,
            how="inner",
            left_on=["start", "instrument_id"],
            right_on=["asof", "instrument_id"],
            suffixes=("_pnl", "_exposure"),
        )
        if paired.empty:
            return pd.DataFrame(columns=columns, index=pd.Index([], name="greek"))

        # One row per portfolio PnL end time.  Exposure columns are summed
        # from the start snapshots of exactly the instruments being attributed.
        exposure_columns = [f"{greek}_exposure" for greek in GREEKS]
        exposure_columns.extend(["delta_notional", "delta_pnl_1pct"])
        pnl_columns = [f"{greek}_pnl" for greek in GREEKS]
        periods = paired.groupby("date")[exposure_columns + pnl_columns].sum(min_count=1)

        records: list[dict[str, Any]] = []
        for greek in GREEKS:
            exposure = periods[f"{greek}_exposure"]
            pnl = periods[f"{greek}_pnl"]
            avg_abs = float(exposure.abs().mean())
            records.append({
                "greek": greek,
                "observation_count": int(pnl.count()),
                "avg_start_exposure": float(exposure.mean()),
                "avg_abs_start_exposure": avg_abs,
                "max_abs_start_exposure": float(exposure.abs().max()),
                "total_pnl": float(pnl.sum()),
                "avg_pnl": float(pnl.mean()),
                "pnl_std": float(pnl.std()),
                "positive_pnl_ratio": float((pnl > 0).mean()),
                "pnl_per_avg_abs_start_exposure": None if not avg_abs else float(pnl.sum() / avg_abs),
                "avg_delta_notional": float(periods["delta_notional"].mean()) if greek == "delta" else None,
                "avg_delta_pnl_1pct": float(periods["delta_pnl_1pct"].mean()) if greek == "delta" else None,
            })
        return pd.DataFrame.from_records(records).set_index("greek")

    def get_option_strategy_summary(self) -> dict[str, Any]:
        """Summarize the strategy's option universe and valid Greek attribution."""
        positions = self.get_option_position_df(include_flat=True)
        attribution = self.get_instrument_greek_pnl_df()
        option_instruments = sorted(positions.index.get_level_values("instrument_id").unique()) if not positions.empty else []
        valid = attribution[attribution["valid"]] if not attribution.empty else attribution
        greek_pnl = {
            greek: float(valid[greek].sum()) if not valid.empty else 0.0
            for greek in GREEKS
        }
        return {
            "snapshot_count": len(self.option_analyzer.snapshots),
            "option_instrument_count": len(option_instruments),
            "option_instruments": option_instruments,
            "underlyings": self._unique_values(positions, "underlying_instrument_id"),
            "rights": self._unique_values(positions, "right"),
            "styles": self._unique_values(positions, "style"),
            "attribution_count": len(attribution),
            "valid_attribution_count": len(valid),
            "greek_pnl": greek_pnl,
            "actual_pnl": float(attribution["actual_pnl"].sum()) if not attribution.empty else 0.0,
            "commission": 0.0,
            "approximate_pnl": float(valid["approximate_pnl"].sum()) if not valid.empty else 0.0,
            "residual_pnl": float(valid["residual_pnl"].sum()) if not valid.empty else 0.0,
        }

    def _export_frames(self) -> dict[str, pd.DataFrame]:
        frames = super()._export_frames()
        frames.update({
            "position_greeks": self.get_position_greeks_df(include_flat=True),
            "instrument_greek_pnl": self.get_instrument_greek_pnl_df(),
            "portfolio_greeks": self.get_portfolio_greeks_df(),
            "portfolio_greek_pnl": self.get_portfolio_greek_pnl_df(),
            "greek_pnl_analysis": self.get_portfolio_greek_pnl_analysis_df(),
        })
        return frames

    @staticmethod
    def _frame(records: Iterable[dict[str, Any]], columns: Iterable[str]) -> pd.DataFrame:
        return pd.DataFrame.from_records(records, columns=list(columns))

    @staticmethod
    def _unique_values(frame: pd.DataFrame, column: str) -> list[Any]:
        if frame.empty:
            return []
        return sorted(value for value in frame[column].dropna().unique())

    @staticmethod
    def _position_columns() -> tuple[str, ...]:
        return (
            "asof", "instrument_id", "quantity", "multiplier", "price", "market_value",
            "underlying_instrument_id", "expiry", "strike", "right", "style",
            "forward_price", "underlying_price", "time_to_expiry", "market_iv",
            "surface_iv", "risk_free_rate", *GREEKS, "delta_notional", "delta_pnl_1pct",
        )


__all__ = ["OptionBacktestReporting"]
