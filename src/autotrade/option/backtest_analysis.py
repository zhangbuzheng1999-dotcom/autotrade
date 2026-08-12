"""Historical snapshots and PnL attribution for option backtests."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping

from autotrade.coreutils.object import OptionContract

from .greek_risk_manager import (
    GREEKS,
    GreekRiskManager,
    GreekRiskState,
    _risk_field,
    _signed_position,
)


@dataclass(frozen=True)
class GreekRiskSnapshot:
    asof: datetime
    states: Mapping[str, GreekRiskState]
    positions: Mapping[str, float]
    commission_since_previous: float = 0.0


@dataclass(frozen=True)
class PnlAttribution:
    start: datetime
    end: datetime
    actual_pnl: float
    commission: float
    greek_pnl: Mapping[str, float | None]
    approximate_pnl: float | None
    residual_pnl: float | None
    valid: bool
    missing: tuple[str, ...]


class OptionBacktestAnalyzer:
    """Freeze manager state at valuation points and explain each interval PnL."""

    def __init__(self, manager: GreekRiskManager) -> None:
        self.manager = manager
        self.snapshots: list[GreekRiskSnapshot] = []
        self.attributions: list[PnlAttribution] = []

    def record(self, asof: datetime, *, commission: float = 0.0) -> GreekRiskSnapshot:
        """Freeze current risk states and OMS positions; explain prior interval."""
        snapshot = GreekRiskSnapshot(
            asof=asof,
            states={
                instrument_id: deepcopy(self.manager.get(instrument_id))
                for instrument_id in self.manager.oms.positions
            },
            positions={instrument_id: _signed_position(position) for instrument_id, position in self.manager.oms.positions.items()},
            commission_since_previous=float(commission),
        )
        if self.snapshots:
            self.attributions.append(self.pnl_attribution(self.snapshots[-1], snapshot))
        self.snapshots.append(snapshot)
        return snapshot

    @staticmethod
    def pnl_attribution(previous: GreekRiskSnapshot, current: GreekRiskSnapshot) -> PnlAttribution:
        """Apply lagged-position first/second-order Greek PnL attribution."""
        components = {greek: 0.0 for greek in GREEKS}
        actual = 0.0
        missing: list[str] = []
        dt_year = (current.asof - previous.asof).total_seconds() / (365.0 * 24.0 * 60.0 * 60.0)

        for instrument_id, quantity in previous.positions.items():
            if not quantity:
                continue
            start, end = previous.states.get(instrument_id), current.states.get(instrument_id)
            if start is None or end is None or start.security is None:
                missing.append(f"{instrument_id}:state_or_multiplier")
                continue
            multiplier = start.multiplier
            start_price = start.price
            end_price = end.price
            if multiplier is None:
                missing.append(f"{instrument_id}:multiplier")
                continue
            scale = quantity * float(multiplier)
            if start_price is None or end_price is None:
                missing.append(f"{instrument_id}:price")
            else:
                actual += scale * (float(end_price) - float(start_price))

            # Options use their forward movement where available.  A linear
            # asset naturally falls back to its own price movement.
            start_data, end_data = start.analytics, end.analytics
            start_factor, end_factor = start.driver_price, end.driver_price
            # A missing risk record means a non-option asset was not relevant
            # to strategy risk calculation; use the conventional linear
            # fallback only for such assets.  An option without a Delta is a
            # missing model input and must invalidate its Greek explanation.
            if start.delta is None and isinstance(start.security, OptionContract):
                missing.append(f"{instrument_id}:option_delta")
                continue
            delta = 1.0 if start.delta is None else start.delta
            if start_factor is None or end_factor is None:
                missing.append(f"{instrument_id}:delta_input")
                continue
            d_factor = end_factor - start_factor
            components["delta"] += scale * delta * d_factor
            start_iv, end_iv = _risk_field(start_data, "surface_iv"), _risk_field(end_data, "surface_iv")
            start_rate, end_rate = _risk_field(start_data, "risk_free_rate"), _risk_field(end_data, "risk_free_rate")
            gamma = _risk_field(start_data, "gamma")
            vega = _risk_field(start_data, "vega")
            theta = _risk_field(start_data, "theta")
            rho = _risk_field(start_data, "rho")
            vanna = _risk_field(start_data, "vanna")
            vomma = _risk_field(start_data, "vomma")
            charm = _risk_field(start_data, "charm")
            if gamma is not None: components["gamma"] += 0.5 * scale * gamma * d_factor**2
            if vega is not None and start_iv is not None and end_iv is not None: components["vega"] += scale * vega * (end_iv - start_iv)
            if theta is not None: components["theta"] += scale * theta * dt_year
            if rho is not None and start_rate is not None and end_rate is not None: components["rho"] += scale * rho * (end_rate - start_rate)
            if vanna is not None and start_iv is not None and end_iv is not None: components["vanna"] += scale * vanna * d_factor * (end_iv - start_iv)
            if vomma is not None and start_iv is not None and end_iv is not None: components["vomma"] += 0.5 * scale * vomma * (end_iv - start_iv) ** 2
            if charm is not None: components["charm"] += scale * charm * d_factor * dt_year

        commission = current.commission_since_previous
        actual -= commission
        valid = not missing
        approximate = sum(components.values()) - commission if valid else None
        return PnlAttribution(
            previous.asof, current.asof, actual, commission, components,
            approximate, None if approximate is None else actual - approximate,
            valid, tuple(sorted(set(missing))),
        )


__all__ = ["GreekRiskSnapshot", "OptionBacktestAnalyzer", "PnlAttribution"]
