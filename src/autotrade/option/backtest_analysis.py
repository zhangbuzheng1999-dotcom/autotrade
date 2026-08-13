"""Historical snapshots and PnL attribution for option backtests."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from collections.abc import Iterable
from typing import Mapping

from autotrade.coreutils.object import OptionContract
from autotrade.coreutils.object import TradeData
from autotrade.coreutils.constant import Direction
from autotrade.engine.event_engine import EVENT_TRADE, Event, EventEngine

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
class InstrumentGreekRiskSnapshot:
    """One asset's immutable risk state at an event-driven valuation time."""

    asof: datetime
    instrument_id: str
    state: GreekRiskState
    position: float


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


@dataclass(frozen=True)
class InstrumentPnlAttribution:
    """Greek PnL explanation for one lagged position over one interval."""

    start: datetime
    end: datetime
    instrument_id: str
    actual_pnl: float
    greek_pnl: Mapping[str, float | None]
    approximate_pnl: float | None
    residual_pnl: float | None
    valid: bool
    missing: tuple[str, ...]


class OptionBacktestAnalyzer:
    """Record independent per-instrument risk and attribution time series.

    This is a facts-only layer: it does not align asset clocks or calculate a
    portfolio.  Consumers such as reporters decide how to aggregate the
    irregular event timestamps.
    """

    def __init__(self, manager: GreekRiskManager) -> None:
        self.manager = manager
        # ``snapshots`` is retained as a record-batch audit trail for callers
        # that previously consumed it.  Per-asset time series below are the
        # canonical source for all new reporting and attribution work.
        self.snapshots: list[GreekRiskSnapshot] = []
        self.instrument_snapshots: dict[str, list[InstrumentGreekRiskSnapshot]] = {}
        self.instrument_attributions: list[InstrumentPnlAttribution] = []
        self.instrument_attributions_by_instrument: dict[str, list[InstrumentPnlAttribution]] = {}
        self._recorded_trade_ids: set[str] = set()
        # Deprecated compatibility history.  Portfolio aggregation belongs in
        # a reporter, which owns the timestamp alignment policy.
        self.attributions: list[PnlAttribution] = []

    def record(
        self,
        asof: datetime,
        *,
        instrument_ids: Iterable[str] | None = None,
        commission: float = 0.0,
    ) -> GreekRiskSnapshot:
        """Record only the assets that actually have a valuation/event.

        Without ``instrument_ids`` the current OMS holdings are recorded for
        backward compatibility.  Event-driven callers should pass the changed
        asset ids, so each asset naturally owns an independent time series.
        ``commission`` remains on the returned batch audit record; allocating
        it to asset PnL is a trade/reporting policy and is intentionally not
        guessed here.
        """
        ids = tuple(dict.fromkeys(
            str(instrument_id)
            for instrument_id in (self.manager.oms.positions if instrument_ids is None else instrument_ids)
        ))
        snapshot = GreekRiskSnapshot(
            asof=asof,
            states={
                instrument_id: deepcopy(self.manager.get(instrument_id))
                for instrument_id in ids
            },
            positions={
                instrument_id: _signed_position(self.manager.oms.get_position(instrument_id))
                for instrument_id in ids
            },
            commission_since_previous=float(commission),
        )
        self.snapshots.append(snapshot)
        for instrument_id in ids:
            current = InstrumentGreekRiskSnapshot(
                asof, instrument_id, snapshot.states[instrument_id], snapshot.positions[instrument_id],
            )
            history = self.instrument_snapshots.setdefault(instrument_id, [])
            if history:
                attribution = self._instrument_pnl_attribution(history[-1], current)
                self.instrument_attributions.append(attribution)
                self.instrument_attributions_by_instrument.setdefault(instrument_id, []).append(attribution)
            history.append(current)
        return snapshot

    def subscribe_trade_events(self, event_engine: EventEngine) -> None:
        """Subscribe after OMS to snapshot the authoritative post-fill position.

        Construct ``OmsBase`` before calling this method.  Event handlers are
        dispatched in registration order, so OMS then projects the fill before
        this analyzer reads it.  A duplicate broker ``tradeid`` is ignored,
        matching OMS's idempotency rule.
        """
        event_engine.register(EVENT_TRADE, self.process_trade_event)

    def unsubscribe_trade_events(self, event_engine: EventEngine) -> None:
        event_engine.unregister(EVENT_TRADE, self.process_trade_event)

    def process_trade_event(self, event: Event) -> None:
        """Record the traded asset at the fill timestamp, after OMS updates."""
        trade = event.data
        if not isinstance(trade, TradeData):
            return
        if not trade.tradeid or trade.tradeid in self._recorded_trade_ids:
            return
        if trade.direction not in {Direction.LONG, Direction.SHORT}:
            return
        # If subscribed after OMS, an ignored/invalid fill never enters the
        # authoritative trade log and must not create a risk snapshot.
        if self.manager.oms.get_trade(trade.tradeid) is not trade:
            return
        if trade.datetime is None:
            raise ValueError("trade-event snapshots require TradeData.datetime")
        self._recorded_trade_ids.add(trade.tradeid)
        self.record(trade.datetime, instrument_ids=[trade.instrument_id])

    @staticmethod
    def _instrument_pnl_attribution(
        previous: InstrumentGreekRiskSnapshot, current: InstrumentGreekRiskSnapshot,
    ) -> InstrumentPnlAttribution:
        """Explain one asset over its own adjacent event snapshots."""
        if previous.instrument_id != current.instrument_id:
            raise ValueError("an instrument attribution requires matching instrument ids")
        instrument_id, quantity = previous.instrument_id, previous.position
        dt_year = (current.asof - previous.asof).total_seconds() / (365.0 * 24.0 * 60.0 * 60.0)
        components = {greek: 0.0 for greek in GREEKS}
        actual = 0.0
        missing: list[str] = []
        start, end = previous.state, current.state
        if not quantity:
            return InstrumentPnlAttribution(previous.asof, current.asof, instrument_id, 0.0, components, 0.0, 0.0, True, ())
        if start.security is None:
            missing.append(f"{instrument_id}:state_or_multiplier")
        else:
            multiplier = start.multiplier
            start_price, end_price = start.price, end.price
            if multiplier is None:
                missing.append(f"{instrument_id}:multiplier")
            else:
                scale = quantity * float(multiplier)
                if start_price is None or end_price is None:
                    missing.append(f"{instrument_id}:price")
                else:
                    actual = scale * (float(end_price) - float(start_price))
                start_data, end_data = start.analytics, end.analytics
                start_factor, end_factor = start.driver_price, end.driver_price
                if start.delta is None and isinstance(start.security, OptionContract):
                    missing.append(f"{instrument_id}:option_delta")
                elif start_factor is None or end_factor is None:
                    missing.append(f"{instrument_id}:delta_input")
                else:
                    delta = 1.0 if start.delta is None else start.delta
                    d_factor = end_factor - start_factor
                    components["delta"] = scale * delta * d_factor
                    start_iv, end_iv = _risk_field(start_data, "surface_iv"), _risk_field(end_data, "surface_iv")
                    start_rate, end_rate = _risk_field(start_data, "risk_free_rate"), _risk_field(end_data, "risk_free_rate")
                    gamma, vega, theta = (_risk_field(start_data, name) for name in ("gamma", "vega", "theta"))
                    rho, vanna, vomma, charm = (_risk_field(start_data, name) for name in ("rho", "vanna", "vomma", "charm"))
                    if gamma is not None: components["gamma"] = 0.5 * scale * gamma * d_factor**2
                    if vega is not None and start_iv is not None and end_iv is not None: components["vega"] = scale * vega * (end_iv - start_iv)
                    if theta is not None: components["theta"] = scale * theta * dt_year
                    if rho is not None and start_rate is not None and end_rate is not None: components["rho"] = scale * rho * (end_rate - start_rate)
                    if vanna is not None and start_iv is not None and end_iv is not None: components["vanna"] = scale * vanna * d_factor * (end_iv - start_iv)
                    if vomma is not None and start_iv is not None and end_iv is not None: components["vomma"] = 0.5 * scale * vomma * (end_iv - start_iv) ** 2
                    if charm is not None: components["charm"] = scale * charm * d_factor * dt_year
        valid = not missing
        approximate = sum(components.values()) if valid else None
        return InstrumentPnlAttribution(previous.asof, current.asof, instrument_id, actual, components, approximate, None if approximate is None else actual - approximate, valid, tuple(sorted(set(missing))))

    @staticmethod
    def aggregate_pnl_attribution(
        previous: GreekRiskSnapshot,
        current: GreekRiskSnapshot,
        instrument_attributions: list[InstrumentPnlAttribution],
    ) -> PnlAttribution:
        """Aggregate already-calculated instrument attributions; never reprice here."""
        components = {greek: sum(item.greek_pnl[greek] or 0.0 for item in instrument_attributions) for greek in GREEKS}
        commission = current.commission_since_previous
        actual = sum(item.actual_pnl for item in instrument_attributions) - commission
        missing = tuple(sorted({reason for item in instrument_attributions for reason in item.missing}))
        valid = not missing
        approximate = sum(components.values()) - commission if valid else None
        return PnlAttribution(
            previous.asof, current.asof, actual, commission, components,
            approximate, None if approximate is None else actual - approximate,
            valid, tuple(sorted(set(missing))),
        )

    @staticmethod
    def pnl_attribution(previous: GreekRiskSnapshot, current: GreekRiskSnapshot) -> PnlAttribution:
        """Deprecated compatibility helper for callers with batch snapshots.

        New event-driven code must consume ``instrument_attributions`` and let
        its reporter define the portfolio timestamp alignment.
        """
        items = []
        for instrument_id, quantity in previous.positions.items():
            start, end = previous.states.get(instrument_id), current.states.get(instrument_id)
            if start is None or end is None:
                continue
            items.append(OptionBacktestAnalyzer._instrument_pnl_attribution(
                InstrumentGreekRiskSnapshot(previous.asof, instrument_id, start, quantity),
                InstrumentGreekRiskSnapshot(current.asof, instrument_id, end, current.positions.get(instrument_id, 0.0)),
            ))
        return OptionBacktestAnalyzer.aggregate_pnl_attribution(
            previous, current, items,
        )


__all__ = ["GreekRiskSnapshot", "InstrumentGreekRiskSnapshot", "InstrumentPnlAttribution", "OptionBacktestAnalyzer", "PnlAttribution"]
