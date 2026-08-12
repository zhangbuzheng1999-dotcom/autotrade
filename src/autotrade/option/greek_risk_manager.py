"""Latest per-instrument Greek risk state for option portfolios.

This module deliberately does not alter ``Security``.  ``SecurityManager``
remains authoritative for prices and contract metadata; this manager keeps the
latest risk record supplied in an option-analytics or custom-data stream.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from autotrade.coreutils.object import Slice
from autotrade.coreutils.constant import Direction
from autotrade.engine.oms import OmsBase
from autotrade.engine.security_manager import SecurityManager


GREEKS = ("delta", "gamma", "vega", "theta", "rho", "vanna", "vomma", "charm")
NON_OPTION_GREEK_RISK_DATA_NAME = "non_option_greek_risk"


def _risk_field(record: Any | None, name: str) -> Any:
    """Read a risk field from analytics attributes or CustomData payload."""
    if record is None:
        return None
    value = getattr(record, name, None)
    if value is not None:
        return value
    payload = getattr(record, "payload", None)
    return None if not isinstance(payload, Mapping) else payload.get(name)


@dataclass(frozen=True)
class GreekRiskState:
    """The latest security and risk record for one instrument.

    ``analytics`` is an ``OptionAnalyticsData`` for an option, or a custom
    record with the same Greek field names for a non-option asset.  ``None``
    means no risk record has been supplied.
    """

    security: Any | None
    analytics: Any | None

    @property
    def price(self) -> float | None:
        """Latest canonical price maintained by ``SecurityManager``."""
        value = None if self.security is None else getattr(self.security, "price", None)
        return None if value is None else float(value)

    @property
    def multiplier(self) -> float | None:
        value = None if self.security is None else getattr(self.security, "multiplier", None)
        return None if value is None else float(value)

    @property
    def forward_price(self) -> float | None:
        value = _risk_field(self.analytics, "forward_price")
        return None if value is None else float(value)

    @property
    def driver_price(self) -> float | None:
        """Forward when supplied; otherwise the asset's canonical price."""
        return self.forward_price if self.forward_price is not None else self.price

    def greek(self, name: str) -> float | None:
        if name not in GREEKS:
            raise KeyError(f"unsupported Greek: {name}")
        value = _risk_field(self.analytics, name)
        return None if value is None else float(value)

    @property
    def delta(self) -> float | None:
        return self.greek("delta")

    @property
    def gamma(self) -> float | None:
        return self.greek("gamma")

    @property
    def vega(self) -> float | None:
        return self.greek("vega")

    @property
    def theta(self) -> float | None:
        return self.greek("theta")

    @property
    def rho(self) -> float | None:
        return self.greek("rho")

    @property
    def vanna(self) -> float | None:
        return self.greek("vanna")

    @property
    def vomma(self) -> float | None:
        return self.greek("vomma")

    @property
    def charm(self) -> float | None:
        return self.greek("charm")

    def unit_exposure(self, greek: str) -> float | None:
        """Sensitivity for one contract, excluding any OMS position."""
        value = self.greek(greek)
        return None if value is None or self.multiplier is None else value * self.multiplier

    @property
    def unit_delta_exposure(self) -> float | None:
        return self.unit_exposure("delta")

    @property
    def unit_dollar_delta_1pct(self) -> float | None:
        exposure = self.unit_delta_exposure
        return None if exposure is None or self.driver_price is None else exposure * self.driver_price * 0.01


@dataclass(frozen=True)
class GreekExposure:
    """Dollar-neutral-unit Greek exposure for an asset or a whole portfolio."""

    quantity: float
    multiplier: float | None
    values: Mapping[str, float | None]
    missing: tuple[str, ...] = ()

    def __getattr__(self, name: str) -> float | None:
        if name in GREEKS:
            return self.values.get(name)
        raise AttributeError(name)


def _signed_position(position: Any | None) -> float:
    if position is None:
        return 0.0
    volume = float(position.volume)
    if getattr(position, "direction", None) == Direction.SHORT and volume > 0:
        return -volume
    return volume


class GreekRiskManager:
    """Maintain latest risk records and calculate current raw exposures."""

    def __init__(self, security_manager: SecurityManager, oms: OmsBase) -> None:
        self.security_manager = security_manager
        self.oms = oms
        self._analytics: dict[str, Any] = {}

    def update(self, analytics: Any) -> GreekRiskState:
        """Store the latest complete analytics/custom record for its instrument."""
        instrument_id = str(analytics.instrument_id)
        self._analytics[instrument_id] = analytics
        return self.get(instrument_id)

    def on_slice(
        self,
        slice_: Slice,
        *,
        option_analytics_data_name: str = "option_analytics",
        non_option_greek_risk_data_name: str = NON_OPTION_GREEK_RISK_DATA_NAME,
    ) -> None:
        """Refresh state from the two deliberately separate risk-data inputs."""
        for analytics in slice_.option_analytics.get(option_analytics_data_name, {}).values():
            self.update(analytics)
        for records in slice_.custom_data.get(non_option_greek_risk_data_name, {}).values():
            for analytics in records:
                self.update(analytics)

    def get(self, instrument_id: str) -> GreekRiskState:
        """Return current Security plus the latest optional risk record."""
        return GreekRiskState(
            security=self.security_manager.get(instrument_id),
            analytics=self._analytics.get(instrument_id),
        )

    def items(self) -> Iterable[tuple[str, GreekRiskState]]:
        for instrument_id in self._analytics:
            yield instrument_id, self.get(instrument_id)

    def asset_exposure(
        self, instrument_id: str, quantity: float | None = None
    ) -> GreekExposure:
        state = self.get(instrument_id)
        quantity = _signed_position(self.oms.get_position(instrument_id)) if quantity is None else float(quantity)
        multiplier = state.multiplier
        if multiplier is None:
            return GreekExposure(quantity, None, {greek: None for greek in GREEKS}, ("multiplier",))
        scale = quantity * float(multiplier)
        values = {
            greek: None if (value := state.greek(greek)) is None else scale * value
            for greek in GREEKS
        }
        return GreekExposure(quantity, float(multiplier), values, tuple(g for g, v in values.items() if v is None))

    def portfolio_exposure(
        self, positions: Mapping[str, float] | None = None
    ) -> GreekExposure:
        if positions is None:
            positions = {instrument_id: _signed_position(position) for instrument_id, position in self.oms.positions.items()}
        totals = {greek: 0.0 for greek in GREEKS}
        missing: list[str] = []
        for instrument_id, quantity in positions.items():
            if not quantity:
                continue
            exposure = self.asset_exposure(instrument_id, quantity)
            for greek, value in exposure.values.items():
                if value is None:
                    missing.append(f"{instrument_id}:{greek}")
                else:
                    totals[greek] += value
        return GreekExposure(sum(abs(float(q)) for q in positions.values()), None, totals, tuple(missing))


__all__ = ["GREEKS", "GreekExposure", "GreekRiskManager", "GreekRiskState", "NON_OPTION_GREEK_RISK_DATA_NAME"]
