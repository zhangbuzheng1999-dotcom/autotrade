"""One risk state and one exposure object for option and hedge risk.

Raw Greeks remain mathematical derivatives.  ``GreekExposure`` represents one
chosen layer: raw, per-contract/position sensitivity, or standardized cash
risk.  Historical attribution deliberately uses position sensitivity; reports
and hedge sizing use position cash risk.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Literal, Mapping

from autotrade.coreutils.constant import Direction
from autotrade.coreutils.object import EquitySecurity, FutureContract, OptionContract, Slice
from autotrade.engine.oms import OmsBase
from autotrade.engine.security_manager import SecurityManager


GREEKS = ("delta", "gamma", "vega", "theta", "rho", "vanna", "vomma", "charm")
CASH_GREEKS = (
    "delta_cash_1pct", "gamma_cash_1pct", "vega_cash_1vol", "theta_cash_1d",
    "rho_cash_1bp", "vanna_cash_1pct_1vol", "vomma_cash_1vol",
    "charm_cash_1pct_1d",
)
ExposureLevel = Literal["raw", "contract", "position", "contract_cash", "position_cash"]
OptionFactorPrice = Literal["forward", "underlying"]
NON_OPTION_GREEK_RISK_DATA_NAME = "non_option_greek_risk"


def _risk_field(record: Any | None, name: str) -> Any:
    if record is None:
        return None
    value = getattr(record, name, None)
    if value is not None:
        return value
    payload = getattr(record, "payload", None)
    return None if not isinstance(payload, Mapping) else payload.get(name)


@dataclass(frozen=True)
class GreekShock:
    """The standardized market shocks used by cash-Greek risk reporting."""

    spot_return: float = 0.01
    vol_change: float = 0.01
    rate_change: float = 0.0001
    time_years: float = 1.0 / 365.0


DEFAULT_GREEK_SHOCK = GreekShock()


@dataclass(frozen=True)
class GreekExposure:
    """One asset/portfolio Greek vector at a named calculation layer."""

    level: ExposureLevel
    factor_id: str | None
    quantity: float | None
    multiplier: float | None
    values: Mapping[str, float | None]
    missing: tuple[str, ...] = ()

    def __getattr__(self, name: str) -> float | None:
        if name in self.values:
            return self.values[name]
        raise AttributeError(name)


@dataclass(frozen=True)
class GreekRiskState:
    """Immutable raw risk state for one instrument at one market timestamp."""

    security: Any | None
    analytics: Any | None
    option_factor_price: OptionFactorPrice = "forward"

    @property
    def asset_kind(self) -> str:
        """Classify runtime securities without relying on symbol conventions."""
        if isinstance(self.security, OptionContract):
            return "option"
        if isinstance(self.security, FutureContract):
            return "future"
        if isinstance(self.security, EquitySecurity):
            return "equity"
        return "other"

    @property
    def instrument_id(self) -> str | None:
        return None if self.security is None else str(self.security.instrument_id)

    @property
    def price(self) -> float | None:
        value = None if self.security is None else getattr(self.security, "price", None)
        return None if value is None else float(value)

    @property
    def multiplier(self) -> float | None:
        value = None if self.security is None else getattr(self.security, "multiplier", None)
        return None if value is None else float(value)

    @property
    def factor_id(self) -> str | None:
        if isinstance(self.security, OptionContract):
            return _risk_field(self.analytics, "underlying_instrument_id") or self.security.underlying_instrument_id
        explicit = _risk_field(self.analytics, "factor_id")
        if explicit is not None:
            return str(explicit)
        return self.instrument_id

    @property
    def factor_price(self) -> float | None:
        # An option's risk factor is never its premium.  The model's Greek is
        # either dV/dForward (Black-97) or dV/dSpot; select one explicitly.
        if isinstance(self.security, OptionContract):
            field = "forward_price" if self.option_factor_price == "forward" else "underlying_price"
            value = _risk_field(self.analytics, field)
            return None if value is None else float(value)
        explicit = _risk_field(self.analytics, "factor_price")
        if explicit is not None:
            return float(explicit)
        forward = _risk_field(self.analytics, "forward_price")
        if forward is not None:
            return float(forward)
        return self.price

    # Names retained as concise aliases for model / attribution code.
    @property
    def forward_price(self) -> float | None:
        value = _risk_field(self.analytics, "forward_price")
        return None if value is None else float(value)

    @property
    def driver_price(self) -> float | None:
        return self.factor_price

    def greek(self, name: str) -> float | None:
        if name not in GREEKS:
            raise KeyError(f"unsupported Greek: {name}")
        value = _risk_field(self.analytics, name)
        if value is not None:
            return float(value)
        # A non-option without supplied risk is a linear exposure to itself.
        if name == "delta" and self.security is not None and not isinstance(self.security, OptionContract):
            return 1.0
        if self.security is not None and not isinstance(self.security, OptionContract):
            return 0.0
        return None

    @property
    def delta(self) -> float | None: return self.greek("delta")
    @property
    def gamma(self) -> float | None: return self.greek("gamma")
    @property
    def vega(self) -> float | None: return self.greek("vega")
    @property
    def theta(self) -> float | None: return self.greek("theta")
    @property
    def rho(self) -> float | None: return self.greek("rho")
    @property
    def vanna(self) -> float | None: return self.greek("vanna")
    @property
    def vomma(self) -> float | None: return self.greek("vomma")
    @property
    def charm(self) -> float | None: return self.greek("charm")

    def exposure(
        self, *, quantity: float | None = None, level: ExposureLevel = "raw",
        shock: GreekShock = DEFAULT_GREEK_SHOCK,
    ) -> GreekExposure:
        """Return a mathematically explicit Greek layer for this state."""
        if level not in {"raw", "contract", "position", "contract_cash", "position_cash"}:
            raise ValueError(f"unsupported exposure level: {level!r}")
        multiplier = self.multiplier
        raw = {greek: self.greek(greek) for greek in GREEKS}
        missing = tuple(greek for greek, value in raw.items() if value is None)
        if level == "raw":
            return GreekExposure(level, self.factor_id, quantity, multiplier, raw, missing)
        if multiplier is None:
            keys = GREEKS if level in {"contract", "position"} else CASH_GREEKS
            return GreekExposure(level, self.factor_id, quantity, None, {key: None for key in keys}, ("multiplier",))
        scale = float(multiplier)
        if level in {"position", "position_cash"}:
            if quantity is None:
                raise ValueError(f"quantity is required for {level}")
            scale *= float(quantity)
        if level in {"contract", "position"}:
            return GreekExposure(level, self.factor_id, quantity, multiplier,
                {greek: None if value is None else scale * value for greek, value in raw.items()}, missing)
        factor_price = self.factor_price
        if factor_price is None:
            return GreekExposure(level, self.factor_id, quantity, multiplier,
                {key: None for key in CASH_GREEKS}, tuple(sorted(set((*missing, "factor_price")))))
        spot = float(factor_price) * shock.spot_return
        terms = {
            "delta_cash_1pct": ("delta", scale * spot),
            "gamma_cash_1pct": ("gamma", scale * spot**2 * 0.5),
            "vega_cash_1vol": ("vega", scale * shock.vol_change),
            "theta_cash_1d": ("theta", scale * shock.time_years),
            "rho_cash_1bp": ("rho", scale * shock.rate_change),
            "vanna_cash_1pct_1vol": ("vanna", scale * spot * shock.vol_change),
            "vomma_cash_1vol": ("vomma", scale * shock.vol_change**2 * 0.5),
            "charm_cash_1pct_1d": ("charm", scale * spot * shock.time_years),
        }
        cash = {
            key: None if raw[greek] is None else raw[greek] * coefficient
            for key, (greek, coefficient) in terms.items()
        }
        return GreekExposure(level, self.factor_id, quantity, multiplier,
            cash, missing)


def _signed_position(position: Any | None) -> float:
    if position is None:
        return 0.0
    volume = float(position.volume)
    return -volume if getattr(position, "direction", None) == Direction.SHORT and volume > 0 else volume


class GreekRiskManager:
    """Store raw records; create sensitivities/cash risks on request."""

    def __init__(
        self, security_manager: SecurityManager, oms: OmsBase,
        *, option_factor_price: OptionFactorPrice = "forward",
    ) -> None:
        if option_factor_price not in {"forward", "underlying"}:
            raise ValueError("option_factor_price must be 'forward' or 'underlying'")
        self.security_manager, self.oms = security_manager, oms
        self.option_factor_price = option_factor_price
        self._analytics: dict[str, Any] = {}

    def update(self, analytics: Any) -> GreekRiskState:
        self._analytics[str(analytics.instrument_id)] = analytics
        return self.get(str(analytics.instrument_id))

    def on_slice(self, slice_: Slice, *, option_analytics_data_name: str = "option_analytics",
                 non_option_greek_risk_data_name: str = NON_OPTION_GREEK_RISK_DATA_NAME) -> None:
        for analytics in slice_.option_analytics.get(option_analytics_data_name, {}).values():
            self.update(analytics)
        for records in slice_.custom_data.get(non_option_greek_risk_data_name, {}).values():
            for analytics in records:
                self.update(analytics)

    def get(self, instrument_id: str) -> GreekRiskState:
        return GreekRiskState(
            self.security_manager.get(instrument_id), self._analytics.get(instrument_id),
            self.option_factor_price,
        )

    def items(self) -> Iterable[tuple[str, GreekRiskState]]:
        for instrument_id in set(self._analytics) | set(self.oms.positions):
            yield instrument_id, self.get(instrument_id)

    def exposure(self, instrument_id: str, *, quantity: float | None = None,
                 level: ExposureLevel = "position_cash", shock: GreekShock = DEFAULT_GREEK_SHOCK) -> GreekExposure:
        quantity = _signed_position(self.oms.get_position(instrument_id)) if quantity is None else float(quantity)
        return self.get(instrument_id).exposure(quantity=quantity, level=level, shock=shock)

    def portfolio_exposure(self, *, positions: Mapping[str, float] | None = None,
                           level: ExposureLevel = "position_cash", shock: GreekShock = DEFAULT_GREEK_SHOCK) -> dict[str, GreekExposure]:
        """Aggregate only within factor IDs; returns one vector per factor."""
        if positions is None:
            positions = {instrument_id: _signed_position(position) for instrument_id, position in self.oms.positions.items()}
        grouped: dict[str, list[GreekExposure]] = {}
        for instrument_id, quantity in positions.items():
            if not quantity:
                continue
            item = self.exposure(instrument_id, quantity=quantity, level=level, shock=shock)
            if item.factor_id is None:
                raise ValueError(f"{instrument_id!r} has no factor_id")
            grouped.setdefault(item.factor_id, []).append(item)
        result: dict[str, GreekExposure] = {}
        for factor_id, items in grouped.items():
            keys = CASH_GREEKS if level in {"contract_cash", "position_cash"} else GREEKS
            result[factor_id] = GreekExposure(
                level, factor_id, None, None,
                {
                    key: None if any(item.values[key] is None for item in items)
                    else sum(float(item.values[key]) for item in items)
                    for key in keys
                },
                tuple(sorted({reason for item in items for reason in item.missing})),
            )
        return result


__all__ = [
    "CASH_GREEKS", "DEFAULT_GREEK_SHOCK", "GREEKS", "GreekExposure", "GreekRiskManager",
    "GreekRiskState", "GreekShock", "NON_OPTION_GREEK_RISK_DATA_NAME", "OptionFactorPrice",
]
