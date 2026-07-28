"""Reusable option-strategy data assembly built on the standard Slice runtime."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import pandas as pd

from autotrade.coreutils.object import (
    OptionAnalyticsData,
    OptionContract,
    Slice,
)
from autotrade.engine.security_manager import SecurityManager
from autotrade.strategy.strategy_base import StrategyBase


@dataclass(slots=True)
class OptionContractView:
    """Current Security state paired with one analytics record."""

    security: OptionContract
    analytics: OptionAnalyticsData


@dataclass(slots=True)
class OptionPanelView:
    """Strategy-local joined view of option Security and analytics data."""

    contracts: dict[str, OptionContractView]

    def to_frame(self) -> pd.DataFrame:
        """Build a detached DataFrame snapshot of the current object view."""
        records = []
        for instrument_id, view in self.contracts.items():
            security = view.security
            analytics = view.analytics
            records.append(
                {
                    "instrument_id": instrument_id,
                    "underlying_instrument_id":
                        security.underlying_instrument_id,
                    "expiry": security.expiry,
                    "strike": security.strike,
                    "right": security.right,
                    "style": security.style,
                    "exchange": security.exchange,
                    "multiplier": security.multiplier,
                    "is_active": security.is_active,
                    "is_tradable": security.is_tradable,
                    "price": security.price,
                    "open": security.open,
                    "high": security.high,
                    "low": security.low,
                    "close": security.close,
                    "volume": security.volume,
                    "turnover": security.turnover,
                    "open_interest": security.open_interest,
                    "bid": security.bid,
                    "ask": security.ask,
                    "bid_size": security.bid_size,
                    "ask_size": security.ask_size,
                    "underlying_price": analytics.underlying_price,
                    "forward_price": analytics.forward_price,
                    "risk_free_rate": analytics.risk_free_rate,
                    "time_to_expiry": analytics.time_to_expiry,
                    "market_iv": analytics.market_iv,
                    "surface_iv": analytics.surface_iv,
                    "delta": analytics.delta,
                    "gamma": analytics.gamma,
                    "vega": analytics.vega,
                    "theta": analytics.theta,
                    "rho": analytics.rho,
                    "vanna": analytics.vanna,
                    "vomma": analytics.vomma,
                    "charm": analytics.charm,
                    "model_id": analytics.model_id,
                    "model_version": analytics.model_version,
                }
            )
        if not records:
            return pd.DataFrame()
        return (
            pd.DataFrame.from_records(records)
            .set_index("instrument_id")
            .sort_index()
        )


class OptionPanelAssembler:
    """Join Slice analytics to current option Securities by instrument ID."""

    @staticmethod
    def build(
        security_manager: SecurityManager,
        analytics_data: Mapping[str, OptionAnalyticsData],
    ) -> OptionPanelView | None:
        if not analytics_data:
            return None

        contracts = {}
        for key, analytics in analytics_data.items():
            if key != analytics.instrument_id:
                raise ValueError(
                    f"analytics key {key!r} does not match instrument_id "
                    f"{analytics.instrument_id!r}"
                )
            security = security_manager.get(analytics.instrument_id)
            if security is None:
                raise KeyError(
                    f"security {analytics.instrument_id!r} was not initialized"
                )
            if not isinstance(security, OptionContract):
                raise TypeError(
                    f"{analytics.instrument_id!r} maps to "
                    f"{type(security).__name__}, expected OptionContract"
                )
            contracts[analytics.instrument_id] = OptionContractView(
                security=security,
                analytics=analytics,
            )

        return OptionPanelView(contracts=contracts)


class OptionStrategy(StrategyBase):
    """Base class that assembles an option panel only on analytics slices."""

    def __init__(
        self,
        event_engine,
        security_manager: SecurityManager,
        *,
        option_analytics_data_name: str = "option_analytics",
    ) -> None:
        super().__init__(
            event_engine=event_engine,
            security_manager=security_manager,
        )
        if not option_analytics_data_name.strip():
            raise ValueError("option_analytics_data_name cannot be empty")
        self.option_analytics_data_name = option_analytics_data_name

    def on_data(self, slice_: Slice) -> None:
        super().on_data(slice_)
        analytics_data = slice_.option_analytics.get(
            self.option_analytics_data_name
        )
        if not analytics_data:
            return
        panel = OptionPanelAssembler.build(
            self.security_manager,
            analytics_data,
        )
        if panel is not None:
            self.on_option_panel(panel, slice_)

    def on_option_panel(
        self,
        panel: OptionPanelView,
        slice_: Slice,
    ) -> None:
        """Handle one assembled analytics panel in a concrete option strategy."""


__all__ = [
    "OptionContractView",
    "OptionPanelAssembler",
    "OptionPanelView",
    "OptionStrategy",
]
