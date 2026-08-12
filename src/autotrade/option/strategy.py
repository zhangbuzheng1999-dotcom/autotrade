"""Reusable option-strategy data assembly built on the standard Slice runtime."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from functools import lru_cache
import pandas as pd

from autotrade.coreutils.object import (
    OptionAnalyticsData,
    OptionContract,
    Slice,
)
from autotrade.engine.security_manager import SecurityManager
from autotrade.strategy.strategy_base import StrategyBase


@lru_cache
def _frame_attribute_names(cls: type) -> tuple[str, ...]:
    """Return stable dataclass fields and readable properties for one type."""
    names = [item.name for item in fields(cls)]
    for base in reversed(cls.__mro__):
        for name, member in vars(base).items():
            if isinstance(member, property) and name not in names:
                names.append(name)
    return tuple(names)


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
        """Build a detached frame from the fields exposed by both objects."""
        records = []
        for instrument_id, view in self.contracts.items():
            record = {"instrument_id": instrument_id}
            for value in (view.security, view.analytics):
                for name in _frame_attribute_names(type(value)):
                    record.setdefault(name, getattr(value, name))
            records.append(record)
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
