"""Margin policies used by the simulated broker."""

from __future__ import annotations

from autotrade.coreutils.object import PositionData


class MarginModel:
    """Calculate position margin from the latest mark and Security settings."""

    def calculate(
        self,
        *,
        position: PositionData,
        mark_price: float,
        security,
    ) -> float:
        if security is None:
            return 0.0
        return (
            abs(float(position.volume))
            * float(mark_price)
            * float(security.multiplier)
            * float(security.margin_rate)
        )


__all__ = ["MarginModel"]
