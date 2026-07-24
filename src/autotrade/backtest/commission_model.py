"""Commission policies used by the simulated broker."""

from __future__ import annotations

from autotrade.coreutils.constant import Direction


class CommissionModel:
    """Calculate commission from a fill and the latest Security settings."""

    def calculate(self, *, direction: Direction, price: float, volume: float, security) -> float:
        if security is None:
            return 0.0
        rate = (
            security.long_commission_rate
            if direction == Direction.LONG
            else security.short_commission_rate
        )
        if rate is None:
            rate = security.commission_rate
        turnover = abs(float(volume)) * float(price) * float(security.multiplier)
        return turnover * float(rate)


__all__ = ["CommissionModel"]
