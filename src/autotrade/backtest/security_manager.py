"""Manager for the latest per-symbol Security objects."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from autotrade.coreutils.constant import Exchange

from autotrade.coreutils.object import (
    MarketData,
    EquitySecurity,
    EquityStateData,
    FutureContract,
    FutureStateData,
    InstrumentStateData,
    OptionContract,
    OptionStateData,
    Security,
)

if TYPE_CHECKING:
    from autotrade.coreutils.object import TimeSlice


class SecurityManager:
    """Create and maintain the single latest Security for every symbol."""

    def __init__(self) -> None:
        self.securities: dict[str, Security] = {}

    def add(
        self,
        symbol: str,
        *,
        exchange: Exchange | None = None,
    ) -> Security:
        security = self.securities.get(symbol)
        if security is None:
            security = Security(
                symbol=symbol,
                time=datetime.min,
                exchange=exchange,
            )
            self.securities[symbol] = security
        return security

    def get(self, symbol: str) -> Security | None:
        return self.securities.get(symbol)

    def on_data(self, data: MarketData | InstrumentStateData) -> None:
        if isinstance(data, InstrumentStateData):
            self._apply_instrument_state(data)
            return
        self.add(data.symbol).update_market(data)

    def on_timeslice(self, time_slice: "TimeSlice") -> None:
        for update in time_slice.security_updates:
            self.on_data(update)

    def _apply_instrument_state(self, state: InstrumentStateData) -> None:
        security_type = _security_type_for_state(state)
        security = self.securities.get(state.symbol)
        if security is None:
            security = security_type(
                symbol=state.symbol,
                time=state.time or datetime.min,
                exchange=state.exchange,
            )
            self.securities[state.symbol] = security
        elif not isinstance(security, security_type):
            if type(security) is not Security:
                raise TypeError(
                    f"cannot change asset type for {state.symbol!r} from "
                    f"{type(security).__name__} to {security_type.__name__}"
                )
            security = _upgrade_security(security, security_type)
            self.securities[state.symbol] = security
        security.apply_state(state)

    def __contains__(self, symbol: str) -> bool:
        return symbol in self.securities

    def __getitem__(self, symbol: str) -> Security:
        return self.securities[symbol]

    def items(self):
        return self.securities.items()

    def values(self):
        return self.securities.values()


def _security_type_for_state(state: InstrumentStateData) -> type[Security]:
    if isinstance(state, EquityStateData):
        return EquitySecurity
    if isinstance(state, FutureStateData):
        return FutureContract
    if isinstance(state, OptionStateData):
        return OptionContract
    return Security


def _upgrade_security(
    security: Security,
    security_type: type[Security],
) -> Security:
    upgraded = security_type(
        symbol=security.symbol,
        time=security.time,
        value=security.value,
        exchange=security.exchange,
    )
    for name in (
        "source",
        "is_tradable",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover",
        "open_interest",
        "bid",
        "ask",
        "bid_size",
        "ask_size",
    ):
        setattr(upgraded, name, getattr(security, name))
    return upgraded


__all__ = ["SecurityManager"]
