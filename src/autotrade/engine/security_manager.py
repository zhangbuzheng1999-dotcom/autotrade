"""Authoritative manager for the latest instrument and market state."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from autotrade.coreutils.constant import Exchange

from autotrade.coreutils.object import (
    BarData,
    ContractData,
    MarketData,
    EquitySecurity,
    EquityStateData,
    FutureContract,
    FutureStateData,
    InstrumentStateData,
    OptionContract,
    OptionStateData,
    Security,
    Tick,
    TickData,
    TradeBar,
)
from autotrade.engine.event_engine import (
    EVENT_DATA,
    EVENT_SLICE,
    Event,
    EventEngine,
)

if TYPE_CHECKING:
    from autotrade.coreutils.object import TimeSlice


class SecurityManager:
    """Create and maintain the single authoritative Security per symbol."""

    def __init__(
        self,
        event_engine: EventEngine | None = None,
        *,
        forward_data: bool = True,
    ) -> None:
        self.event_engine = event_engine
        self.forward_data = forward_data
        self.securities: dict[str, Security] = {}
        if event_engine is not None:
            self.bind(event_engine, forward_data=forward_data)

    def bind(
        self,
        event_engine: EventEngine,
        *,
        forward_data: bool | None = None,
    ) -> None:
        """Bind the shared manager to one data event stream."""
        if self.event_engine is not None and self.event_engine is not event_engine:
            self.event_engine.unregister(EVENT_DATA, self.process_data_event)
        self.event_engine = event_engine
        if forward_data is not None:
            self.forward_data = forward_data
        event_engine.register(EVENT_DATA, self.process_data_event)

    def process_data_event(self, event: Event) -> None:
        """Consume the single public market/instrument data event."""
        self.on_data(event.data)
        if self.forward_data and self.event_engine is not None:
            self.event_engine.put(Event(EVENT_SLICE, event.data))

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

    def on_data(
        self,
        data: MarketData | InstrumentStateData | TickData | BarData | ContractData,
    ) -> None:
        if isinstance(data, InstrumentStateData):
            self._apply_instrument_state(data)
            return
        if isinstance(data, ContractData):
            self._apply_instrument_state(
                InstrumentStateData(
                    symbol=data.symbol,
                    time=None,
                    is_active=True,
                    exchange=data.exchange,
                    multiplier=data.size,
                    attributes={
                        "name": data.name,
                        "product": data.product,
                        "pricetick": data.pricetick,
                        "min_volume": data.min_volume,
                        "max_volume": data.max_volume,
                    },
                )
            )
            return
        if isinstance(data, TickData):
            data = Tick(
                symbol=data.symbol,
                exchange=data.exchange,
                time=data.datetime,
                tick_type="quote",
                price=data.last_price,
                quantity=data.last_volume,
                bid=data.bid_price_1,
                ask=data.ask_price_1,
                bid_size=data.bid_volume_1,
                ask_size=data.ask_volume_1,
                metadata={"legacy_source": data},
            )
        elif isinstance(data, BarData):
            data = TradeBar(
                symbol=data.symbol,
                exchange=data.exchange,
                time=data.datetime,
                interval=data.interval,
                open=data.open_price,
                high=data.high_price,
                low=data.low_price,
                close=data.close_price,
                volume=data.volume,
                turnover=data.turnover,
                open_interest=data.open_interest,
                metadata={"legacy_source": data},
            )
        if not isinstance(data, MarketData):
            raise TypeError(f"unsupported security data type: {type(data).__name__}")
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

    def get_tick(self, symbol: str):
        security = self.get(symbol)
        if security is None:
            return None
        source = security.source
        if isinstance(source, Tick):
            return source.metadata.get("legacy_source", source)
        return None

    def get_contract(self, symbol: str) -> Security | None:
        """Return the canonical Security instead of a duplicate contract cache."""
        return self.get(symbol)


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
