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
from autotrade.engine.event_engine import EVENT_DATA, Event, EventEngine

if TYPE_CHECKING:
    from autotrade.coreutils.object import TimeSlice


class SecurityManager:
    """Create and maintain the single authoritative Security per instrument_id."""

    def __init__(
        self,
        event_engine: EventEngine | None = None,
    ) -> None:
        self.event_engine = event_engine
        self.securities: dict[str, Security] = {}
        if event_engine is not None:
            self.bind(event_engine)

    def bind(
        self,
        event_engine: EventEngine,
    ) -> None:
        """Bind the shared manager to one data event stream."""
        if self.event_engine is not None and self.event_engine is not event_engine:
            self.event_engine.unregister(EVENT_DATA, self.process_data_event)
        self.event_engine = event_engine
        event_engine.register(EVENT_DATA, self.process_data_event)

    def unregister(self) -> None:
        if self.event_engine is not None:
            self.event_engine.unregister(EVENT_DATA, self.process_data_event)

    def start(self) -> None:
        """Lifecycle compatibility hook."""

    def stop(self) -> None:
        self.unregister()

    def process_data_event(self, event: Event) -> None:
        """Consume the single public market/instrument data event."""
        self.on_data(event.data)

    def add(
        self,
        instrument_id: str,
        *,
        exchange: Exchange | None = None,
    ) -> Security:
        security = self.securities.get(instrument_id)
        if security is None:
            security = Security(
                instrument_id=instrument_id,
                time=datetime.min,
                exchange=exchange,
            )
            self.securities[instrument_id] = security
        return security

    def get(self, instrument_id: str) -> Security | None:
        return self.securities.get(instrument_id)

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
                    instrument_id=data.instrument_id,
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
            has_quote = data.bid_price_1 > 0 or data.ask_price_1 > 0
            data = Tick(
                instrument_id=data.instrument_id,
                exchange=data.exchange,
                time=data.datetime,
                tick_type="quote" if has_quote else "trade",
                price=data.last_price,
                quantity=data.last_volume,
                bid=data.bid_price_1 if data.bid_price_1 > 0 else None,
                ask=data.ask_price_1 if data.ask_price_1 > 0 else None,
                bid_size=data.bid_volume_1,
                ask_size=data.ask_volume_1,
                metadata={"legacy_source": data},
            )
        elif isinstance(data, BarData):
            data = TradeBar(
                instrument_id=data.instrument_id,
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
        self.add(data.instrument_id).update_market(data)

    def on_timeslice(self, time_slice: "TimeSlice") -> None:
        for update in time_slice.security_updates:
            self.on_data(update)

    def _apply_instrument_state(self, state: InstrumentStateData) -> None:
        security_type = _security_type_for_state(state)
        security = self.securities.get(state.instrument_id)
        if security is None:
            security = security_type(
                instrument_id=state.instrument_id,
                time=state.time or datetime.min,
                exchange=state.exchange,
            )
            self.securities[state.instrument_id] = security
        elif not isinstance(security, security_type):
            if type(security) is not Security:
                raise TypeError(
                    f"cannot change asset type for {state.instrument_id!r} from "
                    f"{type(security).__name__} to {security_type.__name__}"
                )
            security = _upgrade_security(security, security_type)
            self.securities[state.instrument_id] = security
        security.apply_state(state)

    def __contains__(self, instrument_id: str) -> bool:
        return instrument_id in self.securities

    def __getitem__(self, instrument_id: str) -> Security:
        return self.securities[instrument_id]

    def items(self):
        return self.securities.items()

    def values(self):
        return self.securities.values()

    def get_tick(self, instrument_id: str):
        security = self.get(instrument_id)
        if security is None:
            return None
        source = security.source
        if isinstance(source, Tick):
            return source.metadata.get("legacy_source", source)
        return None

    def get_contract(self, instrument_id: str) -> Security | None:
        """Return the canonical Security instead of a duplicate contract cache."""
        return self.get(instrument_id)


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
        instrument_id=security.instrument_id,
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
