"""Build and deliver standard TimeSlice objects from live market callbacks."""

from __future__ import annotations

from datetime import datetime

from autotrade.coreutils.object import (
    BarData,
    ContractData,
    InstrumentStateData,
    MarketData,
    Slice,
    Tick,
    TickData,
    TimeSlice,
    TradeBar,
    ValuationUpdate,
)
from autotrade.engine.event_engine import EVENT_LIVE_DATA, Event, EventEngine
from autotrade.engine.timeslice_driver import TimeSliceDriver


class LiveDataManager:
    def __init__(
        self,
        event_engine: EventEngine,
        *,
        driver: TimeSliceDriver | None = None,
        data_name: str = "live",
    ) -> None:
        self.event_engine = event_engine
        self.driver = driver or TimeSliceDriver(event_engine)
        self.data_name = data_name
        self.register()

    def register(self) -> None:
        self.event_engine.register(EVENT_LIVE_DATA, self.process_event)

    def unregister(self) -> None:
        self.event_engine.unregister(EVENT_LIVE_DATA, self.process_event)

    def process_event(self, event: Event) -> None:
        self.push(event.data)

    def push(
        self,
        data,
        *,
        data_name: str | None = None,
        strategy_data: bool | None = None,
        valuation_data: bool = False,
    ) -> TimeSlice:
        when = _data_time(data)
        if strategy_data is None:
            strategy_data = isinstance(data, (MarketData, TickData, BarData))
        strategy_item = _canonical_market_data(data)
        named_data = (
            [(data_name or self.data_name, strategy_item)]
            if strategy_data
            else []
        )
        valuation_updates = ()
        if valuation_data:
            value = getattr(strategy_item, "value", None)
            if value is None:
                value = getattr(data, "last_price", None)
            if value is None:
                value = getattr(data, "close_price", None)
            valuation_updates = (
                ValuationUpdate(
                    instrument_id=data.instrument_id,
                    time=when,
                    price=float(value),
                    source=strategy_item,
                ),
            )
        time_slice = TimeSlice(
            time=when,
            slice=Slice.from_named_data(when, named_data),
            security_updates=(data,),
            valuation_updates=valuation_updates,
        )
        self.driver.process(time_slice)
        return time_slice

    def push_batch(
        self,
        *,
        when: datetime,
        named_data,
        security_updates=None,
        valuation_data_names=None,
    ) -> TimeSlice:
        """Emit one synchronized live TimeSlice containing multiple data streams."""
        canonical = [
            (name, _canonical_market_data(data))
            for name, data in named_data
        ]
        valuation_names = set(valuation_data_names or ())
        valuation_updates = tuple(
            ValuationUpdate(
                instrument_id=data.instrument_id,
                time=when,
                price=float(data.value),
                source=data,
            )
            for name, data in canonical
            if name in valuation_names and data.value is not None
        )
        time_slice = TimeSlice(
            time=when,
            slice=Slice.from_named_data(when, canonical),
            security_updates=tuple(
                security_updates
                if security_updates is not None
                else (data for _, data in canonical)
            ),
            valuation_updates=valuation_updates,
        )
        self.driver.process(time_slice)
        return time_slice


def _data_time(data) -> datetime:
    when = getattr(data, "time", None) or getattr(data, "datetime", None)
    return when or datetime.now()


def _canonical_market_data(data):
    if isinstance(data, TickData):
        has_quote = data.bid_price_1 > 0 or data.ask_price_1 > 0
        return Tick(
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
    if isinstance(data, BarData):
        return TradeBar(
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
    return data


__all__ = ["LiveDataManager"]
