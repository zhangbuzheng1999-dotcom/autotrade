"""Order matching policies for the simulated broker."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from autotrade.coreutils.constant import Direction, OrderType
from autotrade.coreutils.object import OrderData, Slice, TimeSlice, TradeBar
from autotrade.engine.security_manager import SecurityManager

if TYPE_CHECKING:
    from autotrade.coreutils.object import Security


@dataclass(slots=True)
class BacktestSettings:
    cheat_on_close: bool = False
    market_fill_price: str = "next_open"
    stop_limit_same_bar: str = "conservative"
    execution_data_name: str | None = None


@dataclass(slots=True)
class FillContext:
    time: datetime
    order: OrderData
    security: "Security"
    slice: Slice
    securities: SecurityManager
    settings: BacktestSettings


@dataclass(frozen=True, slots=True)
class Fill:
    order_id: str
    symbol: str
    price: float
    quantity: float
    time: datetime


class FillModel:
    def fill(self, parameters: FillContext) -> Fill | None:
        order = parameters.order
        if order.type == OrderType.MARKET:
            return self.market_fill(parameters)
        if order.type == OrderType.LIMIT:
            return self.limit_fill(parameters)
        if order.type == OrderType.STP_MKT:
            return self.stop_market_fill(parameters)
        if order.type == OrderType.STP_LMT:
            return self.stop_limit_fill(parameters)
        raise UnsupportedOrderType(order.type)

    def market_fill(self, parameters: FillContext) -> Fill | None:
        return None

    def limit_fill(self, parameters: FillContext) -> Fill | None:
        return None

    def stop_market_fill(self, parameters: FillContext) -> Fill | None:
        return None

    def stop_limit_fill(self, parameters: FillContext) -> Fill | None:
        return None


class BarFillModel(FillModel):
    """Conservative bar-only fill model."""

    def market_fill(self, parameters: FillContext) -> Fill | None:
        order = parameters.order
        bar = _current_trade_bar(parameters)
        if bar is None:
            return None
        same_time = order.datetime == parameters.time
        if same_time and not parameters.settings.cheat_on_close:
            return None
        if same_time and parameters.settings.cheat_on_close:
            price = bar.close
        elif parameters.settings.market_fill_price == "next_close":
            price = bar.close
        else:
            price = bar.open
        return _full_fill(order, price, parameters.time)

    def limit_fill(self, parameters: FillContext) -> Fill | None:
        order = parameters.order
        bar = _current_trade_bar(parameters)
        if bar is None or order.datetime == parameters.time:
            return None
        if order.direction == Direction.LONG and bar.low <= order.price:
            return _full_fill(order, min(bar.open, order.price), parameters.time)
        if order.direction == Direction.SHORT and bar.high >= order.price:
            return _full_fill(order, max(bar.open, order.price), parameters.time)
        return None

    def stop_market_fill(self, parameters: FillContext) -> Fill | None:
        order = parameters.order
        bar = _current_trade_bar(parameters)
        if bar is None or order.datetime == parameters.time:
            return None
        if order.direction == Direction.LONG and bar.high >= order.trigger_price:
            return _full_fill(order, max(order.trigger_price, bar.open), parameters.time)
        if order.direction == Direction.SHORT and bar.low <= order.trigger_price:
            return _full_fill(order, min(order.trigger_price, bar.open), parameters.time)
        return None

    def stop_limit_fill(self, parameters: FillContext) -> Fill | None:
        order = parameters.order
        bar = _current_trade_bar(parameters)
        if bar is None or order.datetime == parameters.time:
            return None
        stop_triggered = bool(getattr(order, "stop_triggered", False))
        triggered_this_bar = False
        if not stop_triggered:
            if order.direction == Direction.LONG and bar.high >= order.trigger_price:
                stop_triggered = True
                triggered_this_bar = True
            elif order.direction == Direction.SHORT and bar.low <= order.trigger_price:
                stop_triggered = True
                triggered_this_bar = True
        if not stop_triggered:
            return None
        setattr(order, "stop_triggered", True)
        if (
            triggered_this_bar
            and parameters.settings.stop_limit_same_bar == "conservative"
        ):
            return None
        return self.limit_fill(parameters)


class MatchingEngine:
    def __init__(
        self,
        *,
        fill_model: FillModel,
        settings: BacktestSettings,
        security_manager: SecurityManager,
    ) -> None:
        self.fill_model = fill_model
        self.settings = settings
        self.security_manager = security_manager

    def match(
        self,
        time_slice: TimeSlice,
        orders: list[OrderData],
        *,
        same_time_only: bool = False,
    ) -> list[tuple[OrderData, Fill]]:
        slice_ = time_slice.slice
        data_name = self.settings.execution_data_name
        if data_name is not None and not slice_.bars.get(data_name):
            return []
        if data_name is None and not slice_.bars:
            return []

        matches: list[tuple[OrderData, Fill]] = []
        for order in orders:
            if same_time_only and order.datetime != time_slice.time:
                continue
            security = self.security_manager.add(order.symbol)
            fill = self.fill_model.fill(
                FillContext(
                    time=slice_.time,
                    order=order,
                    security=security,
                    slice=slice_,
                    securities=self.security_manager,
                    settings=self.settings,
                )
            )
            if fill is not None:
                matches.append((order, fill))
        return matches


class UnsupportedOrderType(Exception):
    def __init__(self, order_type: OrderType):
        super().__init__(f"Unsupported order type: {order_type}")


def _current_trade_bar(parameters: FillContext) -> TradeBar | None:
    return parameters.slice.get_bar(
        parameters.order.symbol,
        data_name=parameters.settings.execution_data_name,
    )


def _full_fill(order: OrderData, price: float, time: datetime) -> Fill | None:
    quantity = max(float(order.volume) - float(order.traded), 0.0)
    if quantity <= 0:
        return None
    return Fill(
        order_id=order.orderid,
        symbol=order.symbol,
        price=price,
        quantity=quantity,
        time=time,
    )


__all__ = [
    "BacktestSettings",
    "BarFillModel",
    "Fill",
    "FillContext",
    "FillModel",
    "MatchingEngine",
    "UnsupportedOrderType",
]
