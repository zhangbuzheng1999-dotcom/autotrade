"""Order state owned by the simulated broker."""

from __future__ import annotations

from datetime import datetime

from autotrade.coreutils.constant import OrderStatus
from autotrade.coreutils.object import CancelRequest, ModifyRequest, OrderData


class RequestRejected(Exception):
    """Expected business rejection while processing a request."""


class SimulatedOrderBook:
    def __init__(self) -> None:
        self.active_orders: dict[str, dict[str, OrderData]] = {}
        self.pending_orders: dict[str, dict[str, OrderData]] = {}

    def submit(self, order: OrderData) -> None:
        self._ensure_symbol(order.symbol)
        self.pending_orders[order.symbol][order.orderid] = order

    def cancel(self, request: CancelRequest, now: datetime) -> OrderData:
        order = self.find(request.symbol, request.orderid)
        if order is None:
            raise RequestRejected(f"撤单失败，订单不存在: {request.orderid}")
        if order.status in {OrderStatus.ALLTRADED, OrderStatus.ALLCANCELLED}:
            raise RequestRejected(f"撤单失败，订单不可撤销: {request.orderid}")
        self.pop(request.symbol, request.orderid)
        order.status = OrderStatus.ALLCANCELLED
        order.datetime = now
        return order

    def modify(self, request: ModifyRequest, now: datetime) -> OrderData:
        order = self.find(request.symbol, request.orderid)
        if order is None:
            raise RequestRejected(f"修改失败，订单不存在: {request.orderid}")
        if order.status in {
            OrderStatus.ALLTRADED,
            OrderStatus.ALLCANCELLED,
            OrderStatus.PARTCANCELLED,
        }:
            raise RequestRejected(f"修改失败，订单不可修改: {request.orderid}")
        if request.qty <= order.traded:
            raise RequestRejected(
                f"修改失败，新订单总量必须大于已成交数量: {request.orderid}"
            )

        self.pop(request.symbol, request.orderid)
        execution_status = order.status
        order.price = request.price
        order.volume = request.qty
        order.trigger_price = request.trigger_price
        order.datetime = now
        order.status = execution_status
        setattr(order, "stop_triggered", False)
        self.submit(order)
        return order

    def activate(
        self,
        now: datetime,
        *,
        include_current: bool = False,
    ) -> list[OrderData]:
        activated: list[OrderData] = []
        for symbol, orders in list(self.pending_orders.items()):
            self._ensure_symbol(symbol)
            for orderid, order in list(orders.items()):
                should_activate = (
                    order.datetime is None
                    or order.datetime < now
                    or (include_current and order.datetime <= now)
                )
                if not should_activate:
                    continue
                order.status = (
                    OrderStatus.PARTTRADED
                    if float(order.traded) > 0
                    else OrderStatus.NOTTRADED
                )
                if include_current:
                    order.datetime = now
                self.active_orders[symbol][orderid] = order
                del orders[orderid]
                activated.append(order)
        return activated

    def iter_active(self):
        for orders in self.active_orders.values():
            yield from list(orders.values())

    def remove_active(self, order: OrderData) -> None:
        self.active_orders.get(order.symbol, {}).pop(order.orderid, None)

    def find(self, symbol: str, orderid: str) -> OrderData | None:
        return (
            self.active_orders.get(symbol, {}).get(orderid)
            or self.pending_orders.get(symbol, {}).get(orderid)
        )

    def pop(self, symbol: str, orderid: str) -> OrderData | None:
        for pool in (self.active_orders, self.pending_orders):
            orders = pool.get(symbol, {})
            if orderid in orders:
                return orders.pop(orderid)
        return None

    def get_active(self) -> list[OrderData]:
        return list(self.iter_active())

    def _ensure_symbol(self, symbol: str) -> None:
        self.active_orders.setdefault(symbol, {})
        self.pending_orders.setdefault(symbol, {})


__all__ = ["RequestRejected", "SimulatedOrderBook"]
