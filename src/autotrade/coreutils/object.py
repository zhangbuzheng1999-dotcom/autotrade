"""
Basic data structure used for general trading function in the trading platform.
"""

import math

from dataclasses import dataclass, field
from datetime import datetime as Datetime
from enum import Enum
from autotrade.coreutils.constant import FetchStatus, Direction, Exchange, Interval, Offset, OrderStatus, Product, \
    OptionType, OrderType, LogLevel
from typing import Any, Generic, Iterable, TypeVar, Optional
from uuid import uuid4

INFO: int = 20

ACTIVE_STATUSES = set([OrderStatus.SUBMITTING, OrderStatus.NOTTRADED,
                       OrderStatus.PARTTRADED, OrderStatus.PENDING,
                       OrderStatus.UNKNOWN, OrderStatus.MODIFIED])


@dataclass(kw_only=True)
class BaseData:
    """Common extension point for runtime data objects."""

    extra: dict | None = field(default=None, init=False)


@dataclass(kw_only=True)
class TickData(BaseData):
    """
    Tick data contains information about:
        * last trade in market
        * orderbook snapshot
        * intraday market statistics.
    """

    instrument_id: str
    exchange: Exchange | None = None
    datetime: Datetime
    gateway_name: str | None = None

    name: str = ""
    volume: float = 0
    turnover: float = 0
    open_interest: float = 0
    last_price: float = 0
    last_volume: float = 0
    limit_up: float = 0
    limit_down: float = 0

    open_price: float = 0
    high_price: float = 0
    low_price: float = 0
    close_price: float = 0
    pre_close: float = 0

    bid_price_1: float = 0
    bid_price_2: float = 0
    bid_price_3: float = 0
    bid_price_4: float = 0
    bid_price_5: float = 0

    ask_price_1: float = 0
    ask_price_2: float = 0
    ask_price_3: float = 0
    ask_price_4: float = 0
    ask_price_5: float = 0

    bid_volume_1: float = 0
    bid_volume_2: float = 0
    bid_volume_3: float = 0
    bid_volume_4: float = 0
    bid_volume_5: float = 0

    ask_volume_1: float = 0
    ask_volume_2: float = 0
    ask_volume_3: float = 0
    ask_volume_4: float = 0
    ask_volume_5: float = 0

    localtime: Datetime | None = None

@dataclass(kw_only=True)
class BarData(BaseData):
    """
    Candlestick bar data of a certain trading period.
    """

    instrument_id: str
    exchange: Exchange | None = None
    datetime: Datetime
    gateway_name: str | None = None

    interval: Interval | None = None
    volume: float = 0
    turnover: float = 0
    open_interest: float = 0
    open_price: float = 0
    high_price: float = 0
    low_price: float = 0
    close_price: float = 0

@dataclass(kw_only=True)
class OrderData(BaseData):
    """
    Order data contains information for tracking lastest status
    of a specific order.
    """

    instrument_id: str
    orderid: str
    exchange: Exchange | None = None
    gateway_name: str | None = None

    type: OrderType = OrderType.LIMIT
    direction: Direction | None = None
    offset: Offset = Offset.NONE
    price: float = 0
    volume: float = 0
    traded: float = 0  # 成交数量
    avgFillPrice: float = 0  # 成交均价
    status: OrderStatus = OrderStatus.SUBMITTING
    datetime: Datetime | None = None
    broker_orderid: str | None = None  # 券商返回的orderid，在gateway中orderid由本地生成，查询时和broker_orderid映射
    reference: str = ""
    trigger_price: float = 0

    def is_active(self) -> bool:
        """
        Check if the order is active.
        """
        return self.status in ACTIVE_STATUSES

    def create_cancel_request(self) -> "CancelRequest":
        """
        Create cancel request object from order.
        """
        req: CancelRequest = CancelRequest(
            orderid=self.orderid, instrument_id=self.instrument_id, exchange=self.exchange
        )
        return req

    def to_dict(self) -> dict:
        """安全序列化为 dict"""
        return {
            "instrument_id": self.instrument_id,
            "exchange": self.exchange.value if self.exchange else None,
            "orderid": self.orderid,
            "type": self.type.value if self.type else None,
            "direction": self.direction.value if self.direction else None,
            "offset": self.offset.value if self.offset else None,
            "price": self.price,
            "volume": self.volume,
            "traded": self.traded,
            "avgFillPrice": self.avgFillPrice,
            "status": self.status.value if self.status else None,
            "datetime": self.datetime.isoformat() if self.datetime else None,
            "broker_orderid": self.broker_orderid,
            "reference": self.reference,
            "trigger_price": self.trigger_price,
            "gateway_name": self.gateway_name,
        }


@dataclass(kw_only=True)
class TradeData(BaseData):
    """
    Trade data contains information of a fill of an order. One order
    can have several trade fills.
    """

    instrument_id: str
    orderid: str
    tradeid: str
    exchange: Exchange | None = None
    gateway_name: str | None = None
    direction: Direction | None = None

    offset: Offset = Offset.NONE
    price: float = 0
    datetime: Datetime | None = None
    traded: float = 0  # 成交数量
    volume: float = 0  # 总订单数量
    avgFillPrice: float = 0  # 成交均价
    status: OrderStatus | None = None
    reference: str = ""

    def is_active(self) -> bool:
        """
        Check if the order is active.
        """
        return self.status in ACTIVE_STATUSES

    def to_dict(self) -> dict:
        """安全序列化为 dict"""
        return {
            "instrument_id": self.instrument_id,
            "exchange": self.exchange.value if self.exchange else None,
            "orderid": self.orderid,
            "tradeid": self.tradeid,
            "direction": self.direction.value if self.direction else None,
            "offset": self.offset.value if self.offset else None,
            "price": self.price,
            "datetime": self.datetime.isoformat() if self.datetime else None,
            "traded": self.traded,
            "volume": self.volume,
            "avgFillPrice": self.avgFillPrice,
            "status": self.status.value if self.status else None,
            "reference": self.reference,
            "gateway_name": self.gateway_name,
        }


@dataclass(kw_only=True)
class PositionData(BaseData):
    """
    Position data is used for tracking each individual position holding.
    """

    instrument_id: str
    direction: Direction
    exchange: Exchange | None = None
    gateway_name: str | None = None
    contract_instrument_id: str | None = None
    volume: float = 0
    frozen: float = 0
    price: float = 0
    pnl: float = 0
    yd_volume: float = 0
    margin: float = 0

    def to_dict(self) -> dict:
        """安全序列化为 dict"""
        return {
            "instrument_id": self.instrument_id,
            "exchange": self.exchange.value if self.exchange else None,
            "direction": self.direction.value if self.direction else None,
            "volume": self.volume,
            "frozen": self.frozen,
            "price": self.price,
            "pnl": self.pnl,
            "yd_volume": self.yd_volume,
            "margin": self.margin,
            "gateway_name": self.gateway_name,
            "contract_instrument_id": self.contract_instrument_id,
        }


@dataclass(kw_only=True)
class AccountData(BaseData):
    """
    Account data contains information about balance, frozen and
    available.
    """

    accountid: str
    gateway_name: str | None = None
    balance: float = 0
    frozen: float = 0.0
    cash: float = 0.0
    margin: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    equity: float = 0.0
    available: float = 0.0

@dataclass
class LogData:
    """
    Log data is used for recording log messages on GUI or in log files.
    """

    msg: str
    level: LogLevel = LogLevel.INFO

    def __post_init__(self) -> None:
        """"""
        self.time: Datetime = Datetime.now()


@dataclass(kw_only=True)
class ContractData(BaseData):
    """
    Contract data contains basic information about each contract traded.
    """

    instrument_id: str
    name: str
    product: Product
    size: float
    pricetick: float
    exchange: Exchange | None = None
    gateway_name: str | None = None

    min_volume: float = 1  # minimum order volume
    max_volume: float | None = None  # maximum order volume
    stop_supported: bool = False  # whether server supports stop order
    net_position: bool = False  # whether gateway uses net position volume
    history_data: bool = False  # whether gateway provides bar history data

    option_strike: float | None = None
    underlying_instrument_id: str | None = None
    option_type: OptionType | None = None
    option_listed: Datetime | None = None
    option_expiry: Datetime | None = None
    option_portfolio: str | None = None
    option_index: str | None = None  # for identifying options with same strike price

@dataclass(kw_only=True)
class QuoteData(BaseData):
    """
    Quote data contains information for tracking lastest status
    of a specific quote.
    """

    instrument_id: str
    quoteid: str
    exchange: Exchange | None = None
    gateway_name: str | None = None

    bid_price: float = 0.0
    bid_volume: int = 0
    ask_price: float = 0.0
    ask_volume: int = 0
    bid_offset: Offset = Offset.NONE
    ask_offset: Offset = Offset.NONE
    status: OrderStatus = OrderStatus.SUBMITTING
    datetime: Datetime | None = None
    reference: str = ""

    def is_active(self) -> bool:
        """
        Check if the quote is active.
        """
        return self.status in ACTIVE_STATUSES

    def create_cancel_request(self) -> "CancelRequest":
        """
        Create cancel request object from quote.
        """
        req: CancelRequest = CancelRequest(
            orderid=self.quoteid, instrument_id=self.instrument_id, exchange=self.exchange
        )
        return req


@dataclass(kw_only=True)
class SubscribeRequest:
    """
    Request sending to specific gateway for subscribing tick data update.
    """

    instrument_id: str
    exchange: Exchange | None = None


@dataclass(kw_only=True)
class OrderRequest:
    """
    Request sending to specific gateway for creating a new order.
    """

    instrument_id: str
    direction: Direction
    type: OrderType
    volume: float
    exchange: Exchange | None = None
    price: float = 0
    trigger_price: float = 0  # STOP类型的触发价格
    adjust_limit: float = 0
    offset: Offset = Offset.NONE
    reference: str = ""

    def create_order_data(
        self, orderid: str, gateway_name: str | None = None
    ) -> OrderData:
        """
        Create order data from request.
        """
        order: OrderData = OrderData(
            instrument_id=self.instrument_id,
            exchange=self.exchange,
            orderid=orderid,
            type=self.type,
            direction=self.direction,
            offset=self.offset,
            price=self.price,
            volume=self.volume,
            reference=self.reference,
            gateway_name=gateway_name,
            trigger_price=self.trigger_price
        )
        return order


@dataclass(kw_only=True)
class CancelRequest:
    """
    Request sending to specific gateway for canceling an existing order.
    """

    orderid: str
    instrument_id: str
    exchange: Exchange | None = None


@dataclass(kw_only=True)
class ModifyRequest:
    """
    Request sending to specific gateway for canceling an existing order.
    """

    orderid: str
    instrument_id: str
    qty: float
    price: float
    exchange: Exchange | None = None
    trigger_price: float = 0  # STOP类型的触发价格


@dataclass(kw_only=True)
class HistoryRequest:
    """
    Request sending to specific gateway for querying history data.
    """

    instrument_id: str
    start: Datetime
    exchange: Exchange | None = None
    end: Datetime | None = None
    interval: Interval | None = None

@dataclass(kw_only=True)
class QuoteRequest:
    """
    Request sending to specific gateway for creating a new quote.
    """

    instrument_id: str
    bid_price: float
    bid_volume: int
    ask_price: float
    ask_volume: int
    exchange: Exchange | None = None
    bid_offset: Offset = Offset.NONE
    ask_offset: Offset = Offset.NONE
    reference: str = ""

    def create_quote_data(
        self, quoteid: str, gateway_name: str | None = None
    ) -> QuoteData:
        """
        Create quote data from request.
        """
        quote: QuoteData = QuoteData(
            instrument_id=self.instrument_id,
            exchange=self.exchange,
            quoteid=quoteid,
            bid_price=self.bid_price,
            bid_volume=self.bid_volume,
            ask_price=self.ask_price,
            ask_volume=self.ask_volume,
            bid_offset=self.bid_offset,
            ask_offset=self.ask_offset,
            reference=self.reference,
            gateway_name=gateway_name,
        )
        return quote


T = TypeVar("T")


@dataclass(slots=True)
class FetchResult(Generic[T]):
    status: FetchStatus
    data: Optional[T] = None
    error: Optional[Exception] = None

    @property
    def ok(self) -> bool:
        return self.status == FetchStatus.SUCCESS


class RequestType(Enum):
    ORDER = "order"
    MODIFY = "modify"
    CANCEL = "cancel"


class RequestState(Enum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class Request:
    """Transport envelope for every command entering the backtest runtime."""

    type: RequestType
    data: Any
    request_id: str = field(default_factory=lambda: uuid4().hex)
    source: str = ""
    created_at: Datetime = field(default_factory=Datetime.now)


@dataclass(frozen=True, slots=True)
class RequestStatus:
    request: Request
    state: RequestState
    message: str = ""
    resource_id: str | None = None
    created_at: Datetime = field(default_factory=Datetime.now)

    @property
    def request_id(self) -> str:
        return self.request.request_id

    @property
    def request_type(self) -> RequestType:
        return self.request.type


@dataclass(slots=True)
class MarketData:
    """Canonical market record consumed by the TimeSlice backtest runtime."""

    instrument_id: str
    time: Datetime
    value: float | None = None
    exchange: Exchange | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class InstrumentStateData:
    """Complete instrument-definition snapshot effective at ``time``."""

    instrument_id: str
    time: Datetime | None
    is_active: bool
    exchange: Exchange | None = None
    multiplier: float = 1.0
    margin_rate: float = 0.0
    commission_rate: float = 0.0
    long_commission_rate: float | None = None
    short_commission_rate: float | None = None
    list_date: Datetime | None = None
    delist_date: Datetime | None = None
    attributes: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.instrument_id.strip():
            raise ValueError("instrument_id cannot be empty")
        if self.multiplier <= 0 or not math.isfinite(float(self.multiplier)):
            raise ValueError("instrument multiplier must be finite and positive")
        for name in (
            "margin_rate",
            "commission_rate",
            "long_commission_rate",
            "short_commission_rate",
        ):
            value = getattr(self, name)
            if value is not None and (value < 0 or not math.isfinite(float(value))):
                raise ValueError(f"{name} must be finite and non-negative")


@dataclass(slots=True)
class EquityStateData(InstrumentStateData):
    pass


@dataclass(slots=True)
class FutureStateData(InstrumentStateData):
    expiry: Datetime | None = None
    root_instrument_id: str | None = None


@dataclass(slots=True)
class OptionStateData(InstrumentStateData):
    underlying_instrument_id: str | None = None
    expiry: Datetime | None = None
    strike: float | None = None
    right: str | None = None
    style: str | None = None

    def __post_init__(self) -> None:
        super(OptionStateData, self).__post_init__()
        if self.strike is not None and (
            self.strike <= 0 or not math.isfinite(float(self.strike))
        ):
            raise ValueError("option strike must be finite and positive")


@dataclass(frozen=True, slots=True)
class ValuationUpdate:
    instrument_id: str
    time: Datetime
    price: float
    source: MarketData


@dataclass(slots=True)
class TradeBar(MarketData):
    interval: Interval | None = None
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0
    turnover: float = 0.0
    open_interest: float = 0.0

    def __post_init__(self) -> None:
        for field_name in (
            "open", "high", "low", "close", "volume", "turnover", "open_interest"
        ):
            _require_finite(getattr(self, field_name), field_name)
        if self.value is not None:
            _require_finite(self.value, "value")
        if self.value is None:
            self.value = self.close

    @property
    def datetime(self) -> Datetime:
        return self.time

    @property
    def open_price(self) -> float:
        return self.open

    @property
    def high_price(self) -> float:
        return self.high

    @property
    def low_price(self) -> float:
        return self.low

    @property
    def close_price(self) -> float:
        return self.close


@dataclass(slots=True)
class QuoteBar(MarketData):
    interval: Interval | None = None
    bid_open: float | None = None
    bid_high: float | None = None
    bid_low: float | None = None
    bid_close: float | None = None
    ask_open: float | None = None
    ask_high: float | None = None
    ask_low: float | None = None
    ask_close: float | None = None
    last_bid_size: float | None = None
    last_ask_size: float | None = None

    def __post_init__(self) -> None:
        if self.value is None:
            if self.bid_close is not None and self.ask_close is not None:
                self.value = (self.bid_close + self.ask_close) / 2
            else:
                self.value = self.bid_close if self.bid_close is not None else self.ask_close


@dataclass(slots=True)
class Tick(MarketData):
    tick_type: str = "trade"
    price: float | None = None
    quantity: float | None = None
    bid: float | None = None
    ask: float | None = None
    bid_size: float | None = None
    ask_size: float | None = None

    def __post_init__(self) -> None:
        if self.value is None:
            self.value = self.price


@dataclass(slots=True)
class Security(MarketData):
    """Latest runtime state for one tradable instrument."""

    source: MarketData | None = None
    is_tradable: bool = True
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: float | None = None
    turnover: float | None = None
    open_interest: float | None = None
    bid: float | None = None
    ask: float | None = None
    bid_size: float | None = None
    ask_size: float | None = None
    is_active: bool = True
    multiplier: float = 1.0
    margin_rate: float = 0.0
    commission_rate: float = 0.0
    long_commission_rate: float | None = None
    short_commission_rate: float | None = None
    list_date: Datetime | None = None
    delist_date: Datetime | None = None
    attributes: dict[str, Any] = field(default_factory=dict)

    @property
    def price(self) -> float | None:
        return self.value

    def update_market(self, data: MarketData) -> None:
        if data.instrument_id != self.instrument_id:
            raise ValueError("market data does not belong to security")
        self.source = data
        self.time = data.time
        self.exchange = data.exchange
        self.value = data.value
        if isinstance(data, TradeBar):
            self.open = data.open
            self.high = data.high
            self.low = data.low
            self.close = data.close
            self.volume = data.volume
            self.turnover = data.turnover
            self.open_interest = data.open_interest
            self.value = data.close
        elif isinstance(data, QuoteBar):
            self.bid = data.bid_close
            self.ask = data.ask_close
            self.bid_size = data.last_bid_size
            self.ask_size = data.last_ask_size
            self.value = _mid_or_one_side(data.bid_close, data.ask_close)
        elif isinstance(data, Tick):
            self.volume = data.quantity
            if data.tick_type == "quote":
                self.bid = data.bid
                self.ask = data.ask
                self.bid_size = data.bid_size
                self.ask_size = data.ask_size
                self.value = _mid_or_one_side(data.bid, data.ask)
            else:
                self.value = data.price

    def apply_state(self, state: InstrumentStateData) -> None:
        if state.instrument_id != self.instrument_id:
            raise ValueError("instrument state does not belong to security")
        self.is_active = state.is_active
        self.is_tradable = state.is_active
        self.exchange = state.exchange
        self.multiplier = state.multiplier
        self.margin_rate = state.margin_rate
        self.commission_rate = state.commission_rate
        self.long_commission_rate = state.long_commission_rate
        self.short_commission_rate = state.short_commission_rate
        self.list_date = state.list_date
        self.delist_date = state.delist_date
        self.attributes = dict(state.attributes)
        if state.time is not None:
            self.time = state.time

    def get_trade_bar(self, *, time: Datetime | None = None) -> TradeBar | None:
        if not isinstance(self.source, TradeBar):
            return None
        if time is not None and self.source.time != time:
            return None
        return self.source


@dataclass(slots=True)
class EquitySecurity(Security):
    pass


@dataclass(slots=True)
class FutureContract(Security):
    expiry: Datetime | None = None
    root_instrument_id: str | None = None

    def apply_state(self, state: InstrumentStateData) -> None:
        if not isinstance(state, FutureStateData):
            raise TypeError("FutureContract requires FutureStateData")
        super(FutureContract, self).apply_state(state)
        self.expiry = state.expiry
        self.root_instrument_id = state.root_instrument_id


@dataclass(slots=True)
class OptionContract(Security):
    underlying_instrument_id: str | None = None
    expiry: Datetime | None = None
    strike: float | None = None
    right: str | None = None
    style: str | None = None
    iv: float | None = None
    delta: float | None = None
    gamma: float | None = None
    vega: float | None = None
    theta: float | None = None

    def apply_state(self, state: InstrumentStateData) -> None:
        if not isinstance(state, OptionStateData):
            raise TypeError("OptionContract requires OptionStateData")
        super(OptionContract, self).apply_state(state)
        self.underlying_instrument_id = state.underlying_instrument_id
        self.expiry = state.expiry
        self.strike = state.strike
        self.right = state.right
        self.style = state.style


@dataclass(slots=True)
class CustomData(MarketData):
    custom_type: str = ""
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class OptionChain(MarketData):
    canonical_instrument_id: str = ""
    underlying_instrument_id: str = ""
    underlying_price: float | None = None
    contracts: dict[str, OptionContract] = field(default_factory=dict)
    filtered_contracts: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.canonical_instrument_id = self.canonical_instrument_id or self.instrument_id


@dataclass(slots=True)
class FuturesChain(MarketData):
    canonical_instrument_id: str = ""
    root_instrument_id: str = ""
    contracts: dict[str, FutureContract] = field(default_factory=dict)
    mapped_contract: str | None = None

    def __post_init__(self) -> None:
        self.canonical_instrument_id = self.canonical_instrument_id or self.instrument_id


@dataclass(slots=True)
class Slice:
    """A complete market snapshot visible at a single end time."""

    time: Datetime | None
    bars: dict[str, dict[str, TradeBar]] = field(default_factory=dict)
    quote_bars: dict[str, dict[str, QuoteBar]] = field(default_factory=dict)
    ticks: dict[str, dict[str, list[Tick]]] = field(default_factory=dict)
    custom_data: dict[str, dict[str, list[CustomData]]] = field(default_factory=dict)
    option_chains: dict[str, dict[str, OptionChain]] = field(default_factory=dict)
    futures_chains: dict[str, dict[str, FuturesChain]] = field(default_factory=dict)
    all_data: list[Any] = field(default_factory=list)
    _primary_bars: dict[str, TradeBar] = field(default_factory=dict)

    @property
    def has_data(self) -> bool:
        return bool(self.all_data)

    @property
    def bar_list(self) -> list[TradeBar]:
        return sorted(
            self._primary_bars.values(),
            key=lambda bar: (bar.instrument_id, _interval_sort_value(bar.interval), bar.time),
        )

    def get_bar(self, instrument_id: str, data_name: str | None = None) -> TradeBar | None:
        if data_name is not None:
            return self.bars.get(data_name, {}).get(instrument_id)
        return self._primary_bars.get(instrument_id)

    def contains_data(self, data_name: str) -> bool:
        return any(
            data_name in index
            for index in (
                self.bars,
                self.quote_bars,
                self.ticks,
                self.custom_data,
                self.option_chains,
                self.futures_chains,
            )
        )

    def _index(self, data_name: str, data: Any) -> None:
        if isinstance(data, TradeBar):
            self.bars.setdefault(data_name, {})[data.instrument_id] = data
            current = self._primary_bars.get(data.instrument_id)
            if current is None or _interval_sort_value(current.interval) > _interval_sort_value(data.interval):
                self._primary_bars[data.instrument_id] = data
        elif isinstance(data, QuoteBar):
            self.quote_bars.setdefault(data_name, {})[data.instrument_id] = data
        elif isinstance(data, Tick):
            self.ticks.setdefault(data_name, {}).setdefault(data.instrument_id, []).append(data)
        elif isinstance(data, CustomData):
            self.custom_data.setdefault(data_name, {}).setdefault(data.instrument_id, []).append(data)
        elif isinstance(data, OptionChain):
            self.option_chains.setdefault(data_name, {})[data.canonical_instrument_id] = data
        elif isinstance(data, FuturesChain):
            self.futures_chains.setdefault(data_name, {})[data.canonical_instrument_id] = data

    @classmethod
    def from_named_data(
        cls,
        when: Datetime,
        data: Iterable[tuple[str, Any]],
    ) -> "Slice":
        named = data if isinstance(data, list) else list(data)
        slice_ = cls(time=when, all_data=[item for _, item in named])
        for data_name, item in named:
            slice_._index(data_name, item)
        return slice_


@dataclass(slots=True)
class TimeSlice:
    time: Datetime | None
    slice: Slice
    security_updates: tuple[MarketData | InstrumentStateData, ...] = ()
    valuation_updates: tuple[ValuationUpdate, ...] = ()
    is_bootstrap: bool = False


def _interval_sort_value(interval: Interval | None) -> float:
    if interval is None:
        return float("inf")
    try:
        return float(interval.value)
    except (TypeError, ValueError):
        return float("inf")


def _require_finite(value: float, field_name: str) -> None:
    if not math.isfinite(float(value)):
        raise ValueError(f"{field_name} must be finite, got {value!r}")


def _mid_or_one_side(
    bid: float | None,
    ask: float | None,
) -> float | None:
    if bid is not None and ask is not None:
        return (bid + ask) / 2
    return bid if bid is not None else ask
