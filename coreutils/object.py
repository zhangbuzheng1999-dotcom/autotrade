"""
Basic data structure used for general trading function in the trading platform.
"""

from dataclasses import dataclass, field
from datetime import datetime as Datetime
from collections import OrderedDict
from coreutils.constant import Direction, Exchange, Interval, Offset, OrderStatus, Product, OptionType, OrderType,LogLevel

INFO: int = 20

ACTIVE_STATUSES = set([OrderStatus.SUBMITTING, OrderStatus.NOTTRADED,
                       OrderStatus.PARTTRADED, OrderStatus.PENDING,
                       OrderStatus.UNKNOWN, OrderStatus.MODIFIED])


@dataclass
class BaseData:
    """
    Any data object needs a gateway_name as source
    and should inherit base data.
    """

    gateway_name: str

    extra: dict | None = field(default=None, init=False)


@dataclass
class TickData(BaseData):
    """
    Tick data contains information about:
        * last trade in market
        * orderbook snapshot
        * intraday market statistics.
    """

    symbol: str
    exchange: Exchange
    datetime: Datetime

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

    def __post_init__(self) -> None:
        """"""
        self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"


@dataclass
class BarData(BaseData):
    """
    Candlestick bar data of a certain trading period.
    """

    symbol: str
    exchange: Exchange
    datetime: Datetime

    interval: Interval | None = None
    volume: float = 0
    turnover: float = 0
    open_interest: float = 0
    open_price: float = 0
    high_price: float = 0
    low_price: float = 0
    close_price: float = 0

    def __post_init__(self) -> None:
        """"""
        self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"


@dataclass
class OrderData(BaseData):
    """
    Order data contains information for tracking lastest status
    of a specific order.
    """

    symbol: str
    exchange: Exchange
    orderid: str

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

    def __post_init__(self) -> None:
        """"""
        self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"
        self.vt_orderid: str = f"{self.gateway_name}.{self.orderid}"

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
            orderid=self.orderid, symbol=self.symbol, exchange=self.exchange
        )
        return req

    def to_dict(self) -> dict:
        """安全序列化为 dict"""
        return {
            "symbol": self.symbol,
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
            "vt_symbol": self.vt_symbol,
            "vt_orderid": self.vt_orderid,
        }

@dataclass
class TradeData(BaseData):
    """
    Trade data contains information of a fill of an order. One order
    can have several trade fills.
    """

    symbol: str
    exchange: Exchange
    orderid: str
    tradeid: str
    direction: Direction | None = None

    offset: Offset = Offset.NONE
    price: float = 0
    datetime: Datetime | None = None
    traded: float = 0  # 成交数量
    volume: float = 0  # 总订单数量
    avgFillPrice: float = 0  # 成交均价
    status: OrderStatus | None = None
    reference: str = ""

    def __post_init__(self) -> None:
        """"""
        self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"
        self.vt_orderid: str = f"{self.gateway_name}.{self.orderid}"
        self.vt_tradeid: str = f"{self.gateway_name}.{self.tradeid}"

    def is_active(self) -> bool:
        """
        Check if the order is active.
        """
        return self.status in ACTIVE_STATUSES

    def to_dict(self) -> dict:
        """安全序列化为 dict"""
        return {
            "symbol": self.symbol,
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
            "vt_symbol": self.vt_symbol,
            "vt_orderid": self.vt_orderid,
            "vt_tradeid": self.vt_tradeid,
        }


@dataclass
class PositionData(BaseData):
    """
    Position data is used for tracking each individual position holding.
    """

    symbol: str
    exchange: Exchange
    direction: Direction

    contract_symbol: str | None = None  # 期货具体合约
    volume: float = 0
    frozen: float = 0
    price: float = 0
    pnl: float = 0
    yd_volume: float = 0
    margin: float = 0

    def __post_init__(self) -> None:
        """"""
        self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"
        self.vt_positionid: str = f"{self.gateway_name}.{self.vt_symbol}.{self.direction.value}"

    def to_dict(self) -> dict:
        """安全序列化为 dict"""
        return {
            "symbol": self.symbol,
            "exchange": self.exchange.value if self.exchange else None,
            "direction": self.direction.value if self.direction else None,
            "volume": self.volume,
            "frozen": self.frozen,
            "price": self.price,
            "pnl": self.pnl,
            "yd_volume": self.yd_volume,
            "margin": self.margin,
            "vt_symbol": self.vt_symbol,
            "vt_positionid": self.vt_positionid,
        }


@dataclass
class AccountData(BaseData):
    """
    Account data contains information about balance, frozen and
    available.
    """

    accountid: str
    balance: float = 0
    frozen: float = 0.0
    cash: float = 0.0
    margin: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    equity: float = 0.0
    available: float = 0.0

    def __post_init__(self) -> None:
        """"""
        self.vt_accountid: str = f"{self.gateway_name}.{self.accountid}"


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

@dataclass
class ContractData(BaseData):
    """
    Contract data contains basic information about each contract traded.
    """

    symbol: str
    exchange: Exchange
    name: str
    product: Product
    size: float
    pricetick: float

    min_volume: float = 1  # minimum order volume
    max_volume: float | None = None  # maximum order volume
    stop_supported: bool = False  # whether server supports stop order
    net_position: bool = False  # whether gateway uses net position volume
    history_data: bool = False  # whether gateway provides bar history data

    option_strike: float | None = None
    option_underlying: str | None = None  # vt_symbol of underlying contract
    option_type: OptionType | None = None
    option_listed: Datetime | None = None
    option_expiry: Datetime | None = None
    option_portfolio: str | None = None
    option_index: str | None = None  # for identifying options with same strike price

    def __post_init__(self) -> None:
        """"""
        self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"


@dataclass
class QuoteData(BaseData):
    """
    Quote data contains information for tracking lastest status
    of a specific quote.
    """

    symbol: str
    exchange: Exchange
    quoteid: str

    bid_price: float = 0.0
    bid_volume: int = 0
    ask_price: float = 0.0
    ask_volume: int = 0
    bid_offset: Offset = Offset.NONE
    ask_offset: Offset = Offset.NONE
    status: OrderStatus = OrderStatus.SUBMITTING
    datetime: Datetime | None = None
    reference: str = ""

    def __post_init__(self) -> None:
        """"""
        self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"
        self.vt_quoteid: str = f"{self.gateway_name}.{self.quoteid}"

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
            orderid=self.quoteid, symbol=self.symbol, exchange=self.exchange
        )
        return req


@dataclass
class SubscribeRequest:
    """
    Request sending to specific gateway for subscribing tick data update.
    """

    symbol: str
    exchange: Exchange

    def __post_init__(self) -> None:
        """"""
        self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"


@dataclass
class OrderRequest:
    """
    Request sending to specific gateway for creating a new order.
    """

    symbol: str
    exchange: Exchange
    direction: Direction
    type: OrderType
    volume: float
    price: float = 0
    trigger_price: float = 0  # STOP类型的触发价格
    adjust_limit: float = 0
    offset: Offset = Offset.NONE
    reference: str = ""

    def __post_init__(self) -> None:
        """"""
        self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"

    def create_order_data(self, orderid: str, gateway_name: str) -> OrderData:
        """
        Create order data from request.
        """
        order: OrderData = OrderData(
            symbol=self.symbol,
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


@dataclass
class CancelRequest:
    """
    Request sending to specific gateway for canceling an existing order.
    """

    orderid: str
    symbol: str
    exchange: Exchange

    def __post_init__(self) -> None:
        """"""
        self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"


@dataclass
class ModifyRequest:
    """
    Request sending to specific gateway for canceling an existing order.
    """

    orderid: str
    symbol: str
    qty: float
    price: float
    exchange: Exchange
    trigger_price: float = 0  # STOP类型的触发价格

    def __post_init__(self) -> None:
        """"""
        self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"


@dataclass
class HistoryRequest:
    """
    Request sending to specific gateway for querying history data.
    """

    symbol: str
    exchange: Exchange
    start: Datetime
    end: Datetime | None = None
    interval: Interval | None = None

    def __post_init__(self) -> None:
        """"""
        self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"


@dataclass
class QuoteRequest:
    """
    Request sending to specific gateway for creating a new quote.
    """

    symbol: str
    exchange: Exchange
    bid_price: float
    bid_volume: int
    ask_price: float
    ask_volume: int
    bid_offset: Offset = Offset.NONE
    ask_offset: Offset = Offset.NONE
    reference: str = ""

    def __post_init__(self) -> None:
        """"""
        self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"

    def create_quote_data(self, quoteid: str, gateway_name: str) -> QuoteData:
        """
        Create quote data from request.
        """
        quote: QuoteData = QuoteData(
            symbol=self.symbol,
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
