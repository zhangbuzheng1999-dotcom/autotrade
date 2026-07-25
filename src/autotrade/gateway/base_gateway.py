from abc import ABC, abstractmethod
from autotrade.engine.event_engine import Event, EventEngine
from autotrade.engine.event_engine import (
    COMMAND_ORDER_CANCEL,
    COMMAND_ORDER_MODIFY,
    COMMAND_ORDER_SUBMIT,
    EVENT_LIVE_DATA,
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_POSITION_SNAPSHOT,
    EVENT_ACCOUNT,
    EVENT_LOG,
    EVENT_QUOTE,
    Message,
)
from autotrade.coreutils.object import (
    TickData,
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    ContractData,
    LogData,
    QuoteData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest,
    QuoteRequest,
    Exchange,
    BarData, ModifyRequest
)
from autotrade.coreutils.constant import LogLevel

class BaseGateway(ABC):
    """
    Abstract gateway class for creating gateways connection
    to different trading systems.

    # How to implement a gateway:

    ---
    ## Basics
    A gateway should satisfies:
    * this class should be thread-safe:
        * all methods should be thread-safe
        * no mutable shared properties between objects.
    * all methods should be non-blocked
    * satisfies all requirements written in docstring for every method and callbacks.
    * automatically reconnect if connection lost.

    ---
    ## methods must implements:
    all @abstractmethod

    ---
    ## callbacks must response manually:
    * on_tick
    * on_trade
    * on_order
    * on_position
    * on_account
    * on_contract

    All the XxxData passed to callback should be constant, which means that
        the object should not be modified after passing to on_xxxx.
    So if you use a cache to store reference of data, use copy.copy to create a new object
    before passing that data into on_xxxx



    """

    # Default name for the gateway.
    default_name: str = ""

    # Fields required in setting dict for connect function.
    default_setting: dict[str, str | int | float | bool] = {}

    # Exchanges supported in the gateway.
    exchanges: list[Exchange] = []

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """"""
        self.event_engine: EventEngine = event_engine
        self.gateway_name: str = gateway_name
        self._execution_bound = False

    def on_event(self, type: str, data: object = None) -> None:
        """
        General event push.
        """
        event: Event = Event(type, data)
        self.event_engine.put(event)

    @property
    def execution_routes(self):
        """Commands consumed directly by every trading gateway."""
        return (
            (COMMAND_ORDER_SUBMIT, self._process_order_command),
            (COMMAND_ORDER_CANCEL, self._process_cancel_command),
            (COMMAND_ORDER_MODIFY, self._process_modify_command),
        )

    def bind_execution(self) -> None:
        if self._execution_bound:
            return
        for name, handler in self.execution_routes:
            self.event_engine.register_command("execution", name, handler)
        self._execution_bound = True

    def unbind_execution(self) -> None:
        if not self._execution_bound:
            return
        for name, handler in self.execution_routes:
            self.event_engine.unregister_command("execution", name, handler)
        self._execution_bound = False

    def _process_order_command(self, message: Message) -> None:
        self.send_order(message.data)

    def _process_cancel_command(self, message: Message) -> None:
        self.cancel_order(message.data)

    def _process_modify_command(self, message: Message) -> None:
        self.modify_order(message.data)


    def on_tick(self, tick: TickData) -> None:
        """Publish market data through the unified data event."""
        self.on_event(EVENT_LIVE_DATA, tick)

    def on_trade(self, trade: TradeData) -> None:
        """Publish a confirmed fill."""
        self.on_event(EVENT_TRADE, trade)

    def on_order(self, order: OrderData) -> None:
        """Publish the latest order state."""
        self.on_event(EVENT_ORDER, order)

    def on_position(self, position: PositionData) -> None:
        """Publish a broker position snapshot for OMS reconciliation."""
        self.on_event(EVENT_POSITION_SNAPSHOT, position)

    def on_account(self, account: AccountData) -> None:
        """Publish the latest account state."""
        self.on_event(EVENT_ACCOUNT, account)

    def on_quote(self, quote: QuoteData) -> None:
        """Publish the latest quote state."""
        self.on_event(EVENT_QUOTE, quote)

    def on_log(self, log: LogData) -> None:
        """
        Log event push.
        """
        self.on_event(EVENT_LOG, log)

    def on_contract(self, contract: ContractData) -> None:
        """Publish instrument definitions through the unified data event."""
        self.on_event(EVENT_LIVE_DATA, contract)

    def write_log(self, msg: str,level:LogLevel) -> None:
        """
        Write a log event from gateway.
        """
        log: LogData = LogData(msg=msg,level=level)
        self.on_log(log)

    @abstractmethod
    def connect(self, setting: dict) -> None:
        """
        Start gateway connection.

        to implement this method, you must:
        * connect to server if necessary
        * log connected if all necessary connection is established
        * do the following query and response corresponding on_xxxx and write_log
            * contracts : on_contract
            * account asset : on_account
            * account holding: on_position
            * orders of account: on_order
            * trades of account: on_trade
        * if any of query above is failed,  write log.

        future plan:
        response callback/change status instead of write_log

        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Close gateway connection.
        """
        pass

    @abstractmethod
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        Subscribe tick data update.
        """
        pass

    @abstractmethod
    def send_order(self, req: OrderRequest) -> str:
        """
        Send a new order to server.

        implementation should finish the tasks blow:
        * create an OrderData from req using OrderRequest.create_order_data
        * assign a unique(gateway instance scope) id to OrderData.orderid
        * send request to server
            * if request is sent, OrderData.status should be set to Status.SUBMITTING
            * if request is failed to sent, OrderData.status should be set to Status.REJECTED
        * response on_order:
        * return orderid

        :return local orderid for created OrderData
        """
        pass

    @abstractmethod
    def cancel_order(self, req: CancelRequest) -> None:
        """
        Cancel an existing order.
        implementation should finish the tasks blow:
        * send request to server
        """
        pass

    def modify_order(self, req: ModifyRequest) -> None:
        pass

    def send_quote(self, req: QuoteRequest) -> str:
        """
        Send a new two-sided quote to server.

        implementation should finish the tasks blow:
        * create an QuoteData from req using QuoteRequest.create_quote_data
        * assign a unique(gateway instance scope) id to QuoteData.quoteid
        * send request to server
            * if request is sent, QuoteData.status should be set to Status.SUBMITTING
            * if request is failed to sent, QuoteData.status should be set to Status.REJECTED
        * response on_quote:
        * return quoteid

        :return local quoteid for created QuoteData
        """
        return ""

    def cancel_quote(self, req: CancelRequest) -> None:
        """
        Cancel an existing quote.
        implementation should finish the tasks blow:
        * send request to server
        """
        return

    @abstractmethod
    def query_account(self) -> None:
        """
        Query account balance.
        """
        pass

    @abstractmethod
    def query_position(self) -> None:
        """
        Query holding positions.
        """
        pass

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """
        Query bar history data.
        """
        return []

    def get_default_setting(self) -> dict[str, str | int | float | bool]:
        """
        Return default setting dict.
        """
        return self.default_setting
