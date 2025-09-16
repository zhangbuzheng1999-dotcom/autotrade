import threading
import pandas as pd
from futu import (TickerHandlerBase, SubType, OpenQuoteContext, OpenSecTradeContext,
                  OpenFutureTradeContext, SecurityFirm, RET_OK, RET_ERROR, TrdEnv, ModifyOrderOp, TradeOrderHandlerBase,
                  )
from engine.event_engine import EventEngine, EVENT_TICK, EVENT_BAR, EVENT_TRADE, EVENT_ORDER,EVENT_POSITION
from gateway.base_gateway import BaseGateway
from coreutils.object import (TickData, BarData, TradeData, Exchange, ModifyRequest, HistoryRequest, CancelRequest,
                              OrderData,
                              OrderRequest, PositionData)
from coreutils.constant import Interval, Direction, OrderType, OrderStatus,LogLevel
from conn import klinepubsub
from coreutils.config import FutuInfo
from coreutils.exceptions import FutuException
from functools import partial
import uuid
from data.db_query import fetch_kline


class FutuTickHandler(TickerHandlerBase):
    """Tick 数据回调处理器，转发给 Gateway"""

    def __init__(self, gateway):
        super().__init__()
        self.gateway = gateway

    def on_recv_rsp(self, rsp_str):
        ret_code, data = super(FutuTickHandler, self).on_recv_rsp(rsp_str)
        if ret_code == RET_OK:
            self.gateway.on_tick(data)  # 转发给 Gateway
        return ret_code, data


class FutuOrderHandler(TradeOrderHandlerBase):
    """订单状态回调处理器，推送标准化事件"""

    def __init__(self, gateway):
        super().__init__()
        self.gateway = gateway

    def on_recv_rsp(self, rsp_pb):
        ret, content = super(FutuOrderHandler, self).on_recv_rsp(rsp_pb)
        if ret == RET_OK:
            for _, row in content.iterrows():
                self.gateway.on_order_update(row)
        return ret, content


class FutuGateway(BaseGateway):
    default_name = "FUTU"

    def __init__(self, event_engine: EventEngine, gateway_name: str = "Futu", security_firm='futures'):
        super().__init__(event_engine, gateway_name)
        self.security_firm = security_firm
        self.kline_clients = []
        self.trd_env = TrdEnv.SIMULATE
        # self.tick_clients = []

        self._lock = threading.Lock()  # 线程锁，保证多线程安全
        self._pending_orders: dict[str, OrderRequest] = {}  # 保存尚未触发的本地挂单，结构：{local_id: req}
        self._lock = threading.Lock()  # 线程锁，保证多线程安全
        self._order_map: dict[str, OrderData] = {}
        self._persistent_tick_symbols: list = []  # tick数据订阅列表,此列表上的symbol即使不存在pending_order也会持续订阅

    def connect(self, setting: dict):
        """连接 Futu OpenD 并注册回调
            setting = {'symbols':[xxx,yyy],'interval':[Interval.K_1M,Interval.K_5M,Interval.K_15M]},
        """

        # 行情
        self.quote_ctx = OpenQuoteContext(host=FutuInfo.host, port=FutuInfo.port)
        self.quote_ctx.set_handler(FutuTickHandler(self))

        # 交易
        if self.security_firm == "futures":
            self.trd_ctx = OpenFutureTradeContext(
                host=FutuInfo.host,
                port=FutuInfo.port,
                security_firm=SecurityFirm.FUTUSECURITIES
            )
        else:
            self.trd_ctx = OpenSecTradeContext(
                host=FutuInfo.host,
                port=FutuInfo.port,
                is_encrypt=None,
                security_firm=SecurityFirm.FUTUSECURITIES  # TODO: 股票交易接口尚未验证
            )

        self.trd_ctx.set_handler(FutuOrderHandler(self))

        symbols = setting.get("symbols")
        intervals = setting.get("intervals")
        if not symbols or not intervals:
            raise FutuException("setting 必须有symbols和intervals")

        if not isinstance(symbols, list) or not isinstance(intervals, list):
            raise FutuException("symbols和intervals 必须为list")

        if not isinstance(symbols[0], str):
            raise FutuException("symbol里面元素必须为str")

        if not isinstance(intervals[0], Interval):
            raise FutuException('intervals 里面元素必须为Interval')

        intervals_bar = [interval for interval in intervals if interval != Interval.TICK]
        intervals_tick = [interval for interval in intervals if interval == Interval.TICK]

        if len(intervals_bar) > 0:
            self.subscribe_bar(symbols, intervals_bar)

        if len(intervals_tick) > 0:
            self._persistent_tick_symbols.append(symbols)
            self.subscribe_tick(symbols)

    def subscribe_bar(self, symbols: list[str], intervals: list[Interval]):
        """
        setting 参数来自 vn.py UI 或 config.json
        例如：
        {
            "symbols": ["HK.MHImian"],
            "interval": "15m"
        }
        """

        sub_kline = klinepubsub.MultiKlineSubscriber("127.0.0.1", 20250, intervals)
        sub_list = [(symbol, interval) for symbol in symbols for interval in intervals]
        for symbol, interval in sub_list:
            sub_kline.subscribe(interval, partial(self.on_kline, interval=interval))
            self.write_log(f"FutuGateway 已启动，订阅 {symbol} 的 {interval} K线",level=LogLevel.INFO)

    def subscribe_tick(self, symbols: list):
        ret, msg = self.quote_ctx.subscribe(symbols, [SubType.TICKER])
        if ret == RET_ERROR:
            self.write_log(f"subscribe_tick ERROR,symbols:{symbols},{msg}",level=LogLevel.ERROR)
        else:
            self.write_log(f"subscribe_tick 成功:{symbols},{msg}",level=LogLevel.INFO)

    def unsubscribe_tick(self, symbols: list):
        ret, msg = self.quote_ctx.unsubscribe(symbols, [SubType.TICKER])
        if ret == RET_ERROR:
            self.write_log(f"unsubscribe_tick ERROR,symbols:{symbols},{msg}",level=LogLevel.ERROR)
        else:
            self.write_log(f"unsubscribe_tick 成功:{symbols},{msg}",level=LogLevel.INFO)

    def on_tick(self, df):
        if df.empty:
            return

        last = df.iloc[-1]
        open = df.iloc[0]['price']
        high = max(df['price'])
        low = min(df['price'])
        close = df.iloc[-1]['price']

        tick = TickData(
            symbol=last["code"],
            exchange=Exchange.UNKNOWN,  # 或 Exchange.HKEX，看你的数据
            datetime=pd.to_datetime(last["time"]),
            last_price=float(last["price"]),
            gateway_name=self.gateway_name,
            open_price=open,
            high_price=high,
            low_price=low,
            close_price=close,
        )

        self._monitor_loop(tick)
        self.on_event(EVENT_TICK, tick)
        self.on_event(EVENT_TICK + tick.symbol, tick)

    def get_order_from_map(self, order_id=None, broker_id=None):
        # futu的orderid(broker_id)与本地的order_id相互转换
        # 输入broker_id出order_id,反之亦然
        if (order_id is None and broker_id is None) or (order_id is not None and broker_id is not None):
            raise AttributeError("order_id与broker_id有且只能有一个空值")

        if order_id is not None:
            return self._order_map.get(order_id)  # 没有时返回 None
        else:
            for k, order in self._order_map.items():
                if order.broker_orderid == f"{broker_id}":
                    return order
            return None

    @staticmethod
    def convert_order_direction(direction=None, futu_direction=None):
        mapping = {
            "BUY": Direction.LONG,
            "SELL": Direction.SHORT
        }
        if direction is not None:
            return [k for k, v in mapping.items() if v == direction][0]
        else:
            return mapping[futu_direction]

    @staticmethod
    def convert_order_type(order_type=None, futu_order_type=None):

        mapping = {
            OrderType.MARKET: "MARKET",
            OrderType.LIMIT: "NORMAL",
            OrderType.STP_LMT: "NORMAL",
            OrderType.STP_MKT: "MARKET",
        }
        if futu_order_type is not None:
            return [k for k, v in mapping.items() if v == futu_order_type][0]
        else:
            return mapping[order_type]

    def on_order_update(self, df):
        broker_id = f'{df['order_id']}'
        pre_order = self.get_order_from_map(broker_id=broker_id)
        # 排除不在self._order_map中的订单
        # 由于futu采用回调,发送订单时on_order_update会先于_send_order中res_order,此时self._order_map还没有映射
        # 因此刚开始提交futu的订单状态并不由on_order_update返回,而是在self._send_order中返回
        if pre_order is None:
            return
        order_status = self.convert_order_status(df["order_status"])
        order_type = self.convert_order_type(futu_order_type=df['order_type'])
        direction = self.convert_order_direction(futu_direction=df["trd_side"])
        volume = df['qty']
        traded = df['dealt_qty']
        price = df['price']
        avgfillprice = df['dealt_avg_price']
        code = df['code']
        reference = df['remark']
        trade_time = pd.Timestamp.now()
        # 和下订单symbol、exchange保持一致
        order_data = OrderData(symbol=pre_order.symbol, exchange=pre_order.exchange, orderid=pre_order.orderid,
                               type=order_type,
                               direction=direction, volume=volume, price=price, gateway_name='FUTU',
                               status=order_status, traded=traded, avgFillPrice=avgfillprice,
                               broker_orderid=broker_id, reference=reference, datetime=trade_time)
        self.on_order(order_data)
        if order_status in [OrderStatus.ALLTRADED, OrderStatus.PARTTRADED]:
            trade_data = TradeData(symbol=code, exchange=Exchange.HKFE, orderid=pre_order.orderid, tradeid=broker_id,
                                   direction=direction, volume=volume, price=price, gateway_name='FUTU',
                                   status=order_status, traded=traded, avgFillPrice=avgfillprice, reference=reference,
                                   datetime=trade_time)
            self.on_trade(trade_data)
            position_data = PositionData(symbol=code,
                                         exchange=Exchange.HKFE,
                                         direction=direction, volume=traded, gateway_name='FUTU')
            self.on_position(position_data)

    @staticmethod
    def convert_order_status(status: str) -> OrderStatus:
        mapping = {
            "WAITING_SUBMIT": OrderStatus.SUBMITTING,  # 待提交
            "SUBMITTING": OrderStatus.SUBMITTING,  # 提交中
            "SUBMITTED": OrderStatus.NOTTRADED,  # 已提交，未成交
            "FILLED_PART": OrderStatus.PARTTRADED,  # 部分成交
            "FILLED_ALL": OrderStatus.ALLTRADED,  # 全部成交
            "CANCELLED_PART": OrderStatus.PARTCANCELLED,  # 部分成交，剩余撤单
            "CANCELLED_ALL": OrderStatus.ALLCANCELLED,  # 全部撤单
            "FAILED": OrderStatus.REJECTED,  # 下单失败
            "DISABLED": OrderStatus.REJECTED,  # 已失效
            "DELETED": OrderStatus.ALLCANCELLED  # 已删除
        }
        return mapping.get(status, OrderStatus.UNKNOWN)

    def on_kline(self, df, interval: Interval):
        if df.empty:
            return
        last = df.iloc[-1]

        bar = BarData(
            symbol=last["code"],
            exchange=Exchange.HKFE,
            datetime=pd.to_datetime(last["trade_time"]),
            interval=interval,  # 直接用绑定的 Interval
            open_price=float(last["open"]),
            high_price=float(last["high"]),
            low_price=float(last["low"]),
            close_price=float(last["close"]),
            volume=float(last["volume"]),
            gateway_name=self.gateway_name
        )

        self.on_event(EVENT_BAR, bar)
        self.on_event(EVENT_BAR + bar.symbol + str(bar.interval.value), bar)

    def on_trade(self, trade: TradeData) -> None:
        """
        Trade event push.
        Trade event of a specific vt_symbol is also pushed.
        """
        self.on_event(EVENT_TRADE, trade)
        self.on_event(EVENT_TRADE + trade.vt_symbol, trade)

    def on_order(self, order: OrderData) -> None:
        """
        Order event push.
        Order event of a specific vt_orderid is also pushed.
        """

        self.on_event(EVENT_ORDER, order)
        self.on_event(EVENT_ORDER + order.vt_orderid, order)

    def close(self):
        self.trd_ctx.close()
        self.quote_ctx.close()
        self.write_log("FutuGateway 已关闭",level=LogLevel.INFO)

    # 以下是空实现，因为目前只做行情
    def subscribe(self, req):
        pass

    def send_order(self, req):
        """
        发送订单的逻辑与取消或者修改订单的逻辑不同,发送订单的on_order由本身触发，而其他修改取消订单会等到回调
        """
        # todo 这里存在一个潜在bug,假设刚提交订单此时res_order还没由futu返回，
        #  但是立马成交,回调on_order_update早于自身的on_order触发，此时self._order_map由没有对应订单信息，就会错过这条更新，
        #  后续需要加上每次发送收到res_order后强制查询并且调用on_order
        with self._lock:
            local_id = uuid.uuid4().hex[:6]  # 生成本地订单 ID
            if req.type in [OrderType.MARKET, OrderType.LIMIT]:
                res_order = self._send_order_futu(local_id, req)
            else:
                res_order = OrderData(symbol=req.symbol, exchange=req.exchange, orderid=local_id, type=req.type,
                                      direction=req.direction, volume=req.volume, price=req.price, gateway_name='FUTU',
                                      broker_orderid=None, status=OrderStatus.PENDING, trigger_price=req.trigger_price,
                                      reference=req.reference, datetime=pd.Timestamp.now())
                self._pending_orders[local_id] = req
                # 更新tick订阅
                self._update_monitoring_tick_subscriptions(sub=True, symbol=req.symbol)
            self._order_map[res_order.orderid] = res_order
        self.on_order(res_order)

    def _update_monitoring_tick_subscriptions(self, sub: bool, symbol: str):
        """
        此函数专门处理条件订单带来的tick订阅问题
        :param sub: True代表执行订阅操作,False为取消订阅
        :param symbol: 对于的symbol
        """
        if sub:
            self.subscribe_tick([symbol])
        else:
            pending_symbol = [req.symbol for req in self._pending_orders.values()]
            # 只有当self._pending_orders也没有对于symbol且也没有持续订阅tick时才会取消tick订阅
            if symbol not in pending_symbol and symbol not in self._persistent_tick_symbols:
                self.unsubscribe_tick([symbol])

    def _monitor_loop(self, tick: TickData):
        """
        持续监控挂单触发条件：
        - 当当前价格满足条件时，通过 futu API 真正下单
        - 更新对应的 OrderInfo，并移除 pending 订单
        """
        current_price = tick.last_price  # 从最新数据中取价格
        for local_id, req in list(self._pending_orders.items()):
            if req.symbol != tick.symbol:  # 只检查同一个标的
                continue
            if self._should_trigger(req=req, current_price=current_price):
                req.type = OrderType.MARKET if req.type == OrderType.STP_MKT else OrderType.LIMIT
                res_order = self._send_order_futu(local_id, req)
                with self._lock:
                    del self._pending_orders[local_id]
                    self._order_map[res_order.orderid] = res_order
                    # 更新tick订阅
                    self._update_monitoring_tick_subscriptions(sub=False, symbol=req.symbol)
                self.on_order(res_order)

    def _send_order_futu(self, local_id, req: OrderRequest):
        res_order = OrderData(symbol=req.symbol, exchange=req.exchange, orderid=local_id, type=req.type,
                              direction=req.direction, volume=req.volume, price=req.price, gateway_name='FUTU',
                              status=OrderStatus.SUBMITTING, reference=req.reference, datetime=pd.Timestamp.now())
        ret_unlock, res_unlock = self.trd_ctx.unlock_trade(FutuInfo.pwd_unlock)
        if ret_unlock != RET_OK:
            res_order.status = OrderStatus.REJECTED

        trd_side = 'BUY' if req.direction == Direction.LONG else 'SELL'
        order_type = 'MARKET' if req.type in [OrderType.MARKET, OrderType.STP_MKT] else 'NORMAL'
        ret, data = self.trd_ctx.place_order(
            price=req.price,
            qty=req.volume,
            code=req.symbol,
            trd_side=trd_side,
            order_type=order_type,
            trd_env=self.trd_env,
            adjust_limit=req.adjust_limit,
            remark=req.reference
        )
        if ret != RET_OK:
            res_order.status = OrderStatus.REJECTED
        else:
            res_order.broker_orderid = data.iloc[0]['order_id']
        return res_order

    @staticmethod
    def _should_trigger(req: OrderRequest, current_price):
        """
        判断挂单是否应当触发。
        :param req: OrderRequest 对象
        :param current_price: 当前价格
        :return: True/False
        """
        trd_side = req.direction
        trigger_price = req.trigger_price
        order_type = req.type

        if order_type in [OrderType.STP_LMT, OrderType.STP_MKT]:
            return (trd_side == Direction.SHORT and current_price <= trigger_price) or \
                (trd_side == Direction.LONG and current_price >= trigger_price)
        return None

    def cancel_order(self, req: CancelRequest):
        res_order = OrderData(symbol=req.symbol, exchange=req.exchange, orderid=req.orderid, type=OrderType.MARKET,
                              direction=Direction.NET, volume=0, price=0, gateway_name='FUTU',
                              status=OrderStatus.UNKNOWN, datetime=pd.Timestamp.now())
        if req.orderid not in self._order_map.keys():
            res_order.status = OrderStatus.REJECTED
        elif req.orderid in self._pending_orders.keys():
            with self._lock:
                res_order.reference = self._pending_orders[req.orderid].reference
                res_order.status = OrderStatus.ALLCANCELLED
                del self._pending_orders[req.orderid]
                # 更新tick订阅
                self._update_monitoring_tick_subscriptions(sub=False, symbol=req.symbol)

        else:
            pre_order = self.get_order_from_map(order_id=req.orderid)
            broker_orderid = pre_order.broker_orderid
            res_order.broker_orderid = broker_orderid

            ret_unlock, res_unlock = self.trd_ctx.unlock_trade(FutuInfo.pwd_unlock)
            if ret_unlock != RET_OK:
                res_order.status = OrderStatus.REJECTED

            # 发起撤单请求，qty 和 price 参数会被忽略（CANCEL 模式下无意义）
            ret_modify_order, res_modify_order = self.trd_ctx.modify_order(
                ModifyOrderOp.CANCEL,
                broker_orderid,
                0, 0,
                trd_env=self.trd_env
            )
            if ret_modify_order != RET_OK:
                res_order.status = OrderStatus.REJECTED
            else:
                # futu不会返回时CANCEL_ALL或者CANCEL_PART
                # 当向futu提交撤单且提交成功时，具体状态由on order返回
                return
        self.on_order(res_order)

    def modify_order(self, req: ModifyRequest) -> None:
        # 只支持修改价格和数量
        # 如果在本地查询不到order信息
        res_order = OrderData(symbol=req.symbol, exchange=req.exchange, orderid=req.orderid, type=OrderType.MARKET,
                              direction=Direction.NET, volume=req.qty, price=req.price, gateway_name='FUTU',
                              trigger_price=req.trigger_price, status=OrderStatus.UNKNOWN, datetime=pd.Timestamp.now())
        if req.orderid not in self._order_map.keys():
            res_order.status = OrderStatus.REJECTED
        # 如果order还停留在本地
        elif req.orderid in self._pending_orders.keys():
            with self._lock:
                res_req = self._pending_orders[req.orderid]
                res_req.price = req.price
                res_req.qty = req.qty
                res_req.trigger_price = req.trigger_price
                order_id = req.orderid
                res_order = OrderData(symbol=res_req.symbol, exchange=res_req.exchange, orderid=order_id,
                                      type=res_req.type, direction=res_req.direction, volume=res_req.qty,
                                      price=res_req.price, gateway_name='FUTU', broker_orderid=None,
                                      status=OrderStatus.MODIFIED, trigger_price=res_req.trigger_price,
                                      reference=res_req.reference, datetime=pd.Timestamp.now())

        # 如果已经发送到了futu
        else:
            pre_order = self.get_order_from_map(order_id=req.orderid)
            broker_orderid = pre_order.broker_orderid
            res_order.broker_orderid = broker_orderid

            ret_unlock, res_unlock = self.trd_ctx.unlock_trade(FutuInfo.pwd_unlock)
            if ret_unlock != RET_OK:
                res_order.status = OrderStatus.REJECTED

            ret_modify_order, res_modify_order = self.trd_ctx.modify_order(
                ModifyOrderOp.NORMAL,
                res_order.broker_orderid,
                req.qty, req.price,
                trd_env=self.trd_env, aux_price=req.trigger_price
            )
            if ret_modify_order != RET_OK:
                res_order.status = OrderStatus.REJECTED
            else:
                # 当向futu提交改单且提交成功时，具体状态由on order返回
                return
        self.on_order(res_order)

    def query_order(self, req):
        # 解锁账户
        ret_unlock, res_unlock = self.trd_ctx.unlock_trade(FutuInfo.pwd_unlock)
        if ret_unlock != RET_OK:
            raise FutuException(f'req:{req}{res_unlock}')

        # 查询当前订单
        ret1, df1 = self.trd_ctx.order_list_query(order_id=req.orderid, trd_env=self.trd_env)
        if ret1 == RET_OK and not df1.empty:
            return ret1, df1

        # 查询历史订单（注意频率限制）
        ret2, df2 = self.trd_ctx.history_order_list_query(trd_env=self.trd_env)
        if ret2 == RET_OK:
            match_df = df2[df2["order_id"] == req.orderid]
            if not match_df.empty:
                return ret2, match_df
        else:
            raise FutuException(f'req:{req}{df2}')

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        start = req.start
        end = req.end

        # 请求第一页 K线数据
        ret, data, page_req_key = self.quote_ctx.request_history_kline(
            code=req.symbol,
            ktype=req.interval,
            start=start,
            end=end,
            max_count=500
        )

        # 如果返回成功，开始处理翻页
        if ret != RET_OK:
            raise FutuException(f'req:{req}{data}')

        if start is not None and end is not None:
            while page_req_key is not None:
                ret, data_temp, page_req_key = self.quote_ctx.request_history_kline(
                    code=req.symbol,
                    ktype=req.interval,
                    start=start,
                    end=end,
                    page_req_key=page_req_key,
                    max_count=500
                )
                if ret == RET_OK:
                    data = pd.concat([data, data_temp], ignore_index=True)
                else:
                    raise FutuException(f'req:{req}{data}')

        bars: list[BarData] = []
        for _, row in data.iterrows():
            bar = BarData(
                symbol=req.symbol,
                exchange=req.exchange,
                datetime=pd.to_datetime(row["trade_time"]),
                interval=req.interval,
                open_price=float(row["open"]),
                high_price=float(row["high"]),
                low_price=float(row["low"]),
                close_price=float(row["close"]),
                volume=float(row["volume"]),
                gateway_name=self.gateway_name
            )
            bars.append(bar)

        return bars


    def query_account(self):

        pass

    def query_position(self, symbol='', main_contract=list | None):
        # 如果需要查询期货主力合约
        if main_contract is not None:
            ret, data = self.quote_ctx.get_future_info(main_contract)
            if ret != RET_OK:
                raise FutuException(f'req:{data}')
            else:
                symbol = data['origin_code'].to_list()

        ret, data = self.trd_ctx.position_list_query(code=symbol, trd_env=self.trd_env)
        if ret != RET_OK:
            raise FutuException(f'req:{data}')
        position_list = []
        for i in range(0, len(data.index)):
            position = data.iloc[i]
            symbol = position['code']
            exchange = Exchange.HKFE
            direction = Direction.LONG if position['direction'] == 'LONG' else Direction.SHORT
            volume = position['qty']
            price = position['price']
            frozen = position['can_sell_qty'] - volume
            position_data = PositionData(symbol=symbol, exchange=exchange, direction=direction, volume=volume,
                                         frozen=frozen, price=price, gateway_name='futu')
            position_list.append(position_data)

    @staticmethod
    def query_db(db_name, table_name, start_date=None, end_date=None, ):
        res = fetch_kline(db_name, table_name, start_date, end_date)
        return res

    @staticmethod
    def request_kline(code, ktype, start_date=None, end_date=None,
                      limit=10, host=FutuInfo.host, port=FutuInfo.port):
        """
        主动从 Futu OpenD 请求历史 K 线数据，支持翻页获取全量数据。

        参数:
        --------
        code : str
            证券代码，例如 'HK.MHImain'
        ktype : str
            K线周期，例如 '5m'、'15m'、'30m'、'1h'、'2h'、'3h'、'4h'
        start_date : str, optional
            起始时间，格式如 '2025-01-01'
        end_date : str, optional
            结束时间，格式如 '2025-07-01'，可为空表示查询到最新
        limit : int, optional
            当start_date和end_date为空，默认从当前日期往前获取limit条数据
        host : str
            Futu OpenD 主机地址，默认 '127.0.0.1'
        port : int
            Futu OpenD 端口，默认 11111

        返回:
        --------
        pd.DataFrame
            所有时间段的 K线数据，按时间升序排列

        示例:
        --------
        code = 'HK.MHImain'
        ktype = '5m'
        start_date = '2025-07-01'
        df = request_kline(code, ktype, start_date=start_date)
        print(df.head())
        """
        # 将用户传入的 ktype 映射为 Futu API 所需的内部表示
        futu_ktype_dict = {
            '5m': 'K_5M', '15m': 'K_15M', '30m': 'K_30M',
            '1h': 'K_1H', '2h': 'K_2H', '3h': 'K_3H', '4h': 'K_4H'
        }

        if ktype not in futu_ktype_dict:
            raise ValueError(f"不支持的 ktype: {ktype}")

        futu_ktype = futu_ktype_dict[ktype]
        quote_ctx = OpenQuoteContext(host=host, port=port)

        # 请求第一页 K线数据
        ret, data, page_req_key = quote_ctx.request_history_kline(
            code=code,
            ktype=futu_ktype,
            start=start_date,
            end=end_date,
            max_count=limit
        )

        # 如果返回成功，开始处理翻页
        if ret == RET_OK:
            if start_date is not None and end_date is not None:
                while page_req_key is not None:
                    ret, data_temp, page_req_key = quote_ctx.request_history_kline(
                        code=code,
                        ktype=futu_ktype,
                        start=start_date,
                        end=end_date,
                        page_req_key=page_req_key,
                        max_count=500
                    )
                    if ret == RET_OK:
                        data = pd.concat([data, data_temp], ignore_index=True)
                    else:

                        print('[错误] 请求下一页数据失败:', data_temp)
                        break
        else:
            print('[错误] 请求历史K线失败:', data)
            quote_ctx.close()
            return pd.DataFrame()

        quote_ctx.close()  # 关闭连接，避免占用连接池资源
        data = data.reset_index(drop=True)
        return data


if __name__ == "__main__":
    from engine.event_engine import EventEngine, Event


    # 定义一个回调，用于打印收到的 Bar
    def print_bar(event: Event):
        bar = event.data
        print(f"[测试] 收到 Bar: {bar.symbol} {bar.datetime} 开:{bar.open_price} 收:{bar.close_price}")


    def print_tick(event: Event):
        bar = event.data


    def on_trade(event: Event):
        trade = event.data
        print(f"[测试] 收到 trade: {trade.symbol} {trade.vt_symbol},{trade.price},order_id:{trade.orderid}")


    def on_order(event: Event):
        trade = event.data
        print(
            f"[测试] 收到 on order: {trade.symbol} {trade.vt_symbol},{trade.price} {trade.orderid} {trade.broker_orderid} {trade.status}")


    # 创建事件引擎
    event_engine = EventEngine()
    event_engine.register(EVENT_BAR, print_bar)
    event_engine.register(EVENT_TICK, print_tick)
    event_engine.register(EVENT_ORDER, on_order)
    event_engine.register(EVENT_TRADE, on_trade)
    event_engine.start()
    # 创建 Gateway
    gateway = FutuGateway(event_engine)

    # 启动 Gateway，并订阅行情
    gateway.connect({
        "symbols": ['HK.MHImain'],  # 订阅标的
        "intervals": [Interval.K_5M]  # 订阅周期
    })

    req = OrderRequest(symbol='HK.MHImain', direction=Direction.LONG,
                       type=OrderType.STP_LMT, price=20000, volume=1, trigger_price=200000,
                       exchange=Exchange.HKFE)
    gateway.send_order(req)

    req_modify = ModifyRequest(symbol='HK.MHImain', orderid='6ca7d4', exchange=Exchange.HKFE,qty=1,price=30000,trigger_price=25)
    gateway.modify_order(req_modify)


    req_cancel = CancelRequest(symbol='HK.MHImain', orderid='03e55b', exchange=Exchange.HKFE)
    gateway.cancel_order(req_cancel)
