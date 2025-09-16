import warnings
from copy import deepcopy
from pyecharts.globals import ThemeType
from pyecharts.charts import Kline, Line, Bar, Grid, Tab, Page
from pyecharts import options as opts
from pyexpat.errors import messages

from coreutils.object import (ModifyRequest, CancelRequest,
                              OrderRequest,
                              OrderData,
                              BarData,
                              TradeData,
                              Exchange, PositionData, LogData)
import numpy as np
from collections import defaultdict
import pandas as pd
from coreutils.constant import Interval, Direction, OrderType, OrderStatus, LogLevel
from backtest.backtest_gateway import BacktestGateway
from backtest.backtest_oms_engine import BacktestOms
from plotting.kline_dashboard import ChartManager, create_bar, create_line, create_kline, create_table
from engine.event_engine import (Event, EVENT_ORDER, EVENT_TRADE, EVENT_POSITION,
                                 EVENT_ORDER_REQ, EVENT_CANCEL_REQ, EVENT_MODIFY_REQ, EVENT_LOG, EVENT_BAR)
from coreutils.logger import get_logger
from backtest.backtest_event_engine import BacktestEventEngine


class BacktestEngine:
    """
    专业级回测引擎
    - 支持多标的
    - 支持保证金、逐日盯市
    - 支持策略 on_bars / on_order / on_trade
    - 提供绩效分析、Plotly图表
    """

    def __init__(self, event_engine: BacktestEventEngine, daily_update_interval: Interval | None = None,
                 matched_interval: Interval | None = None,
                 initial_cash: float = 1_000_000,
                 risk_free: float = 0.02,
                 annual_days: int = 240, engine_id='backtest',logger=None
                 ):
        """

        :param initial_cash:
        :param risk_free:
        :param annual_days:
        :param daily_update_interval: 比如要用15m或者2h更新equity或者holding profit，
                                                    就需要daily_update_interval =  Interval.K_15M
        :param matched_interval: 撮合交易用的频率
        """
        self.gateway_name = "backtest"

        # 交易所与gateway
        self.gateway = BacktestGateway(gateway_name=self.gateway_name, backtest_engine=self)

        # 撮合与资金管理
        self.oms = BacktestOms(initial_cash=initial_cash)

        # 初始设置
        self.initial_cash = initial_cash
        self.risk_free = risk_free
        self.annual_days = annual_days

        # 历史行情 & 标的
        self.history = list()
        self.symbols: list[str] = []
        self.update_datetime = None
        self.current_datetime = None
        self._kline_data: pd.DataFrame = pd.DataFrame()  # 用于回测画k线图
        # 策略实例
        self.strategy = None

        # 回测记录
        self.account_daily: dict = {}
        self.portfolio_daily: dict = {}
        self.contract_daily: dict = {}
        self.position_daily: dict = {}
        self.trades = []

        # 合约参数
        self.contract_params: dict[str, dict] = {}

        # daily_update
        self._daily_update_data = []  # 用于更新equity等的数据
        self.daily_update_interval = daily_update_interval
        self.matched_interval = matched_interval

        # 回测统计
        self.backtest_res: dict = {}

        # 日志系统
        if logger is None:
            self.logger = get_logger(name=engine_id, logfile=f'/logs/{engine_id}.log')
        else:
            self.logger = logger
        # 事件引擎
        self.event_engine = event_engine
        self.register_event()

    # =========================
    # 参数与初始化
    # =========================
    def set_contracts(self, contract_params: dict[str, dict]):
        """
        设置合约参数：
        contract_params = {
            "RB99.SHFE": {"size":10, "margin_rate":0.1, "long_rate":0.0002, "short_rate":0.0002}
        }
        """
        self.contract_params = contract_params
        for symbol, params in contract_params.items():
            self.oms.set_contract_params(
                symbol,
                size=params.get("size", 1),
                long_rate=params.get("long_rate", 0),
                short_rate=params.get("short_rate", 0),
                margin_rate=params.get("margin_rate", 0.1)
            )

    def load_data(self, data_list: list):
        """
        加载历史数据 data_dict: {symbol: DataFrame(index=datetime, columns=['open','high','low','close','symbol',
        'trade_date','ktype'])}
        """
        if not isinstance(data_list, list):
            raise TypeError('data_list must be a list')
        print(f"===========数据开始加载============")
        required_cols = {'symbol', 'open', 'high', 'low', 'close', 'datetime', 'ktype'}
        parts = []
        for data in data_list:
            if not required_cols.issubset(data.columns):
                raise ValueError(f"loaded data must have columns {required_cols}")
            if data.empty:
                continue
            if not isinstance(data['ktype'].iloc[0], Interval):
                raise TypeError("ktype data must be Interval")

            d = data.copy()
            d.loc[:, 'datetime'] = pd.to_datetime(d['datetime'], errors='coerce')
            d.loc[:, 'end_date'] = d['datetime'].shift(-1) - pd.Timedelta(seconds=1)

            # 只丢掉时间相关的 NaT（通常是最后一行）
            parts.append(d.dropna(subset=['datetime', 'end_date']))

        df = pd.concat(parts, ignore_index=True, copy=False)
        del parts
        df = df.sort_values(by=['end_date', 'ktype'], ascending=True)

        # 寻找最小的ktype比如同样有1m,15m选择1m作为交易所的撮合数据
        interval_list = pd.unique(df["ktype"])
        self.matched_interval = min(interval_list) if self.matched_interval is None else self.matched_interval
        self.daily_update_interval = max(
            interval_list) if self.daily_update_interval is None else self.daily_update_interval

        # 准备回测画图数据
        self.symbols = pd.unique(df['symbol']).tolist()
        self._kline_data = df[df['ktype'] == self.daily_update_interval]
        for dt in df.itertuples(index=False):
            # row: (datetime, open, high, low, close, volume, symbol)
            bar = BarData(
                symbol=dt.symbol,
                exchange=Exchange.HKFE,
                datetime=dt.datetime,
                interval=dt.ktype,
                open_price=dt.open,
                high_price=dt.high,
                low_price=dt.low,
                close_price=dt.close,
                gateway_name=self.gateway_name,
            )
            self.history.append(bar)

        # 如果多周期发布警告
        ktypes = pd.unique(df['ktype']).tolist()
        if len(ktypes) > 1:
            warning_mesg = (f"data里面包含多个interval类型;data_list中的data必须符合以下规则："
                            f"\n 1. data中datetime列表示数据开始的时间，如15m的数据'2025-01-01 09:15:00'"
                            f"代表09:15:00到09:30:00的k线"
                            f"\n 2. 默认数据是连续的,中间没有缺失")
            warnings.warn(warning_mesg)

    # =========================
    # 核心回测逻辑
    # =========================
    def run(self):
        print(f"回测开始，共 {len(self.history)} 根K线")

        pre_update_daily_time = None
        updated_data = {}

        for bar in self.history:
            if bar.interval == self.matched_interval:
                self.current_datetime = bar.datetime
            self.on_bar(bar)
            # self.history 已按时间升序
            # 只用盯市基准周期来聚合（例如 15m）
            if bar.interval == self.daily_update_interval:
                # 窗口切换：bar.datetime 与上一个 15m 结束时间不同 → 先 flush 上一窗口
                if pre_update_daily_time is not None and bar.datetime != pre_update_daily_time:
                    if updated_data:
                        self.update_datetime = bar.datetime
                        self._update_daily(updated_data)
                        updated_data = {}

                # 累积当前窗口的数据
                updated_data[bar.symbol] = bar.close_price
                pre_update_daily_time = bar.datetime

        # 循环结束，flush 最后一个窗口
        if updated_data:
            self.update_datetime = pre_update_daily_time
            self._update_daily(updated_data)

        self.backtest_res = self.calculate_statistics()
        print("回测结束")

    def register_event(self):
        """注册事件监听"""
        self.event_engine.register(EVENT_ORDER_REQ, self._on_order_req)
        self.event_engine.register(EVENT_MODIFY_REQ, self._on_modify_req)
        self.event_engine.register(EVENT_CANCEL_REQ, self._on_cancel_req)
        self.event_engine.register(EVENT_LOG, self._on_log)

    def _on_order_req(self, event: Event):
        req: OrderRequest = event.data
        log_data = LogData(msg=f"[BacktestEngine] 收到发送订单请求{req}")
        self._on_log(Event(EVENT_LOG,log_data))
        self.gateway.send_order(req)

    def _on_cancel_req(self, event: Event):
        req: CancelRequest = event.data
        log_data = LogData(msg=f"[BacktestEngine] 收到取消订单请求{req}")
        self._on_log(Event(EVENT_LOG,log_data))
        self.gateway.cancel_order(req)

    def _on_modify_req(self, event: Event):
        req: ModifyRequest = event.data
        log_data = LogData(msg=f"[BacktestEngine] 收到修改订单请求{req}")
        self._on_log(Event(EVENT_LOG,log_data))
        self.gateway.modify_order(req)

    def _on_log(self, event: Event):
        log_data: LogData = event.data
        if log_data.level == LogLevel.DEBUG:
            self.logger.debug(f"BackTestTime:{self.current_datetime} {log_data.msg}")
        elif log_data.level == LogLevel.INFO:
            self.logger.info(f"BackTestTime:{self.current_datetime} {log_data.msg}")
        elif log_data.level == LogLevel.WARNING:
            self.logger.warning(f"BackTestTime:{self.current_datetime} {log_data.msg}")
        elif log_data.level == LogLevel.ERROR:
            self.logger.error(f"BackTestTime:{self.current_datetime} {log_data.msg}")

    def _push_bar_event(self, bar: BarData):
        self.event_engine.put(Event(EVENT_BAR, bar))

    def _push_order_event(self, order: OrderData):
        self.event_engine.put(Event(EVENT_ORDER, order))

    def _push_trade_event(self, trade: TradeData):
        self.event_engine.put(Event(EVENT_TRADE, trade))

    def _push_position_event(self, position: PositionData):
        self.event_engine.put(Event(EVENT_POSITION, position))

    def on_bar(self, bar: BarData):
        self._push_bar_event(bar)
        # 主要是解决在不同频率bar顺序传入时(例如1m和15m),gateway错误采用15m数据判断开平仓
        if bar.interval == self.matched_interval:
            self.gateway.on_bar(bar)

    def on_trade(self, trade: TradeData):
        self.oms.process_trade_event(Event(EVENT_TRADE, trade))
        self._push_trade_event(trade)

    def on_position(self, position: PositionData):
        self._push_position_event(position)

    def on_order(self, order: OrderData):
        self.oms.process_order_event(Event(EVENT_ORDER, order))
        self._push_order_event(order)

    # =========================
    # 逐日盯市 & 权益记录
    # =========================
    def _update_daily(self, updated_data):
        self.oms.renew_unrealized_pnl(updated_data)
        account = deepcopy(self.oms.get_account("BACKTEST"))
        self.account_daily[self.update_datetime] = {'cash': account.cash,
                                                     'margin': account.margin,
                                                     'realized_pnl': account.realized_pnl,
                                                     'unrealized_pnl': account.unrealized_pnl,
                                                     'equity': account.equity,
                                                     'available': account.available}
        self.contract_daily[self.update_datetime] = deepcopy(self.oms.get_contract_log())
        self.position_daily[self.update_datetime] = deepcopy(self.oms.get_all_positions())

    # =========================
    # 绩效分析
    # =========================
    def calculate_statistics(self):
        df = pd.DataFrame.from_dict(self.account_daily, orient="index")
        total_return = (df["equity"].iloc[-1] / self.initial_cash) - 1
        daily_return = df["equity"].pct_change().dropna()
        sharpe = (daily_return.mean() - self.risk_free / self.annual_days) / \
                 (daily_return.std() + 1e-9) * np.sqrt(self.annual_days)
        max_drawdown = self._calc_max_drawdown(df["equity"])
        annual_return = ((1 + daily_return.mean()) ** self.annual_days) - 1

        print("\n===== 回测绩效 =====")
        print(f"初始资金: {self.initial_cash:.2f}")
        print(f"结束资金: {df['equity'].iloc[-1]:.2f}")
        print(f"总收益率: {total_return * 100:.2f}%")
        print(f"年化收益率: {annual_return * 100:.2f}%")
        print(f"最大回撤: {max_drawdown * 100:.2f}%")
        print(f"Sharpe Ratio: {sharpe:.2f}")
        return {"total_return": f"{total_return * 100:.2f}%", "annual_return": f"{annual_return * 100:.2f}%",
                "sharpe": sharpe, "max_drawdown": f"{max_drawdown * 100:.2f}%"}

    def _calc_max_drawdown(self, equity_series):
        peak = equity_series.iloc[0]
        max_dd = 0
        for x in equity_series:
            peak = max(peak, x)
            dd = (peak - x) / peak
            max_dd = max(max_dd, dd)
        return max_dd

    def get_trade_log_df(self):
        # 转化trade_log成数据框
        trade_log = pd.DataFrame([
            {
                "datetime": trade.datetime,
                "symbol": trade.symbol,
                "orderid": trade.orderid,
                "direction": trade.direction,
                "price": trade.price,
                "traded": trade.traded,
                "volume": trade.volume,
                "avgFillPrice": trade.avgFillPrice,
                "status": trade.status,
            }
            for trade in self.oms.trade_log
        ])
        return trade_log

    def get_account_daily_df(self):
        account_data = pd.DataFrame.from_dict(self.account_daily, orient="index")
        return account_data

    def _prepare_plot_data(self):
        """
        主要用于把不同资产的数据转换成数据框，供backttest_plot使用
        """
        # 准备统计表的数据
        table_data = {}
        trade_log = self.get_trade_log_df()
        table_data["trade_log"] = trade_log
        table_data["statistic"] = pd.DataFrame([self.backtest_res])

        plot_data = {}
        # 将{'2025-01-01': {'mhi': {'vol': 1},'hsi': {'vol': 1}}}转成{'mhi':{'2025-01-01':{'vol': 1}}....}形式
        contract_log = {}
        for date, symbols in self.contract_daily.items():
            for symbol, info in symbols.items():
                contract_log.setdefault(symbol, {})[date] = info

        # 准备数据
        for symbol, info in contract_log.items():
            spec_contract_detail = pd.DataFrame.from_dict(info, orient="index")
            spec_contract_detail.index = pd.to_datetime(spec_contract_detail.index)

            spec_trade_log = trade_log[trade_log["symbol"] == symbol].copy()
            spec_trade_log['datetime_fit'] = pd.cut(spec_trade_log['datetime'], bins=spec_contract_detail.index,
                                                    right=False, labels=spec_contract_detail.index[0:-1],
                                                    ordered=False)

            buy_log = spec_trade_log[spec_trade_log["direction"] == Direction.LONG] \
                .groupby('datetime_fit', observed=False)['volume'].sum()

            sell_log = spec_trade_log[spec_trade_log["direction"] == Direction.SHORT] \
                .groupby('datetime_fit', observed=False)['volume'].sum()

            spec_contract_detail['buy_log'] = buy_log
            spec_contract_detail['sell_log'] = sell_log
            plot_data[symbol] = spec_contract_detail

        return (plot_data, table_data)

    def performance_plot(self, plot_symbol_detail=True, plot_path="backtest_plot.html"):
        # 1 组合权益变动页
        account_data = self.get_account_daily_df()
        x_data = account_data.index.to_list()
        # 1.1权益图
        equity_plot = create_line(x_data=x_data, y_data=account_data['equity'], y_label='equity')
        cash_plot = create_line(x_data=x_data, y_data=account_data['cash'], y_label='cash')

        # 1.2 盈亏图
        realized_pnl_plot = create_line(x_data=x_data, y_data=account_data['realized_pnl'], y_label='realized_pnl')
        unrealized_pnl_plot = create_line(x_data=x_data, y_data=account_data['unrealized_pnl'],
                                          y_label='unrealized_pnl')
        realized_pnl_plot = realized_pnl_plot.overlap(unrealized_pnl_plot)

        # 1.3 资金占用
        margin_plot = create_line(x_data=x_data, y_data=account_data['margin'], y_label='margin')
        available_plot = create_line(x_data=x_data, y_data=account_data['available'], y_label='available')
        margin_plot = margin_plot.overlap(available_plot)

        # 1.4 权益页
        equity_page = ChartManager(theme=ThemeType.DARK)
        equity_page.set_main(equity_plot, height=30)
        equity_page.add_to_main(cash_plot)
        equity_page.add_sub_chart(realized_pnl_plot)
        equity_page.add_sub_chart(margin_plot)

        backtest_plot = Tab()
        backtest_plot.add(equity_page.output(), 'Portfolio Performance Dashboard')

        # 2. 回测统计和交易记录
        plot_data, table_data = self._prepare_plot_data()
        statistic_data = table_data['statistic']
        trade_log = table_data['trade_log']
        statistic_table = create_table(statistic_data, title='statistic')
        trade_log_table = create_table(trade_log, title='trade_log')

        backtest_plot.add(statistic_table, 'Backtest Statistic')
        backtest_plot.add(trade_log_table, 'Backtest Trade Log')

        # 3. 分资产统计
        if plot_symbol_detail:

            for symbol, data in plot_data.items():
                # 准备k线数据
                kline_data = self._kline_data[self._kline_data['symbol'
                                              ] == symbol][['datetime', 'open', 'high', 'low', 'close']].copy()
                kline_data.columns = ['date', 'open', 'high', 'low', 'close']
                kline_data['date'] = pd.to_datetime(kline_data['date'])
                # 持仓和交易
                trade_data = plot_data[symbol]
                trade_data['date'] = pd.to_datetime(trade_data.index)

                # 数据聚合
                plot_symbol_df = pd.merge(trade_data, kline_data, on='date', how='outer')
                kline_plot = create_kline(plot_symbol_df[['date', 'open', 'high', 'low', 'close']])
                x_data = plot_symbol_df['date'].to_list()

                # 这里用stack进行对齐，例如同一15mBUY和SELL都有数据，bar图会在同一时点位置上上下显示
                trade_plot = (Bar()
                              .add_xaxis(x_data)
                              .add_yaxis("BUY", plot_symbol_df['buy_log'].to_list(), stack="SAME")
                              .add_yaxis("SELL", (-plot_symbol_df['sell_log']).to_list(), stack="SAME")
                              .set_global_opts(
                    tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="shadow"),
                )
                              .set_series_opts(label_opts=opts.LabelOpts(is_show=False, position="inside"))
                              )
                position = create_line(x_data=x_data, y_data=plot_symbol_df['volume'], y_label='POSITION')
                trade_plot = trade_plot.overlap(position)

                symbol_detail_plot = ChartManager(theme=ThemeType.DARK, vgap_pct=7)
                symbol_detail_plot.set_main(kline_plot)
                symbol_detail_plot.add_sub_chart(trade_plot)
                backtest_plot.add(symbol_detail_plot.output(), tab_name=symbol)

        backtest_plot.render(plot_path)
        print(f"\n====================")
        print(f"回测图输出到{plot_path}")


