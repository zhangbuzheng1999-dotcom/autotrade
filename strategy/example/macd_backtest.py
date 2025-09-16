from coreutils.logger import get_logger
from coreutils.constant import Interval, Exchange, Direction, OrderType, OrderStatus, LogLevel
from backtest.backtest_event_engine import Event, BacktestEventEngine
from backtest.backtest_engine import BacktestEngine
import pandas as pd
from strategy.example.macd import MACDStrategy

# ==============1.读取数据=================
df = pd.read_csv('c:/users/holy_/desktop/autotrade/strategy/example/1h_2023.csv')
# 数据列必须有'symbol', 'open', 'high', 'low', 'close', 'datetime', 'ktype'
df['ktype'] = Interval.K_1H
df['symbol'] = 'HK.MHImain'
df = df[['symbol', 'open', 'high', 'low', 'close', 'trade_date', 'ktype']]
df.columns = ['symbol', 'open', 'high', 'low', 'close', 'datetime', 'ktype']

# ==============2.日志系统=================
logger = get_logger(name='backtest', logfile='macd.log')
# ==============3.回测事件引擎=================
# 回测里面事件引擎必须用BacktestEventEngine
event_engine = BacktestEventEngine()
event_engine.start()
# ==============4.导入策略=================
strategy = MACDStrategy(event_engine=event_engine, symbol="HK.MHImain", work_interval=Interval.K_1H)
strategy.initialize()
# ==============5.回测引擎=================
engine = BacktestEngine(event_engine=event_engine, logger=logger, initial_cash=50000,
                        daily_update_interval=Interval.K_1H)
# 导入之前的数据
engine.load_data(data_list=[df])
engine.set_contracts(contract_params={
    "HK.MHImain": {"size": 10, "margin_rate": 0.1, "long_rate": 0.00006, "short_rate": 0.00006}
})
# 启动回测引擎
engine.run()
engine.performance_plot(plot_path='macd.html')
engine.get_trade_log_df().to_csv(f'macd_tradelog.csv')
engine.get_account_daily_df().to_csv(f'macd_account.csv')



