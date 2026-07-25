from autotrade.coreutils.logger import get_logger
from autotrade.coreutils.constant import Exchange, Interval
from autotrade.backtest.event_engine import BacktestEventEngine
from autotrade.backtest.backtest_engine import BacktestEngine
from autotrade.backtest.data import (
    DataManager,
    DataRoutingConfig,
    FutureStateReader,
    TradeBarReader,
)
import pandas as pd

from example.macd import MACDStrategy

# ==============1.读取数据=================
df = pd.read_pickle("mhi_1h.pkl")

# ==============2.日志系统=================
logger = get_logger(name='backtest', logfile='macd.log')
# ==============3.回测事件引擎=================
# 回测里面事件引擎必须用BacktestEventEngine
event_engine = BacktestEventEngine()
# ==============4.导入策略=================
strategy = MACDStrategy(event_engine=event_engine, instrument_id="HK.MHImain", work_interval=Interval.K_1H)
strategy.initialize()
# ==============5.回测引擎=================
engine = BacktestEngine(event_engine=event_engine, logger=logger, initial_cash=50000)
data_manager = DataManager(
    DataRoutingConfig(
        strategy_data_names={"1h"},
        security_data_names={"instruments", "1h"},
        valuation_data_names={"1h"},
    )
)
data_manager.add_data(
    "instruments",
    FutureStateReader().read(
        pd.DataFrame([{
            "instrument_id": "HK.MHImain",
            "multiplier": 10,
            "margin_rate": 0.1,
            "commission_rate": 0.00006,
        }]),
        exchange=Exchange.HKFE,
    ),
)
data_manager.add_data(
    "1h",
    TradeBarReader().read(
        df,
        interval=Interval.K_1H,
        exchange=Exchange.HKFE,
    ),
)
# 启动回测引擎
engine.run(data_manager.stream())
engine.get_trade_log_df().to_csv(f'backtest_res/macd_tradelog.csv')
engine.get_account_daily_df().to_csv(f'backtest_res/macd_account.csv')



