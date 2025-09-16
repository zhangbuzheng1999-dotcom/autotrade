from coreutils.constant import Interval
from engine.trade_engine import CtaEngine
from coreutils.logger import get_logger
from engine.oms_engine import OmsBase
from engine.event_engine import EventEngine
from conn.engine_mes_adapter import EngineMesAdapter
from gateway.gateway_futu import FutuGateway as Gateway
from pathlib import Path
from strategy.example.macd import MACDStrategy


def get_base_dir():
    if "__file__" in globals():
        # 普通脚本运行: __file__ 存在
        return Path(__file__).resolve().parent.parent
    else:
        # 交互模式 (Jupyter/IPython): 用当前工作目录
        return Path().resolve()


BASE_DIR = get_base_dir()
engine_id = 'mhi'
# ==============1.日志系统=================
LOG_DIR = BASE_DIR / "logs" / f'{engine_id}.log' # 日志最好放在/autotrade/logs里面，方便app读取
logger = get_logger(name=engine_id, logfile=str(LOG_DIR.resolve()))
# ==============2.事件引擎=================
event_engine = EventEngine()
event_engine.start()
# ==============3.oms引擎=================
oms = OmsBase(event_engine)
# ==============4.gateway=================
gateway = Gateway(event_engine)
# ==============5.app通信模块(可选)=================
# 需要启动/autotrade/app/ws_hub.py
cta_adapter = EngineMesAdapter(engine_id, event_engine, oms)
cta_adapter.start()
# ==============6.cta 引擎=================
cta_engine = CtaEngine(oms=oms, event_engine=event_engine, gateway=gateway, logger=logger)
gateway.connect(
    setting={'symbols': ['HK.MHImain'], 'intervals': [Interval.TICK, Interval.K_1M, Interval.K_5M, Interval.K_15M]})
# ==============7.实例化引擎=================
strategy = MACDStrategy(event_engine=event_engine, symbol="HK.MHImain", work_interval=Interval.K_1H)
strategy.initialize()

