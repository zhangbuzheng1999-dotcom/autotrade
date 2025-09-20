from coreutils.constant import Interval
from engine.trade_engine import CtaEngine
from coreutils.logger import LoggerEngine
from engine.oms_engine import OmsBase
from engine.event_engine import EventEngine, Event
from conn.engine_mes_adapter import EngineMesAdapter
from gateway.gateway_futu import FutuGateway as Gateway
from strategy.example.macd import MACDStrategy
engine_id = 'mhi'

# ==============1.事件引擎=================
event_engine = EventEngine()
event_engine.start()


# ==============2.日志系统=================
class WechatLog(LoggerEngine):
    def __init__(self, event_engine, engine_id, LOG_DIR=None):
        super().__init__(event_engine, engine_id, LOG_DIR)

    def process_error(self, msg: str):
        self.logger.wechat(content=msg, level='error')


logger_engine = WechatLog(event_engine, engine_id)
# ==============3.oms引擎=================
oms = OmsBase(event_engine)
# ==============4.gateway=================
gateway = Gateway(event_engine)
# ==============5.app通信模块(可选)=================
# 需要启动/autotrade/app/ws_hub.py
cta_adapter = EngineMesAdapter(engine_id, event_engine, oms)
cta_adapter.start()
# ==============6.cta 引擎=================
cta_engine = CtaEngine(oms=oms, event_engine=event_engine, gateway=gateway)
gateway.connect(
    setting={'symbols': ['HK.MHImain'], 'intervals': [Interval.TICK, Interval.K_1M, Interval.K_5M, Interval.K_15M]})
# ==============7.实例化引擎=================
strategy = MACDStrategy(event_engine=event_engine, symbol="HK.MHImain", work_interval=Interval.K_1H)
strategy.initialize()
