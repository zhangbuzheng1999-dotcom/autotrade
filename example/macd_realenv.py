from pathlib import Path

from dotenv import load_dotenv

from autotrade.conn.engine_mes_adapter import EngineMesAdapter
from autotrade.coreutils.constant import Interval
from autotrade.engine.live_engine import LiveEngine
from autotrade.gateway.gateway_futu import FutuGateway
from example.macd import MACDStrategy


load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env")

engine = LiveEngine(
    gateway_factory=FutuGateway,
    engine_id="mhi",
)

strategy = MACDStrategy(
    event_engine=engine.event_engine,
    instrument_id="HK.MHImain",
    work_interval=Interval.K_1H,
)
strategy.initialize()

app_adapter = engine.install(
    EngineMesAdapter("mhi", engine.event_engine, engine.oms)
)
app_adapter.start()

engine.start(
    {
        "symbols": ["HK.MHImain"],
        "intervals": [
            Interval.TICK,
            Interval.K_1M,
            Interval.K_5M,
            Interval.K_15M,
        ],
    }
)
