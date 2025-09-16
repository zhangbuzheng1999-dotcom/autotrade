# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Tuple, Optional
from collections import deque
import numpy as np
from talib import MACD
from coreutils.constant import Interval, Exchange, Direction, OrderType
from coreutils.object import BarData, TradeData, OrderRequest, PositionData, LogData
from strategy.strategy_base import StrategyBase, TargetOrder
from backtest.backtest_engine import BacktestEngine
from engine.event_engine import EventEngine, EVENT_TICK, EVENT_ORDER, EVENT_TRADE, EVENT_BAR, EVENT_LOG, \
    EVENT_ORDER_REQ, EVENT_CANCEL_REQ, EVENT_MODIFY_REQ

# 依赖：你已有的 StrategyBase / TargetOrder / 常量 等
# from your_module import StrategyBase, TargetOrder, ENTRY_REFS, EVENT_LOG, ...

class MACDStrategy(StrategyBase):
    """
    使用 talib.MACD 的最小示例：
    - 金叉：空仓 -> 买入开多
    - 死叉：持多 -> 卖出平多
    - 仅做多（无反手做空），下单用市价 MKT 简化
    - 信号只在选定 interval 的 bar 收到后触发
    """

    def __init__(
            self,
            event_engine,
            symbol: str,
            exchange: Exchange = Exchange.HKFE,
            work_interval: Interval = Interval.K_1M,
            volume: int = 1,
            fast: int = 12,
            slow: int = 26,
            signal: int = 9,
    ):
        super().__init__(event_engine)

        # 基本参数
        self.symbol = symbol
        self.exchange = exchange
        self.work_interval = work_interval
        self.volume = int(volume)
        self.time = datetime.now()
        # MACD 参数
        self.fast = fast
        self.slow = slow
        self.signal = signal

        # 数据与状态
        self._closes: deque = deque(maxlen=max(self.slow * 3, 200))
        self._last_close: Optional[float] = None
        self._pending_signal: Optional[str] = None  # 'buy' / 'sell' / None

        # 简化的持仓（只跟踪多头净仓）
        self.position = PositionData(
            symbol=self.symbol,
            exchange=self.exchange,
            volume=0,
            price=0.0,
            direction=Direction.NET,
            gateway_name="MACD",
        )

    # ===================== 事件回调 =====================

    def on_trade(self, trade: TradeData):
        self.write_log(LogData(msg=f"[MACD] {self.data_time} 收到TradeData:{trade}"))

        if trade.symbol != self.symbol:
            return

        if trade.direction == Direction.LONG:
            self.position.volume += trade.volume
            self.position.price = trade.price
        elif trade.direction == Direction.SHORT:
            self.position.volume -= trade.volume

    def on_bar(self, bar: BarData):
        self.data_time = bar.datetime
        # 仅在指定合约与周期上工作
        if bar.symbol != self.symbol or bar.interval != self.work_interval:
            return

        close = float(bar.close_price)
        self._last_close = close
        self._closes.append(close)

        # 使用 talib.MACD 计算
        if len(self._closes) < self.slow + self.signal + 1:
            return  # 数据不足

        arr = np.asarray(self._closes, dtype=float)
        macd, macd_signal, _ = MACD(
            arr,
            fastperiod=self.fast,
            slowperiod=self.slow,
            signalperiod=self.signal
        )

        # 需要至少两根有效值用于判断穿越
        if np.isnan(macd[-1]) or np.isnan(macd_signal[-1]) or np.isnan(macd[-2]) or np.isnan(macd_signal[-2]):
            return

        prev_diff = macd[-2] - macd_signal[-2]
        curr_diff = macd[-1] - macd_signal[-1]

        golden = (prev_diff <= 0) and (curr_diff > 0)  # 金叉
        dead = (prev_diff >= 0) and (curr_diff < 0)  # 死叉

        if golden and self.position.volume == 0:
            self._pending_signal = "buy"
            self.write_log(LogData(msg=f"[MACD] {self.data_time} 金叉 -> BUY @ {close}"))
            self._request_realign()

        if dead and self.position.volume > 0:
            self._pending_signal = "sell"
            self.write_log(LogData(msg=f"[MACD] {self.data_time} 死叉 -> SELL @ {close}"))
            self._request_realign()

    def on_tick(self, tick):
        # 本示例仅基于 bar 运行
        return

    # ===================== 计划生成/执行 =====================

    def _compute_desired_entry(self) -> Dict[str, "TargetOrder"]:
        targets: Dict[str, "TargetOrder"] = {}
        if self._pending_signal is None or self._last_close is None:
            return targets

        px = float(self._last_close)

        if self._pending_signal == "buy" and self.position.volume == 0:
            targets["entry"] = TargetOrder(
                symbol=self.symbol,
                reference=f"{self.data_time}entry",
                direction=Direction.LONG,
                price=px,
                trigger_price=px,
                volume=self.volume,
                type_=OrderType.MARKET,  # 简化：市价单
            )

        elif self._pending_signal == "sell" and self.position.volume > 0:
            # 平掉全部多头
            targets["close"] = TargetOrder(
                symbol=self.symbol,
                reference=f"{self.data_time}close",
                direction=Direction.SHORT,
                price=px,
                trigger_price=px,
                volume=self.position.volume,
                type_=OrderType.MARKET,
            )

        return targets

    def _build_plan(self) -> List[Tuple[str, OrderRequest]]:
        plan: List[Tuple[str, OrderRequest]] = []

        desired_entry = self._compute_desired_entry()
        for _, tgt in desired_entry.items():
            req = OrderRequest(
                symbol=self.symbol,
                exchange=self.exchange,
                direction=tgt.direction,
                type=tgt.type,
                price=tgt.price,
                trigger_price=tgt.trigger_price,
                volume=abs(tgt.volume),
                reference=tgt.reference,
            )
            plan.append(("place", req))

        # 消费掉本轮信号
        self._pending_signal = None
        return plan


