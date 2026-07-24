"""Smoke test: multi-asset, multi-period Bollinger strategy."""

from __future__ import annotations

import math
import sys
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path
from statistics import fmean, pstdev

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from autotrade.coreutils.constant import Direction, Exchange, Interval, OrderStatus, OrderType
from autotrade.coreutils.object import OrderData, OrderRequest, TradeData

from autotrade.backtest import BacktestEngine
from autotrade.backtest.data import (
    DataManager,
    DataRoutingConfig,
    EquityStateReader,
    TradeBarReader,
)
from autotrade.strategy.strategy_base import StrategyBase


SYMBOLS = ["AAA", "BBB", "CCC"]


class QuietLogger:
    def debug(self, msg): pass
    def info(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): pass


class MultiPeriodBollingerStrategy(StrategyBase):
    def __init__(
        self,
        event_engine,
        security_manager,
        symbols: list[str],
        *,
        window: int = 8,
        band_width: float = 1.15,
        max_position: int = 2,
    ) -> None:
        super().__init__(event_engine, security_manager)
        self.symbols = symbols
        self.window = window
        self.band_width = band_width
        self.max_position = max_position
        self.five_minute_closes = {symbol: deque(maxlen=window) for symbol in symbols}
        self.positions = defaultdict(int)
        self.pending = defaultdict(bool)
        self.orders_seen = 0
        self.trades_seen = 0

    def on_data(self, slice_) -> None:
        for symbol in self.symbols:
            fast_bar = slice_.get_bar(symbol, "1m")
            slow_bar = slice_.get_bar(symbol, "5m")
            if slow_bar is not None:
                self.five_minute_closes[symbol].append(slow_bar.close)
            if fast_bar is None:
                continue

            closes = self.five_minute_closes[symbol]
            if len(closes) < self.window or self.pending[symbol]:
                continue

            mean = fmean(closes)
            sigma = pstdev(closes) or 1e-9
            upper = mean + self.band_width * sigma
            lower = mean - self.band_width * sigma
            position = self.positions[symbol]

            if fast_bar.close < lower and position < self.max_position:
                self._send_market_order(symbol, fast_bar.exchange, Direction.LONG)
            elif fast_bar.close > upper and position > -self.max_position:
                self._send_market_order(symbol, fast_bar.exchange, Direction.SHORT)

    def on_order(self, order: OrderData):
        self.orders_seen += 1
        if order.status in {OrderStatus.ALLTRADED, OrderStatus.ALLCANCELLED, OrderStatus.REJECTED}:
            self.pending[order.symbol] = False

    def on_trade(self, trade: TradeData):
        self.trades_seen += 1
        signed_volume = int(trade.volume) if trade.direction == Direction.LONG else -int(trade.volume)
        self.positions[trade.symbol] += signed_volume
        self.pending[trade.symbol] = False

    def _send_market_order(self, symbol: str, exchange: Exchange | None, direction: Direction) -> None:
        self.pending[symbol] = True
        self.push_order_request(
            OrderRequest(
                symbol=symbol,
                exchange=exchange or Exchange.SSE,
                direction=direction,
                type=OrderType.MARKET,
                volume=1,
                reference="multi_period_bollinger",
            )
        )


def make_multi_asset_bars(
    symbols: list[str],
    *,
    start: datetime,
    periods: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    minute_rows = []
    five_minute_rows = []

    for symbol_index, symbol in enumerate(symbols):
        base = 100 + symbol_index * 15
        previous_close = base
        symbol_minute_rows = []

        for i in range(periods):
            when = start + timedelta(minutes=i + 1)
            wave = math.sin(i / 5 + symbol_index) * 2.8
            trend = math.sin(i / 23 + symbol_index * 0.7) * 1.6
            shock = 0.0
            if i % 37 in {0, 1, 2}:
                shock = 3.0 - symbol_index
            elif i % 53 in {0, 1, 2}:
                shock = -3.2 + symbol_index * 0.4

            close = base + wave + trend + shock
            open_ = previous_close
            high = max(open_, close) + 0.35
            low = min(open_, close) - 0.35
            row = {
                "symbol": symbol,
                "time": when,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": 1_000 + symbol_index * 100 + i,
            }
            minute_rows.append(row)
            symbol_minute_rows.append(row)
            previous_close = close

        for offset in range(4, len(symbol_minute_rows), 5):
            chunk = symbol_minute_rows[offset - 4 : offset + 1]
            five_minute_rows.append(
                {
                    "symbol": symbol,
                    "time": chunk[-1]["time"],
                    "open": chunk[0]["open"],
                    "high": max(item["high"] for item in chunk),
                    "low": min(item["low"] for item in chunk),
                    "close": chunk[-1]["close"],
                    "volume": sum(item["volume"] for item in chunk),
                }
            )

    sort_columns = ["time", "symbol"]
    return (
        pd.DataFrame(minute_rows).sort_values(sort_columns).reset_index(drop=True),
        pd.DataFrame(five_minute_rows).sort_values(sort_columns).reset_index(drop=True),
    )


def main() -> None:
    minute_bars, five_minute_bars = make_multi_asset_bars(
        SYMBOLS,
        start=datetime(2024, 1, 1, 9, 30),
        periods=180,
    )

    engine = BacktestEngine(logger=QuietLogger())
    data_manager = DataManager(
        DataRoutingConfig(
            strategy_data_names={"1m", "5m"},
            security_data_names={"instruments", "1m"},
            valuation_data_names={"1m"},
        )
    )
    data_manager.add_data(
        "instruments",
        EquityStateReader().read(
            pd.DataFrame(
                {
                    "symbol": SYMBOLS,
                    "multiplier": 1,
                    "margin_rate": 0.1,
                }
            ),
            exchange=Exchange.SSE,
        ),
    )
    data_manager.add_data(
        "1m",
        TradeBarReader().read(
            minute_bars,
            interval=Interval.K_1M,
            exchange=Exchange.SSE,
        ),
    )
    data_manager.add_data(
        "5m",
        TradeBarReader().read(
            five_minute_bars,
            interval=Interval.K_5M,
            exchange=Exchange.SSE,
        ),
    )

    strategy = MultiPeriodBollingerStrategy(
        engine.event_engine,
        engine.security_manager,
        SYMBOLS,
    )
    strategy.initialize()
    engine.run(data_manager.stream())

    print("processed_slices:", engine.processed_slice_count)
    print("symbols:", engine.symbols)
    print("orders_seen:", strategy.orders_seen)
    print("trades_seen:", strategy.trades_seen)
    print("oms_orders:", len(engine.oms.orders))
    print("oms_trades:", len(engine.oms.trade_log))
    print("account_snapshots:", len(engine.account_daily))
    print("final_equity:", list(engine.account_daily.values())[-1]["equity"])
    print("positions:", {symbol: pos.volume for symbol, pos in engine.oms.positions.items()})

    assert engine.processed_slice_count == len(minute_bars["time"].unique())
    assert engine.symbols == SYMBOLS
    assert strategy.orders_seen > 0
    assert strategy.trades_seen > 0
    assert len(engine.oms.trade_log) == strategy.trades_seen
    assert len(engine.account_daily) > 0


if __name__ == "__main__":
    main()
