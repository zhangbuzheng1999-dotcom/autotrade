"""Pure performance calculations for recorded backtest equity."""

from __future__ import annotations

import math
import sys

import pandas as pd


class PerformanceAnalyzer:
    def __init__(
        self,
        *,
        initial_cash: float,
        risk_free: float = 0.02,
        annual_days: int = 240,
    ) -> None:
        self.initial_cash = float(initial_cash)
        self.risk_free = float(risk_free)
        self.annual_days = int(annual_days)

    def calculate(self, account_history: dict, *, print_result: bool = True) -> dict:
        df = pd.DataFrame.from_dict(account_history, orient="index").sort_index()
        if df.empty:
            return {}

        equity = df["equity"].astype(float)
        final_equity = float(equity.iloc[-1])
        total_return = (
            final_equity / self.initial_cash - 1
            if self.initial_cash > 0
            else math.nan
        )
        max_drawdown = self.max_drawdown(equity)

        timestamps = pd.to_datetime(equity.index)
        elapsed_seconds = (timestamps[-1] - timestamps[0]).total_seconds()
        annual_return = self.calculate_annual_return(
            self.initial_cash,
            final_equity,
            elapsed_seconds,
        )

        timed_equity = pd.Series(equity.to_numpy(), index=timestamps).sort_index()
        daily_equity = timed_equity.resample("1D").last().dropna()
        daily_returns = daily_equity.pct_change().dropna()
        if len(daily_returns) >= 2 and daily_returns.std() > 0:
            sharpe = (
                (daily_returns.mean() - self.risk_free / self.annual_days)
                / daily_returns.std()
                * math.sqrt(self.annual_days)
            )
        else:
            sharpe = math.nan

        if print_result:
            print("\n===== 回测绩效 =====")
            print(f"初始资金: {self.initial_cash:.2f}")
            print(f"结束资金: {final_equity:.2f}")
            print(f"总收益率: {total_return * 100:.2f}%")
            print(f"年化收益率: {annual_return * 100:.2f}%")
            print(f"最大回撤: {max_drawdown * 100:.2f}%")
            print(f"Sharpe Ratio: {sharpe:.2f}")

        return {
            "total_return": f"{total_return * 100:.2f}%",
            "annual_return": f"{annual_return * 100:.2f}%",
            "sharpe": sharpe,
            "max_drawdown": f"{max_drawdown * 100:.2f}%",
        }

    @staticmethod
    def calculate_annual_return(
        initial_equity: float,
        final_equity: float,
        elapsed_seconds: float,
    ) -> float:
        if (
            elapsed_seconds < 24 * 60 * 60
            or initial_equity <= 0
            or final_equity <= 0
            or not math.isfinite(initial_equity)
            or not math.isfinite(final_equity)
        ):
            return math.nan

        years = elapsed_seconds / (365.25 * 24 * 60 * 60)
        annual_log_return = (
            math.log(final_equity) - math.log(initial_equity)
        ) / years
        if annual_log_return > math.log(sys.float_info.max):
            return math.inf
        return math.expm1(annual_log_return)

    @staticmethod
    def max_drawdown(equity_series) -> float:
        peak = float(equity_series.iloc[0])
        maximum = 0.0
        for equity in equity_series:
            peak = max(peak, float(equity))
            drawdown = (peak - float(equity)) / peak
            maximum = max(maximum, drawdown)
        return maximum


__all__ = ["PerformanceAnalyzer"]
