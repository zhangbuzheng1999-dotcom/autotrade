from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from py_vollib.black.greeks.analytical import delta as black_delta
from py_vollib.black.greeks.analytical import gamma as black_gamma
from py_vollib.black.greeks.analytical import rho as black_rho
from py_vollib.black.greeks.analytical import theta as black_theta
from py_vollib.black.greeks.analytical import vega as black_vega
from py_vollib.black.implied_volatility import (
    implied_volatility_of_discounted_option_price as black_iv,
)


GREEK_COLUMNS = [
    "iv", "delta", "gamma", "vega", "theta", "rho", "vanna", "vomma", "charm",
]


def _finite_float(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return np.nan
    return result if np.isfinite(result) else np.nan


def _flag(value: Any) -> str | None:
    if pd.isna(value):
        return None
    value = str(value).strip().lower()
    return value[0] if value and value[0] in {"c", "p"} else None


def _calculate_row(
    price: Any,
    forward: Any,
    strike: Any,
    t_days: Any,
    rate: Any,
    flag: Any,
    annual_days: int,
) -> dict[str, float]:
    result = {column: np.nan for column in GREEK_COLUMNS}
    price, forward, strike, t_days, rate = map(
        _finite_float, (price, forward, strike, t_days, rate)
    )
    flag = _flag(flag)
    if (
        flag is None
        or any(pd.isna(v) for v in (price, forward, strike, t_days, rate))
        or min(price, forward, strike, t_days) <= 0
    ):
        return result

    maturity = t_days / annual_days
    try:
        iv = float(black_iv(price, forward, strike, rate, maturity, flag))
    except Exception:
        return result
    if not np.isfinite(iv) or iv <= 0:
        return result

    try:
        result["iv"] = iv
        result["delta"] = float(black_delta(flag, forward, strike, maturity, rate, iv))
        result["gamma"] = float(black_gamma(flag, forward, strike, maturity, rate, iv))
        # Decimal-volatility, annual-time and decimal-rate derivative conventions.
        result["vega"] = float(black_vega(flag, forward, strike, maturity, rate, iv) / 0.01)
        result["theta"] = float(black_theta(flag, forward, strike, maturity, rate, iv) * annual_days)
        result["rho"] = float(black_rho(flag, forward, strike, maturity, rate, iv) / 0.01)

        bump = max(abs(iv) * 1e-3, 1e-4)
        iv_low, iv_high = max(iv - bump, 1e-8), iv + bump
        delta_low = float(black_delta(flag, forward, strike, maturity, rate, iv_low))
        delta_high = float(black_delta(flag, forward, strike, maturity, rate, iv_high))
        result["vanna"] = (delta_high - delta_low) / (iv_high - iv_low)
        vega_low = float(black_vega(flag, forward, strike, maturity, rate, iv_low) / 0.01)
        vega_high = float(black_vega(flag, forward, strike, maturity, rate, iv_high) / 0.01)
        result["vomma"] = (vega_high - vega_low) / (iv_high - iv_low)

        one_day = 1.0 / annual_days
        if maturity > one_day:
            delta_tomorrow = float(
                black_delta(flag, forward, strike, maturity - one_day, rate, iv)
            )
            result["charm"] = (delta_tomorrow - result["delta"]) * annual_days
    except Exception:
        # Preserve successfully calculated lower-order values.
        pass
    return result


def calculate_black97_greeks(
    frame: pd.DataFrame,
    *,
    annual_days: int = 365,
) -> pd.DataFrame:
    """Calculate Black97 IV/Greeks from a minimal, standardized input frame.

    Required columns: order_book_id, date, option_price, forward_price,
    strike_price, t_days, risk_free_rate, option_type.
    """
    required = {
        "order_book_id", "date", "option_price", "forward_price",
        "strike_price", "t_days", "risk_free_rate", "option_type",
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Greek input missing required columns: {sorted(missing)}")

    rows = [
        _calculate_row(*values, annual_days=annual_days)
        for values in frame[
            [
                "option_price", "forward_price", "strike_price", "t_days",
                "risk_free_rate", "option_type",
            ]
        ].itertuples(index=False, name=None)
    ]
    result = frame.copy()
    greek_frame = pd.DataFrame(rows, index=result.index)
    for column in GREEK_COLUMNS:
        result[column] = greek_frame[column]
    return result
