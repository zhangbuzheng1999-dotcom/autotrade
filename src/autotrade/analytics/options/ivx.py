from __future__ import annotations

import numpy as np
import pandas as pd


REQUIRED_COLUMNS = {
    "date",
    "option_price",
    "t_days",
    "strike_price",
    "option_type",
    "risk_free_rate",
}


def _maturity_variance(options: pd.DataFrame, forward: float) -> float:
    """Calculate model-free annualized variance for one maturity."""
    t = float(options["t_days"].iloc[0]) / 365.0
    if not np.isfinite(forward) or forward <= 0 or t <= 0:
        return np.nan

    strikes = np.sort(options["strike_price"].dropna().unique().astype(float))
    below = strikes[strikes <= forward]
    if len(strikes) < 2 or len(below) == 0:
        return np.nan
    k0 = float(below[-1])

    quotes = []
    for strike in strikes:
        strike_options = options[options["strike_price"] == strike]
        calls = pd.to_numeric(
            strike_options.loc[strike_options["option_type"] == "C", "option_price"],
            errors="coerce",
        ).dropna()
        puts = pd.to_numeric(
            strike_options.loc[strike_options["option_type"] == "P", "option_price"],
            errors="coerce",
        ).dropna()
        if strike < k0 and not puts.empty:
            quote = float(puts.median())
        elif strike > k0 and not calls.empty:
            quote = float(calls.median())
        elif strike == k0 and not calls.empty and not puts.empty:
            quote = float((calls.median() + puts.median()) / 2.0)
        else:
            continue
        if np.isfinite(quote) and quote >= 0:
            quotes.append((float(strike), quote))

    if len(quotes) < 2:
        return np.nan

    strike_array = np.asarray([item[0] for item in quotes], dtype=float)
    quote_array = np.asarray([item[1] for item in quotes], dtype=float)
    delta_k = np.empty(len(strike_array), dtype=float)
    delta_k[0] = strike_array[1] - strike_array[0]
    delta_k[-1] = strike_array[-1] - strike_array[-2]
    if len(strike_array) > 2:
        delta_k[1:-1] = (strike_array[2:] - strike_array[:-2]) / 2.0

    rate = float(options["risk_free_rate"].iloc[0])
    variance = (
        2.0
        / t
        * np.sum(delta_k / np.square(strike_array) * np.exp(rate * t) * quote_array)
        - (forward / k0 - 1.0) ** 2 / t
    )
    return float(variance) if np.isfinite(variance) and variance >= 0 else np.nan


def _forward_by_maturity(day: pd.DataFrame) -> pd.DataFrame:
    pairs = day.pivot_table(
        index=["t_days", "strike_price"],
        columns="option_type",
        values="option_price",
        aggfunc="median",
    ).reset_index()
    if not {"C", "P"}.issubset(pairs.columns):
        return pd.DataFrame(columns=["t_days", "forward_price"])

    rate = float(day["risk_free_rate"].iloc[0])
    pairs["forward_candidate"] = pairs["strike_price"] + np.exp(
        rate * pairs["t_days"] / 365.0
    ) * (pairs["C"] - pairs["P"])
    return (
        pairs.groupby("t_days", as_index=False)["forward_candidate"]
        .median()
        .rename(columns={"forward_candidate": "forward_price"})
    )


def _calculate_day(day: pd.DataFrame, target_days: int, min_days: int) -> dict:
    forward = _forward_by_maturity(day)
    candidates = []
    for row in forward.itertuples(index=False):
        t_days = int(row.t_days)
        if t_days <= min_days:
            continue
        maturity = day[day["t_days"] == t_days]
        strikes = pd.to_numeric(maturity["strike_price"], errors="coerce").dropna()
        if not ((strikes <= row.forward_price).any() and (strikes >= row.forward_price).any()):
            continue
        variance = _maturity_variance(maturity, float(row.forward_price))
        if np.isfinite(variance):
            candidates.append((t_days, variance))

    candidates.sort()
    result = {
        "ivx": np.nan,
        "near_t_days": None,
        "next_t_days": None,
        "near_variance": np.nan,
        "next_variance": np.nan,
    }
    if not candidates:
        return result

    near_days, near_variance = candidates[0]
    result.update(near_t_days=near_days, near_variance=near_variance)
    if near_days >= target_days:
        result["ivx"] = 100.0 * np.sqrt(near_variance)
        return result
    if len(candidates) < 2:
        return result

    next_days, next_variance = candidates[1]
    result.update(next_t_days=next_days, next_variance=next_variance)
    target = float(target_days)
    total_variance = (
        near_days * near_variance * (next_days - target)
        + next_days * next_variance * (target - near_days)
    ) / ((next_days - near_days) * target)
    if np.isfinite(total_variance) and total_variance >= 0:
        result["ivx"] = 100.0 * np.sqrt(total_variance)
    return result


def calculate_ivx(
    option_panel: pd.DataFrame,
    *,
    target_days: int = 30,
    min_days: int = 7,
) -> pd.DataFrame:
    """Calculate one model-free IVX value per trading date.

    IVX is quoted in volatility points, e.g. ``20.5`` means 20.5%.
    """
    missing = REQUIRED_COLUMNS - set(option_panel.columns)
    if missing:
        raise ValueError(f"Missing IVX input columns: {sorted(missing)}")
    if target_days <= 0 or min_days < 0:
        raise ValueError("target_days must be positive and min_days must be non-negative")

    panel = option_panel.copy()
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce")
    panel["option_type"] = panel["option_type"].astype(str).str.upper()
    for column in ("option_price", "t_days", "strike_price", "risk_free_rate"):
        panel[column] = pd.to_numeric(panel[column], errors="coerce")
    panel = panel.dropna(subset=list(REQUIRED_COLUMNS))

    rows = []
    for date, day in panel.groupby("date", sort=True):
        result = _calculate_day(day, target_days=target_days, min_days=min_days)
        result["date"] = date
        result["option_count"] = int(len(day))
        rows.append(result)
    return pd.DataFrame(rows)
