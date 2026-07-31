from __future__ import annotations

import argparse
import os
from multiprocessing import Pool
from pathlib import Path
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
from tqdm import tqdm


DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data" / "greek_results"


def _normalize_flag(flag: Any) -> str | None:
    if pd.isna(flag):
        return None
    flag_str = str(flag).strip().lower()
    if not flag_str:
        return None
    first = flag_str[0]
    if first in {"c", "p"}:
        return first
    return None


def _to_float(value: Any) -> float:
    try:
        out = float(value)
    except Exception:
        return np.nan
    if not np.isfinite(out):
        return np.nan
    return out


def _compute_one_black_greek_row(
    opt_price: Any,
    forward_price: Any,
    strike_price: Any,
    t_days: Any,
    rate: Any,
    flag: Any,
    annual_days: int,
) -> dict[str, float]:
    out = {
        "iv": np.nan,
        "delta": np.nan,
        "gamma": np.nan,
        "vega": np.nan,
        "theta": np.nan,
        "rho": np.nan,
        "vanna": np.nan,
        "vomma": np.nan,
        "charm": np.nan,
    }

    price = _to_float(opt_price)
    fwd = _to_float(forward_price)
    strike = _to_float(strike_price)
    t_days_val = _to_float(t_days)
    r = _to_float(rate)
    flag_std = _normalize_flag(flag)

    if (
        pd.isna(price)
        or pd.isna(fwd)
        or pd.isna(strike)
        or pd.isna(t_days_val)
        or pd.isna(r)
        or flag_std is None
        or price <= 0
        or fwd <= 0
        or strike <= 0
        or t_days_val <= 0
    ):
        return out

    t = t_days_val / annual_days
    if t <= 0:
        return out

    try:
        iv = float(black_iv(price, fwd, strike, r, t, flag_std))
    except Exception:
        return out

    if not np.isfinite(iv) or iv <= 0:
        return out

    def _safe_bump(base: float, rel: float, floor: float) -> float:
        return max(abs(base) * rel, floor)

    try:
        out["iv"] = iv
        out["delta"] = float(black_delta(flag_std, fwd, strike, t, r, iv))
        out["gamma"] = float(black_gamma(flag_std, fwd, strike, t, r, iv))
        # analytics.py 当前按:
        # - vega * d_iv
        # - theta * (days / 365)
        # - rho * d_r
        # 做归因，因此这里输出统一转换为对小数 sigma / 年化 T / 小数 r 的导数口径。
        out["vega"] = float(black_vega(flag_std, fwd, strike, t, r, iv) / 0.01)
        out["theta"] = float(black_theta(flag_std, fwd, strike, t, r, iv) * annual_days)
        out["rho"] = float(black_rho(flag_std, fwd, strike, t, r, iv) / 0.01)

        # 二阶项口径也统一成 analytics.py 可直接使用的形式：
        # - vanna  : d(delta) / d(iv_decimal)
        # - vomma  : d(vega_decimal) / d(iv_decimal)
        # - charm  : 每 1 年日历时间流逝导致的 delta 变化
        vol_h = _safe_bump(iv, rel=1e-3, floor=1e-4)
        iv_lo = max(iv - vol_h, 1e-8)
        iv_hi = iv + vol_h

        delta_lo = float(black_delta(flag_std, fwd, strike, t, r, iv_lo))
        delta_hi = float(black_delta(flag_std, fwd, strike, t, r, iv_hi))
        out["vanna"] = (delta_hi - delta_lo) / (iv_hi - iv_lo)

        vega_lo = float(black_vega(flag_std, fwd, strike, t, r, iv_lo) / 0.01)
        vega_hi = float(black_vega(flag_std, fwd, strike, t, r, iv_hi) / 0.01)
        out["vomma"] = (vega_hi - vega_lo) / (iv_hi - iv_lo)

        dt_year = 1.0 / annual_days
        if t > dt_year:
            delta_after_one_day = float(black_delta(flag_std, fwd, strike, t - dt_year, r, iv))
            out["charm"] = (delta_after_one_day - out["delta"]) * annual_days
    except Exception:
        # If any Greek fails, keep whatever already computed and fill the rest with NaN.
        pass

    return out


def calculate_option_greeks_for_day(
    df: pd.DataFrame,
    *,
    order_book_id_col: str = "order_book_id",
    opt_price_col: str = "close",
    forward_price_col: str = "forward_price",
    strike_price_col: str = "strike_price",
    option_type_col: str = "option_type",
    t_days_col: str = "T_days",
    rate_col: str = "r",
    annual_days: int = 365,
) -> pd.DataFrame:
    """
    计算单个交易日、多只期权的 Black Greeks。

    返回列:
    ['order_book_id', 'iv', 'delta', 'gamma', 'vega', 'theta', 'rho', 'vanna', 'vomma', 'charm']
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df 必须是 pandas.DataFrame")

    required_cols = {
        order_book_id_col,
        opt_price_col,
        forward_price_col,
        strike_price_col,
        option_type_col,
        t_days_col,
        rate_col,
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"df 缺少必要列: {sorted(missing)}")

    res = df[[order_book_id_col]].copy()
    res = res.rename(columns={order_book_id_col: "order_book_id"})
    res["order_book_id"] = res["order_book_id"].astype(str)

    greek_rows = []
    for row in df[
        [
            opt_price_col,
            forward_price_col,
            strike_price_col,
            t_days_col,
            rate_col,
            option_type_col,
        ]
    ].itertuples(index=False, name=None):
        greek_rows.append(
            _compute_one_black_greek_row(
                opt_price=row[0],
                forward_price=row[1],
                strike_price=row[2],
                t_days=row[3],
                rate=row[4],
                flag=row[5],
                annual_days=annual_days,
            )
        )

    greek_df = pd.DataFrame(greek_rows, index=res.index)
    return pd.concat([res, greek_df], axis=1)


def _daily_worker(payload: dict[str, Any]) -> pd.DataFrame:
    date = payload["date"]
    df = payload["df"]
    kwargs = payload["kwargs"]

    daily_res = calculate_option_greeks_for_day(df, **kwargs)
    daily_res.insert(1, "date", pd.Timestamp(date))
    return daily_res


def calculate_option_greeks_for_dates(
    df: pd.DataFrame,
    *,
    date_col: str = "date",
    order_book_id_col: str = "order_book_id",
    opt_price_col: str = "close",
    forward_price_col: str = "forward_price",
    strike_price_col: str = "strike_price",
    option_type_col: str = "option_type",
    t_days_col: str = "T_days",
    rate_col: str = "r",
    annual_days: int = 365,
    n_jobs: int | None = None,
    chunksize: int = 1,
    show_progress: bool = True,
) -> pd.DataFrame:
    """
    计算多个交易日、多只期权的 Black Greeks。

    返回列:
    ['order_book_id', 'date', 'iv', 'delta', 'gamma', 'vega', 'theta', 'rho', 'vanna', 'vomma', 'charm']
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df 必须是 pandas.DataFrame")
    if date_col not in df.columns:
        raise ValueError(f"df 缺少必要列: {date_col}")

    work_df = df.copy()
    work_df[date_col] = pd.to_datetime(work_df[date_col], errors="coerce")
    if work_df[date_col].isna().any():
        raise ValueError(f"{date_col} 存在无法解析的日期")

    calc_kwargs = {
        "order_book_id_col": order_book_id_col,
        "opt_price_col": opt_price_col,
        "forward_price_col": forward_price_col,
        "strike_price_col": strike_price_col,
        "option_type_col": option_type_col,
        "t_days_col": t_days_col,
        "rate_col": rate_col,
        "annual_days": annual_days,
    }

    grouped_payloads = [
        {"date": date, "df": sub.copy(), "kwargs": calc_kwargs}
        for date, sub in work_df.groupby(date_col, sort=True)
    ]

    if not grouped_payloads:
        return pd.DataFrame(columns=["order_book_id", "date", "iv", "delta", "gamma", "vega", "theta", "rho", "vanna", "vomma", "charm"])

    total = len(grouped_payloads)
    if n_jobs is None:
        n_jobs = max(1, (os.cpu_count() or 1) - 1)
    n_jobs = max(1, int(n_jobs))

    results: list[pd.DataFrame] = []
    iterator = None

    if n_jobs == 1:
        iterator = map(_daily_worker, grouped_payloads)
        if show_progress:
            iterator = tqdm(iterator, total=total, desc="Calculating Greeks")
        results = list(iterator)
    else:
        with Pool(processes=n_jobs) as pool:
            iterator = pool.imap(_daily_worker, grouped_payloads, chunksize=chunksize)
            if show_progress:
                iterator = tqdm(iterator, total=total, desc="Calculating Greeks")
            results = list(iterator)

    out = pd.concat(results, axis=0, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"])
    out = out[["order_book_id", "date", "iv", "delta", "gamma", "vega", "theta", "rho", "vanna", "vomma", "charm"]]
    return out.sort_values(["date", "order_book_id"]).reset_index(drop=True)


def run_batch_from_directory(
    input_dir: str | os.PathLike[str],
    output_dir: str | os.PathLike[str] = DEFAULT_OUTPUT_DIR,
    *,
    pattern: str = "*.pkl",
    n_jobs: int | None = None,
    chunksize: int = 1,
    show_progress: bool = True,
    date_col: str = "date",
    order_book_id_col: str = "order_book_id",
    opt_price_col: str = "close",
    forward_price_col: str = "forward_price",
    strike_price_col: str = "strike_price",
    option_type_col: str = "option_type",
    t_days_col: str = "T_days",
    rate_col: str = "r",
    annual_days: int = 365,
) -> pd.DataFrame:
    """
    仅在脚本入口使用：
    读取目录下的期权面板文件，调用 DataFrame 接口计算，并将结果保存到输出目录。
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    file_paths = sorted(input_path.glob(pattern))
    if not file_paths:
        return pd.DataFrame(columns=["source_file", "rows_in", "rows_out", "output_path"])

    iterator = tqdm(file_paths, total=len(file_paths), desc="Batch Greek Files") if show_progress else file_paths
    rows: list[dict[str, Any]] = []

    for path in iterator:
        panel_df = pd.read_pickle(path)
        greek_df = calculate_option_greeks_for_dates(
            panel_df,
            date_col=date_col,
            order_book_id_col=order_book_id_col,
            opt_price_col=opt_price_col,
            forward_price_col=forward_price_col,
            strike_price_col=strike_price_col,
            option_type_col=option_type_col,
            t_days_col=t_days_col,
            rate_col=rate_col,
            annual_days=annual_days,
            n_jobs=n_jobs,
            chunksize=chunksize,
            show_progress=False,
        )
        out_file = output_path / path.name
        greek_df.to_pickle(out_file)
        rows.append(
            {
                "source_file": path.name,
                "rows_in": int(len(panel_df)),
                "rows_out": int(len(greek_df)),
                "output_path": str(out_file),
            }
        )

    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch-calculate Black Greeks from data/opt_greek_underlying.")
    parser.add_argument("--input-dir", default="data/opt_greek_underlying")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--pattern", default="*.pkl")
    parser.add_argument("--n-jobs", type=int, default=max(1, (os.cpu_count() or 1) - 1))
    parser.add_argument("--chunksize", type=int, default=1)
    args = parser.parse_args()

    summary = run_batch_from_directory(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        pattern=args.pattern,
        n_jobs=args.n_jobs,
        chunksize=args.chunksize,
        show_progress=True,
    )
    if summary.empty:
        print("No input files found.")
        return
    print("\nRESULTS")
    print(summary.to_string(index=False))


__all__ = [
    "calculate_option_greeks_for_day",
    "calculate_option_greeks_for_dates",
    "run_batch_from_directory",
]


if __name__ == "__main__":
    main()
