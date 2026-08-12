from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm.auto import tqdm

"""
forward_curve.py
================

功能概述
--------
本脚本用于从期权横截面数据中构建一条按到期天数 T_days 定义的 forward curve，
供后续的隐含波动率计算、Greeks 计算、波动率曲面构建、PnL 归因等任务使用。

核心目标不是“最理论化地解释 carry”，而是：
1. 尽可能稳健地提取每个期限的 forward；
2. 在部分期限数据缺失、C/P 不完整、个别期限异常时仍能继续运行；
3. 对后续定价模块提供统一、可查询的 F(T) 输入；
4. 在失败时尽量返回 np.nan 或空曲线，而不是中断整个流程。

----------------------------------------------------------------------
一、为什么要单独构建 forward curve
----------------------------------------------------------------------

在很多期权任务中，真正进入定价器的核心状态变量并不是 spot S，而是 forward / futures price F：

- 对期货期权，Black-76 直接以 F 为输入；
- 对指数期权，即使从 spot + carry 出发，最终也常常会转写成 forward 形式；
- 在构建基于 forward moneyness 的 IV 曲面时，需要稳定的 F(T)；
- 在做 Greeks / PnL 归因时，若 F(T) 不稳，会直接导致 moneyness 抖动、IV 抖动、风险暴露抖动。

因此，本脚本把“提取 forward curve”独立出来，作为后续所有模块的基础输入层。

----------------------------------------------------------------------
二、基本思路
----------------------------------------------------------------------

本脚本将整个流程拆成 5 层：

Layer 1. 输入标准化
    将原始期权表统一为标准字段：
    price, T_days, K, flag, r, underlying_price, weight

Layer 2. 权重构造（可选）
    在函数外部或本脚本中构造用于提取 forward 的权重。
    例如：
    - volume
    - oi
    - volume × ATM 邻近权重
    - 自定义报价质量权重

Layer 3. 单期限 forward 提取
    对每个 T_days 的横截面，利用 put-call parity 从多个 strike 提取一个 forward：
        F_k = K + exp(rT) * (C - P)
    再对不同 strike 上的 F_k 做聚合，得到单一期限的 F(T)。

Layer 4. 整条 forward curve 构建
    对所有期限逐个提取 forward，形成离散曲线。
    对缺失期限进行补点：
    - 中间缺口：log-linear interpolation
    - 边界缺口：flat extrapolation
    若某期限完全无法从 parity 提取，则可选择 fallback 到：
        F = S * exp(rT)
    这等价于假设 q = 0，是一个工程兜底，不代表严格理论正确。

Layer 5. 后处理
    从 forward curve 派生其他对象，例如：
    - 隐含 carry curve: (r - q)(T) = ln(F / S) / T
    - 将 forward 回填到逐行期权表
    - 供后续 IV / Greeks / surface 引擎使用

----------------------------------------------------------------------
三、两种 forward 模式
----------------------------------------------------------------------

1. implied_forward 模式
-----------------------
优先使用期权横截面信息，从 call-put parity 隐含提取 forward。

优点：
- 更贴近市场报价；
- 更适合后续按市场一致性定价；
- 对做 IV / Greeks / PnL 归因更自然。

缺点：
- 会受到期权报价噪音影响；
- 深虚值/深实值或配对不完整时容易失败；
- 若某期限没有有效的 C/P 配对，需要 fallback。

2. exogenous_forward 模式
-------------------------
不从期权横截面提取，而是直接使用：
    F = S * exp(rT)

优点：
- 更稳定；
- 当期权链很脏、很 sparse 时可作为兜底；
- 可作为 parity 提取结果的对照。

缺点：
- 相当于默认 q = 0；
- 若资产真实存在分红、carry、基差，该 forward 会有系统性偏差；
- 对市场一致性不如 implied_forward。

----------------------------------------------------------------------
四、为什么用 parity 提取单期限 forward
----------------------------------------------------------------------

欧式期权的 put-call parity 为：

    C - P = exp(-rT) * (F - K)

移项可得：

    F = K + exp(rT) * (C - P)

因此，只要在同一期限、同一 strike 上同时有 call 和 put，
就可以从该 strike 反推出一个 F_k。

但实际市场中：
- 不同 strike 上的 F_k 会因报价噪音而不同；
- 深虚值/深实值点通常不够稳；
- 某些 strike 只有 C 或只有 P；
- 某些期限甚至整个链条配对不全。

所以脚本不直接信任单个 strike，而是：
1. 先计算多个 strike 上的 F_k；
2. 再对它们按权重聚合，形成单一期限的 F(T)。

----------------------------------------------------------------------
五、为什么要把权重设计放到函数外
----------------------------------------------------------------------

本脚本不强制把“ATM 邻近性”写死在主函数内部，而是鼓励用户在函数外构造 weight。

原因：
- 职责更清晰：主函数负责“聚合”，外部负责“信谁更多”；
- 更灵活：你可以自由尝试不同权重设计；
- 更适合研究：可比较 volume、OI、ATM-kernel、报价质量分数等不同方案。

典型做法：
    final_weight = raw_liquidity_weight * atm_proximity_weight * quote_quality_weight

若你已经在外部算好了 weight，本脚本会直接使用；
若没有，则默认等权。

----------------------------------------------------------------------
六、补点逻辑
----------------------------------------------------------------------

本脚本对离散期限曲线使用：

1. 中间缺口：log-linear interpolation
   即在 log(F) 上做线性插值，而不是直接在 F 上线性插值。

   原因：
   - forward 更接近乘法结构；
   - log-linear 比直接线性更自然；
   - 一般比对 F 直接线性插值更稳。

2. 边界缺口：flat extrapolation
   即用最近的已知期限 forward 进行平端外推。

   原因：
   - 作为工程兜底更稳定；
   - 避免在边界强行假设 carry 结构；
   - 比“按单一期限 r 去滚动 F”更少引入错误经济假设。

----------------------------------------------------------------------
七、失败与异常处理原则
----------------------------------------------------------------------

本脚本遵循以下原则：

1. 单个期限失败，不应导致整条曲线构建中断；
2. 尽量把失败写进状态列 status_raw，而不是直接 raise；
3. 数值失败时返回 np.nan；
4. 若所有期限都无法构建有效 forward，则返回空 ForwardCurve；
5. attach_forward_to_options 时，空 curve 会自然回填 np.nan。

这使得脚本更适合用于批量历史数据、回测、日频更新等场景。

----------------------------------------------------------------------
八、适用范围与限制
----------------------------------------------------------------------

本脚本默认基于欧式 put-call parity。
因此以下情况应特别注意：

- 若期权是美式，严格 parity 不再完全成立；
- 若标的存在显著分红、仓储成本、便利收益等，而你又使用了 exogenous_forward 模式，
  则 F = S * exp(rT) 只是工程近似；
- 若期货期权已有对应期货合约市场价，很多时候直接使用市场期货价作为 forward anchor
  会比从期权链反推更稳；
- 本脚本关注的是 forward 提取，不负责期权定价本身。

----------------------------------------------------------------------
九、典型使用方式
----------------------------------------------------------------------

方式 A：先在外部构造权重，再提 implied forward curve
    1. 原始数据标准化
    2. 用 volume / OI / ATM kernel 构造 weight
    3. 调用 build_implied_forward_curve(...)
    4. 将 curve 回填到期权表，用于 IV / Greeks

方式 B：直接使用 exogenous forward curve
    1. 准备 underlying_price 和 r
    2. 调用 build_exogenous_forward_curve(...)
    3. 用于稳定性优先的任务或作为 implied forward 的对照

----------------------------------------------------------------------
十、输出对象
----------------------------------------------------------------------

1. ForwardCurve
   - 保存离散期限上的 F(T)
   - 提供 get_forward(T_days) 查询接口
   - 提供 summary() 查看整条曲线

2. ForwardCurveBuildResult
   - curve: ForwardCurve
   - maturity_table: 每个期限的原始提取结果、状态、最终 forward 等调试信息

maturity_table 中常见字段：
- F_raw: 该期限原始提取或 fallback 得到的 forward
- method_raw: 原始 forward 的来源
- status_raw: 原始提取状态
- F_final: 最终进入曲线的 forward
- method_final: 最终 forward 的来源（原始/curve_fill/missing）

"""
# =========================================================
# Layer 1. Standardization / Input Preparation
# =========================================================

def prepare_option_table(
    opt_df: pd.DataFrame,
    *,
    price_col: str = "price",
    maturity_col: str = "T_days",
    strike_col: str = "K",
    flag_col: str = "flag",
    rate_col: str = "r",
    spot_col: str | None = "underlying_price",
    weight_col: str | None = "weight",
    extra_rename: dict[str, str] | None = None,
) -> pd.DataFrame:
    """
    标准化输入期权表。

    输出标准列：
    - price
    - T_days
    - K
    - flag  ('C' / 'P')
    - r
    - underlying_price   (optional)
    - weight             (optional)

    保留原表其他列，不强行删除。
    """
    if not isinstance(opt_df, pd.DataFrame):
        raise TypeError("opt_df must be a pandas DataFrame")

    df = opt_df.copy()

    rename_map: dict[str, str] = {
        price_col: "price",
        maturity_col: "T_days",
        strike_col: "K",
        flag_col: "flag",
        rate_col: "r",
    }
    if spot_col is not None and spot_col in df.columns:
        rename_map[spot_col] = "underlying_price"
    if weight_col is not None and weight_col in df.columns:
        rename_map[weight_col] = "weight"
    if extra_rename:
        rename_map.update(extra_rename)

    df = df.rename(columns=rename_map)

    required_cols = ["price", "T_days", "K", "flag", "r"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(f"prepare_option_table 缺少必要列: {missing}")

    for c in ["price", "T_days", "K", "r"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    if "underlying_price" in df.columns:
        df["underlying_price"] = pd.to_numeric(df["underlying_price"], errors="coerce")
    if "weight" in df.columns:
        df["weight"] = pd.to_numeric(df["weight"], errors="coerce")

    df["flag"] = df["flag"].astype(str).str.upper().str.strip()
    df.loc[df["flag"].isin(["CALL"]), "flag"] = "C"
    df.loc[df["flag"].isin(["PUT"]), "flag"] = "P"

    # 基础清洗：这里只做最小过滤
    df = df[df["flag"].isin(["C", "P"])].copy()
    df = df[np.isfinite(df["price"])].copy()
    df = df[np.isfinite(df["T_days"])].copy()
    df = df[np.isfinite(df["K"])].copy()
    df = df[np.isfinite(df["r"])].copy()
    df = df[(df["price"] >= 0.0) & (df["T_days"] > 0.0) & (df["K"] > 0.0)].copy()

    if "weight" not in df.columns:
        df["weight"] = 1.0
    else:
        df["weight"] = df["weight"].where(np.isfinite(df["weight"]), 1.0)
        df["weight"] = df["weight"].clip(lower=0.0)

    return df.reset_index(drop=True)


# =========================================================
# Layer 2. Weight Builder (optional)
# =========================================================

def build_forward_weights(
    opt_df: pd.DataFrame,
    *,
    annual_days: int = 365,
    liquidity_col: str | None = None,
    use_atm_kernel: bool = False,
    atm_kernel_width: float | None = None,
    min_weight: float = 0.0,
    out_col: str = "weight",
) -> pd.DataFrame:
    """
    构造 forward 提取用权重。

    说明：
    - 这层是可选的。你也可以在函数外自己把最终权重算好，再传给主流程。
    - 如果 use_atm_kernel=False，本函数只做权重规范化。
    - 如果 use_atm_kernel=True，会先粗估每个期限的 F_tilde，再给 ATM 附近更高权重。
    """
    df = prepare_option_table(opt_df)

    if liquidity_col is not None and liquidity_col in df.columns:
        liq = pd.to_numeric(df[liquidity_col], errors="coerce")
        liq = liq.where(np.isfinite(liq), 0.0).clip(lower=0.0)
        df[out_col] = liq
    else:
        df[out_col] = pd.to_numeric(df.get("weight", 1.0), errors="coerce").fillna(0.0).clip(lower=0.0)

    if not use_atm_kernel:
        df[out_col] = df[out_col].clip(lower=min_weight)
        return df

    paired = _make_paired_cp_table(df, annual_days=annual_days, weight_col=out_col)
    if paired.empty:
        df[out_col] = df[out_col].clip(lower=min_weight)
        return df

    rough_forward_by_t = (
        paired.groupby("T_days")["F_k"]
        .median()
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
        .to_dict()
    )

    if len(rough_forward_by_t) == 0:
        df[out_col] = df[out_col].clip(lower=min_weight)
        return df

    atm_multiplier = np.ones(len(df), dtype=float)

    for t_days, sub_idx in df.groupby("T_days").groups.items():
        if t_days not in rough_forward_by_t:
            continue

        f_tilde = float(rough_forward_by_t[t_days])
        if not np.isfinite(f_tilde) or f_tilde <= 0:
            continue

        strikes = np.sort(df.loc[sub_idx, "K"].dropna().unique())
        if atm_kernel_width is not None and atm_kernel_width > 0:
            width = float(atm_kernel_width)
        else:
            if len(strikes) >= 2:
                diffs = np.diff(strikes)
                grid_step = float(np.nanmedian(diffs)) if len(diffs) > 0 else np.nan
            else:
                grid_step = np.nan

            if np.isfinite(grid_step) and grid_step > 0:
                width = max(2.0 * grid_step, 0.05 * f_tilde)
            else:
                width = max(0.05 * f_tilde, 1e-8)

        k_vals = df.loc[sub_idx, "K"].to_numpy(dtype=float)
        z = (k_vals - f_tilde) / width
        atm_multiplier[list(sub_idx)] = np.exp(-0.5 * z * z)

    df[out_col] = np.clip(
        df[out_col].to_numpy(dtype=float) * atm_multiplier,
        a_min=min_weight,
        a_max=None,
    )
    return df


# =========================================================
# Layer 3. Single Maturity Forward Extraction
# =========================================================

def _make_paired_cp_table(
    opt_df: pd.DataFrame,
    *,
    annual_days: int = 365,
    weight_col: str = "weight",
) -> pd.DataFrame:
    """
    将逐行期权表聚合为按 (T_days, K) 配对后的表：
    包含 C, P, r, w, underlying_price, F_k。
    """
    df = prepare_option_table(opt_df)

    if weight_col not in df.columns:
        df[weight_col] = 1.0
    else:
        df[weight_col] = pd.to_numeric(df[weight_col], errors="coerce").fillna(0.0).clip(lower=0.0)

    agg_map: dict[str, Any] = {
        "price": "mean",
        "r": "mean",
        weight_col: "mean",
    }
    if "underlying_price" in df.columns:
        agg_map["underlying_price"] = "mean"

    tmp = (
        df.groupby(["T_days", "K", "flag"], as_index=False)
        .agg(agg_map)
        .sort_values(["T_days", "K", "flag"])
        .reset_index(drop=True)
    )

    price_wide = tmp.pivot(index=["T_days", "K"], columns="flag", values="price")
    r_by_pair = tmp.groupby(["T_days", "K"])["r"].mean()
    out = price_wide.join(r_by_pair)

    if "underlying_price" in tmp.columns:
        spot_by_pair = tmp.groupby(["T_days", "K"])["underlying_price"].mean()
        out = out.join(spot_by_pair)

    weight_wide = tmp.pivot(index=["T_days", "K"], columns="flag", values=weight_col)
    if "C" in weight_wide.columns and "P" in weight_wide.columns:
        out["w"] = (weight_wide["C"].fillna(0.0) + weight_wide["P"].fillna(0.0)) / 2.0
    elif "C" in weight_wide.columns:
        out["w"] = weight_wide["C"].fillna(0.0)
    elif "P" in weight_wide.columns:
        out["w"] = weight_wide["P"].fillna(0.0)
    else:
        out["w"] = 0.0

    out = out.reset_index()
    out = out.dropna(subset=["C", "P", "r"]).copy()
    if out.empty:
        return out

    T = out["T_days"].to_numpy(dtype=float) / float(annual_days)
    out["F_k"] = out["K"].to_numpy(dtype=float) + np.exp(out["r"].to_numpy(dtype=float) * T) * (
        out["C"].to_numpy(dtype=float) - out["P"].to_numpy(dtype=float)
    )

    out = out.replace([np.inf, -np.inf], np.nan)
    out = out[np.isfinite(out["F_k"]) & (out["F_k"] > 0)].copy()
    return out.reset_index(drop=True)


@dataclass
class ForwardExtractionResult:
    T_days: float
    F: float | None
    method: str
    n_pairs: int
    f_std: float | None
    f_mean: float | None
    weight_sum: float | None
    status: str


def extract_forward_one_maturity(
    opt_df: pd.DataFrame,
    *,
    annual_days: int = 365,
    weight_col: str = "weight",
    robust_method: str = "weighted_mean",
    min_pairs: int = 1,
    max_rel_dispersion: float | None = None,
) -> ForwardExtractionResult:
    """
    对单个期限提取一个 forward。

    robust_method:
    - 'weighted_mean'
    - 'median'

    max_rel_dispersion:
    - 若给定，则要求 std(F_k) / mean(F_k) <= threshold
    """
    df = prepare_option_table(opt_df)
    unique_t = sorted(df["T_days"].dropna().unique())

    if len(unique_t) == 0:
        return ForwardExtractionResult(
            T_days=np.nan,
            F=None,
            method="none",
            n_pairs=0,
            f_std=None,
            f_mean=None,
            weight_sum=None,
            status="no_valid_rows",
        )

    if len(unique_t) > 1:
        raise ValueError("extract_forward_one_maturity 只接受单一期限数据")

    t_days = float(unique_t[0])
    paired = _make_paired_cp_table(df, annual_days=annual_days, weight_col=weight_col)

    if paired.empty:
        return ForwardExtractionResult(
            T_days=t_days,
            F=None,
            method="none",
            n_pairs=0,
            f_std=None,
            f_mean=None,
            weight_sum=None,
            status="no_cp_pairs",
        )

    n_pairs = int(len(paired))
    f_mean = float(paired["F_k"].mean())
    f_std = float(paired["F_k"].std(ddof=0)) if n_pairs > 1 else 0.0
    rel_disp = f_std / f_mean if (np.isfinite(f_std) and np.isfinite(f_mean) and f_mean > 0) else np.nan
    weight_sum = float(pd.to_numeric(paired["w"], errors="coerce").fillna(0.0).sum())

    if n_pairs < min_pairs:
        return ForwardExtractionResult(
            T_days=t_days,
            F=None,
            method="none",
            n_pairs=n_pairs,
            f_std=f_std,
            f_mean=f_mean,
            weight_sum=weight_sum,
            status="insufficient_pairs",
        )

    if max_rel_dispersion is not None and np.isfinite(rel_disp) and rel_disp > max_rel_dispersion:
        return ForwardExtractionResult(
            T_days=t_days,
            F=None,
            method="none",
            n_pairs=n_pairs,
            f_std=f_std,
            f_mean=f_mean,
            weight_sum=weight_sum,
            status="dispersion_too_large",
        )

    if robust_method == "median":
        f_val = float(paired["F_k"].median())
        method = "implied_median"
    elif robust_method == "weighted_mean":
        w = pd.to_numeric(paired["w"], errors="coerce").fillna(0.0).clip(lower=0.0)
        w_sum = float(w.sum())
        if w_sum > 0:
            w_norm = w.to_numpy(dtype=float) / w_sum
            f_val = float(np.sum(paired["F_k"].to_numpy(dtype=float) * w_norm))
            method = "implied_weighted_mean"
        else:
            f_val = float(paired["F_k"].mean())
            method = "implied_equal_mean"
    else:
        raise ValueError("robust_method must be one of ['weighted_mean', 'median']")

    if not np.isfinite(f_val) or f_val <= 0:
        return ForwardExtractionResult(
            T_days=t_days,
            F=None,
            method="none",
            n_pairs=n_pairs,
            f_std=f_std,
            f_mean=f_mean,
            weight_sum=weight_sum,
            status="invalid_forward",
        )

    return ForwardExtractionResult(
        T_days=t_days,
        F=f_val,
        method=method,
        n_pairs=n_pairs,
        f_std=f_std,
        f_mean=f_mean,
        weight_sum=weight_sum,
        status="ok",
    )


# =========================================================
# Layer 4. Forward Curve Engine
# =========================================================

class ForwardCurve:
    """
    离散期限上的 forward 曲线。

    特点：
    - 内部保存 {T_days: F}
    - 查询时按 T_days 做 log-linear 插值
    - 边界做平端外推
    """

    def __init__(self, F_dict: dict[float, float]):
        clean: dict[float, float] = {}
        for k, v in F_dict.items():
            if pd.notna(k) and pd.notna(v) and np.isfinite(k) and np.isfinite(v) and k > 0 and v > 0:
                clean[float(k)] = float(v)

        self.F_dict = dict(sorted(clean.items(), key=lambda kv: kv[0]))
        self.T_days_grid = np.array(list(self.F_dict.keys()), dtype=float)
        self.F_grid = np.array(list(self.F_dict.values()), dtype=float)

    def __len__(self) -> int:
        return len(self.F_dict)

    def get_forward(self, T_days: float) -> float:
        """
        log-linear interpolation + flat extrapolation
        """
        if not np.isfinite(T_days) or T_days <= 0 or len(self.T_days_grid) == 0:
            return np.nan

        if len(self.T_days_grid) == 1:
            return float(self.F_grid[0])

        if T_days <= self.T_days_grid[0]:
            return float(self.F_grid[0])

        if T_days >= self.T_days_grid[-1]:
            return float(self.F_grid[-1])

        idx = np.searchsorted(self.T_days_grid, T_days, side="right")
        left_idx = idx - 1
        right_idx = idx

        t0 = float(self.T_days_grid[left_idx])
        t1 = float(self.T_days_grid[right_idx])
        f0 = float(self.F_grid[left_idx])
        f1 = float(self.F_grid[right_idx])

        if not (np.isfinite(f0) and np.isfinite(f1) and f0 > 0 and f1 > 0 and t1 > t0):
            return np.nan

        alpha = (T_days - t0) / (t1 - t0)
        log_f = np.log(f0) + alpha * (np.log(f1) - np.log(f0))
        return float(np.exp(log_f))

    def summary(self) -> pd.DataFrame:
        if len(self.T_days_grid) == 0:
            return pd.DataFrame(columns=["T_days", "F"])
        return pd.DataFrame({"T_days": self.T_days_grid, "F": self.F_grid})


@dataclass
class ForwardCurveBuildResult:
    curve: ForwardCurve
    maturity_table: pd.DataFrame


def _spot_implied_forward_from_subdf(sub_df: pd.DataFrame, *, annual_days: int = 365) -> float | None:
    """
    fallback: F = S * exp(rT), 等价于设 q = 0
    """
    if "underlying_price" not in sub_df.columns:
        return None

    s = pd.to_numeric(sub_df["underlying_price"], errors="coerce").mean()
    r = pd.to_numeric(sub_df["r"], errors="coerce").mean()
    t_days = pd.to_numeric(sub_df["T_days"], errors="coerce").mean()

    if not (pd.notna(s) and pd.notna(r) and pd.notna(t_days)):
        return None
    if not (np.isfinite(s) and np.isfinite(r) and np.isfinite(t_days) and s > 0 and t_days > 0):
        return None

    t = float(t_days) / float(annual_days)
    f = float(s * np.exp(r * t))
    return f if np.isfinite(f) and f > 0 else None


def cal_forward_curve(
    opt_df: pd.DataFrame,
    *,
    mode: str = "implied_forward",
    annual_days: int = 365,
    weight_col: str = "weight",
    robust_method: str = "weighted_mean",
    min_pairs: int = 1,
    max_rel_dispersion: float | None = None,
    fallback_to_spot: bool = True,
    fill_missing: bool = True,
    return_details: bool = True,
) -> ForwardCurve | ForwardCurveBuildResult:
    """
    构建 forward curve。

    mode='implied_forward'
        - 优先 parity 提取 F
        - 若失败且 fallback_to_spot=True，则退回 F = S * exp(rT)
        - 对仍缺失的期限，使用 log-linear 插值 + flat 外推补齐

    mode='exogenous_forward'
        - 直接用 F = S * exp(rT)
        - 若 spot 缺失，则后续再靠曲线补点

    关键兜底：
    - 单个期限异常，不中断整个流程；该期限记 np.nan / error status
    - 若所有期限都没有有效锚点，返回空 curve，不抛错
    """
    if mode not in {"implied_forward", "exogenous_forward"}:
        raise ValueError("mode must be one of ['implied_forward', 'exogenous_forward']")

    df = prepare_option_table(opt_df)
    all_t = sorted(df["T_days"].dropna().astype(float).unique())

    records: list[dict[str, Any]] = []
    f_dict: dict[float, float] = {}

    for t_days, sub in df.groupby("T_days", sort=True):
        t_days = float(t_days)
        implied_res: ForwardExtractionResult | None = None
        used_f = np.nan
        used_method = "none"
        status = "not_processed"

        try:
            if mode == "implied_forward":
                implied_res = extract_forward_one_maturity(
                    sub,
                    annual_days=annual_days,
                    weight_col=weight_col,
                    robust_method=robust_method,
                    min_pairs=min_pairs,
                    max_rel_dispersion=max_rel_dispersion,
                )

                if implied_res.F is not None and implied_res.status == "ok":
                    used_f = float(implied_res.F)
                    used_method = implied_res.method
                    status = "ok"

                elif fallback_to_spot:
                    spot_f = _spot_implied_forward_from_subdf(sub, annual_days=annual_days)
                    if spot_f is not None and np.isfinite(spot_f) and spot_f > 0:
                        used_f = float(spot_f)
                        used_method = "spot_carry_fallback"
                        status = f"fallback_from_{implied_res.status}"
                    else:
                        status = implied_res.status
                else:
                    status = implied_res.status

            elif mode == "exogenous_forward":
                spot_f = _spot_implied_forward_from_subdf(sub, annual_days=annual_days)
                if spot_f is not None and np.isfinite(spot_f) and spot_f > 0:
                    used_f = float(spot_f)
                    used_method = "spot_carry"
                    status = "ok"
                else:
                    status = "spot_missing"

        except Exception as e:
            used_f = np.nan
            used_method = "error"
            status = f"error: {type(e).__name__}: {e}"

        if np.isfinite(used_f) and used_f > 0:
            f_dict[t_days] = float(used_f)

        records.append(
            {
                "T_days": t_days,
                "F_raw": float(used_f) if np.isfinite(used_f) else np.nan,
                "method_raw": used_method,
                "status_raw": status,
                "n_pairs": None if implied_res is None else implied_res.n_pairs,
                "f_std": None if implied_res is None else implied_res.f_std,
                "f_mean": None if implied_res is None else implied_res.f_mean,
                "weight_sum": None if implied_res is None else implied_res.weight_sum,
            }
        )

    maturity_table = pd.DataFrame(records).sort_values("T_days").reset_index(drop=True)

    if len(f_dict) == 0:
        out = ForwardCurve({})
        if return_details:
            maturity_table["F_final"] = np.nan
            maturity_table["method_final"] = "missing"
            return ForwardCurveBuildResult(curve=out, maturity_table=maturity_table)
        return out

    if fill_missing:
        anchor_curve = ForwardCurve(f_dict)
        final_f_dict = dict(f_dict)

        final_method_map = {}
        for t in f_dict.keys():
            method_series = maturity_table.loc[maturity_table["T_days"] == t, "method_raw"]
            final_method_map[float(t)] = method_series.iloc[0] if len(method_series) > 0 else "anchor"

        for t_days in all_t:
            if t_days in final_f_dict:
                continue
            f_fill = anchor_curve.get_forward(float(t_days))
            if np.isfinite(f_fill) and f_fill > 0:
                final_f_dict[float(t_days)] = float(f_fill)
                final_method_map[float(t_days)] = "curve_fill"
    else:
        final_f_dict = dict(f_dict)
        final_method_map = {}
        for t in f_dict.keys():
            method_series = maturity_table.loc[maturity_table["T_days"] == t, "method_raw"]
            final_method_map[float(t)] = method_series.iloc[0] if len(method_series) > 0 else "anchor"

    curve = ForwardCurve(final_f_dict)

    maturity_table["F_final"] = maturity_table["T_days"].map(final_f_dict)
    maturity_table["method_final"] = maturity_table["T_days"].map(final_method_map).fillna("missing")

    if return_details:
        return ForwardCurveBuildResult(curve=curve, maturity_table=maturity_table)
    return curve


# =========================================================
# Layer 5. Derived Objects / Post Processing
# =========================================================

def derive_carry_curve(
    curve: ForwardCurve,
    spot: float,
    *,
    annual_days: int = 365,
) -> pd.DataFrame:
    """
    由 forward curve 派生隐含净 carry:
        (r - q)(T) = ln(F / S) / T
    """
    if not np.isfinite(spot) or spot <= 0:
        raise ValueError("spot must be positive and finite")

    summary = curve.summary().copy()
    if summary.empty:
        return pd.DataFrame(columns=["T_days", "F", "carry"])

    t = summary["T_days"].to_numpy(dtype=float) / float(annual_days)
    f = summary["F"].to_numpy(dtype=float)
    carry = np.log(f / float(spot)) / t

    out = summary.copy()
    out["carry"] = carry
    return out


def attach_forward_to_options(
    opt_df: pd.DataFrame,
    curve: ForwardCurve,
    *,
    out_col: str = "forward",
) -> pd.DataFrame:
    """
    将 curve 上的 forward 回填到逐行期权表。
    """
    df = prepare_option_table(opt_df)
    df[out_col] = df["T_days"].apply(curve.get_forward)
    return df


# =========================================================
# Convenience Wrappers
# =========================================================

def build_implied_forward_curve(
    opt_df: pd.DataFrame,
    *,
    annual_days: int = 365,
    weight_col: str = "weight",
    robust_method: str = "weighted_mean",
    min_pairs: int = 1,
    max_rel_dispersion: float | None = None,
    fallback_to_spot: bool = True,
    fill_missing: bool = True,
    return_details: bool = True,
) -> ForwardCurve | ForwardCurveBuildResult:
    return cal_forward_curve(
        opt_df,
        mode="implied_forward",
        annual_days=annual_days,
        weight_col=weight_col,
        robust_method=robust_method,
        min_pairs=min_pairs,
        max_rel_dispersion=max_rel_dispersion,
        fallback_to_spot=fallback_to_spot,
        fill_missing=fill_missing,
        return_details=return_details,
    )


def build_exogenous_forward_curve(
    opt_df: pd.DataFrame,
    *,
    annual_days: int = 365,
    fill_missing: bool = True,
    return_details: bool = True,
) -> ForwardCurve | ForwardCurveBuildResult:
    return cal_forward_curve(
        opt_df,
        mode="exogenous_forward",
        annual_days=annual_days,
        fill_missing=fill_missing,
        return_details=return_details,
    )

@dataclass
class MultiDateForwardBuildResult:
    """
    多交易日批量构建 forward curve 的返回对象。

    字段
    ----
    curve_map:
        dict[trade_date, ForwardCurve]
        每个交易日对应一条 ForwardCurve。若该日构建失败，则对应空曲线。

    curve_summary:
        DataFrame
        将每个交易日的 curve.summary() 纵向拼接后的结果，至少包含：
        - trade_date
        - T_days
        - F

    maturity_panel:
        DataFrame
        将每个交易日的 maturity_table 纵向拼接后的结果，至少包含：
        - trade_date
        - T_days
        - F_raw
        - F_final
        - method_raw
        - method_final
        - status_raw
        等调试字段。
    """
    curve_map: dict[Any, ForwardCurve]
    curve_summary: pd.DataFrame
    maturity_panel: pd.DataFrame

def _build_single_date_forward_task(
    trade_date: Any,
    sub_df: pd.DataFrame,
    *,
    mode: str,
    annual_days: int,
    weight_col: str,
    robust_method: str,
    min_pairs: int,
    max_rel_dispersion: float | None,
    fallback_to_spot: bool,
    fill_missing: bool,
) -> tuple[Any, ForwardCurve, pd.DataFrame]:
    """
    单个交易日的 forward curve 构建任务。

    返回
    ----
    (trade_date, curve, maturity_table)

    设计原则
    --------
    - 单日失败不抛到最外层；
    - 若失败，则返回空 curve + 一张记录错误信息的 maturity_table；
    - 这样多日批量任务不会因为单个日期异常而整体中断。
    """
    try:
        res = cal_forward_curve(
            sub_df,
            mode=mode,
            annual_days=annual_days,
            weight_col=weight_col,
            robust_method=robust_method,
            min_pairs=min_pairs,
            max_rel_dispersion=max_rel_dispersion,
            fallback_to_spot=fallback_to_spot,
            fill_missing=fill_missing,
            return_details=True,
        )

        maturity_table = res.maturity_table.copy()
        maturity_table.insert(0, "trade_date", trade_date)
        return trade_date, res.curve, maturity_table

    except Exception as e:
        # 单个交易日彻底失败时，仍然返回空结果，不中断整体批处理
        empty_curve = ForwardCurve({})
        err_df = pd.DataFrame(
            {
                "trade_date": [trade_date],
                "T_days": [np.nan],
                "F_raw": [np.nan],
                "method_raw": ["error"],
                "status_raw": [f"error: {type(e).__name__}: {e}"],
                "n_pairs": [np.nan],
                "f_std": [np.nan],
                "f_mean": [np.nan],
                "weight_sum": [np.nan],
                "F_final": [np.nan],
                "method_final": ["missing"],
            }
        )
        return trade_date, empty_curve, err_df

def build_forward_curves_by_date(
    opt_df: pd.DataFrame,
    *,
    date_col: str = "trade_date",
    mode: str = "implied_forward",
    annual_days: int = 365,
    weight_col: str = "weight",
    robust_method: str = "weighted_mean",
    min_pairs: int = 1,
    max_rel_dispersion: float | None = None,
    fallback_to_spot: bool = True,
    fill_missing: bool = True,
    n_jobs: int = 1,
    show_progress: bool = True,
    sort_dates: bool = True,
) -> MultiDateForwardBuildResult:
    """
    按交易日批量构建 forward curve。

    作用
    ----
    当输入数据包含多个交易日时，本函数会按 date_col 分组，
    对每个交易日分别调用一次 cal_forward_curve(...)，
    最终返回：
    1. 每个交易日对应的 ForwardCurve；
    2. 所有交易日拼接后的 curve_summary；
    3. 所有交易日拼接后的 maturity_panel。

    参数
    ----
    date_col:
        交易日列名，例如 'trade_date'。

    n_jobs:
        - n_jobs=1: 默认单进程 for 循环；
        - n_jobs>1: 使用 ProcessPoolExecutor 开启多进程。

    show_progress:
        是否显示 tqdm 进度条。

    返回
    ----
    MultiDateForwardBuildResult:
    - curve_map: dict[trade_date, ForwardCurve]
    - curve_summary: DataFrame，列含 trade_date, T_days, F
    - maturity_panel: DataFrame，列含 trade_date + maturity_table 详细信息

    注意
    ----
    1. Windows 下使用多进程时，调用入口必须放在：
           if __name__ == "__main__":
       里面，否则可能重复启动子进程。

    2. 单个交易日失败不会中断整体流程，
       该交易日会返回空 curve，并在 maturity_panel 中记录 error 状态。
    """
    if date_col not in opt_df.columns:
        raise KeyError(f"build_forward_curves_by_date 缺少日期列: {date_col}")

    if n_jobs < 1:
        raise ValueError("n_jobs must be >= 1")

    df = opt_df.copy()
    date_values = df[date_col].dropna().unique().tolist()
    if sort_dates:
        try:
            date_values = sorted(date_values)
        except Exception:
            pass

    grouped_data: list[tuple[Any, pd.DataFrame]] = [
        (dt, df.loc[df[date_col] == dt].copy()) for dt in date_values
    ]

    curve_map: dict[Any, ForwardCurve] = {}
    maturity_tables: list[pd.DataFrame] = []

    # ---------- 单进程 ----------
    if n_jobs == 1:
        iterator = grouped_data
        if show_progress:
            iterator = tqdm(iterator, total=len(grouped_data), desc="Building forward curves by date")

        for trade_date, sub_df in iterator:
            dt, curve, maturity_table = _build_single_date_forward_task(
                trade_date,
                sub_df,
                mode=mode,
                annual_days=annual_days,
                weight_col=weight_col,
                robust_method=robust_method,
                min_pairs=min_pairs,
                max_rel_dispersion=max_rel_dispersion,
                fallback_to_spot=fallback_to_spot,
                fill_missing=fill_missing,
            )
            curve_map[dt] = curve
            maturity_tables.append(maturity_table)

    # ---------- 多进程 ----------
    else:
        futures = []
        with ProcessPoolExecutor(max_workers=n_jobs) as executor:
            for trade_date, sub_df in grouped_data:
                fut = executor.submit(
                    _build_single_date_forward_task,
                    trade_date,
                    sub_df,
                    mode=mode,
                    annual_days=annual_days,
                    weight_col=weight_col,
                    robust_method=robust_method,
                    min_pairs=min_pairs,
                    max_rel_dispersion=max_rel_dispersion,
                    fallback_to_spot=fallback_to_spot,
                    fill_missing=fill_missing,
                )
                futures.append(fut)

            iterator = as_completed(futures)
            if show_progress:
                iterator = tqdm(iterator, total=len(futures), desc="Building forward curves by date")

            for fut in iterator:
                dt, curve, maturity_table = fut.result()
                curve_map[dt] = curve
                maturity_tables.append(maturity_table)

    # 拼接 maturity_panel
    if len(maturity_tables) > 0:
        maturity_panel = pd.concat(maturity_tables, axis=0, ignore_index=True)
    else:
        maturity_panel = pd.DataFrame(
            columns=[
                "trade_date",
                "T_days",
                "F_raw",
                "method_raw",
                "status_raw",
                "n_pairs",
                "f_std",
                "f_mean",
                "weight_sum",
                "F_final",
                "method_final",
            ]
        )

    # 拼接 curve_summary
    curve_summary_list: list[pd.DataFrame] = []
    for trade_date, curve in curve_map.items():
        sub_summary = curve.summary().copy()
        if sub_summary.empty:
            continue
        sub_summary.insert(0, "trade_date", trade_date)
        curve_summary_list.append(sub_summary)

    if len(curve_summary_list) > 0:
        curve_summary = pd.concat(curve_summary_list, axis=0, ignore_index=True)
    else:
        curve_summary = pd.DataFrame(columns=["trade_date", "T_days", "F"])

    # 若要求按日期和期限排序
    if not maturity_panel.empty:
        sort_cols = [c for c in ["trade_date", "T_days"] if c in maturity_panel.columns]
        if len(sort_cols) > 0:
            maturity_panel = maturity_panel.sort_values(sort_cols).reset_index(drop=True)

    if not curve_summary.empty:
        sort_cols = [c for c in ["trade_date", "T_days"] if c in curve_summary.columns]
        if len(sort_cols) > 0:
            curve_summary = curve_summary.sort_values(sort_cols).reset_index(drop=True)

    return MultiDateForwardBuildResult(
        curve_map=curve_map,
        curve_summary=curve_summary,
        maturity_panel=maturity_panel,
    )
# =========================================================
# Example
# =========================================================
if __name__ == "__main__":
    # 示例包含：
    # 1) 44 天：正常的 C/P 配对，可直接提 implied forward
    # 2) 72 天：某个 strike 只有 call，没有 put；其余 strike 正常，仍可提取
    # 3) 135 天：只有 call，没有任何 put；若有 spot，则走 spot fallback
    # 4) 226 天：有一部分配对，但 underlying_price 缺失；若能从 parity 提取就正常，否则只能留空/等 curve fill

    sample = pd.DataFrame(
        [
            # ===== 44 days, 正常链 =====
            {"price": 0.1826, "T_days": 44, "K": 2.20, "flag": "C", "r": 0.020, "underlying_price": 2.30, "volume": 2501},
            {"price": 0.0617, "T_days": 44, "K": 2.20, "flag": "P", "r": 0.020, "underlying_price": 2.30, "volume": 1674},
            {"price": 0.1460, "T_days": 44, "K": 2.25, "flag": "C", "r": 0.020, "underlying_price": 2.30, "volume": 1250},
            {"price": 0.0777, "T_days": 44, "K": 2.25, "flag": "P", "r": 0.020, "underlying_price": 2.30, "volume": 822},
            {"price": 0.1225, "T_days": 44, "K": 2.30, "flag": "C", "r": 0.020, "underlying_price": 2.30, "volume": 1323},
            {"price": 0.0969, "T_days": 44, "K": 2.30, "flag": "P", "r": 0.020, "underlying_price": 2.30, "volume": 852},

            # ===== 72 days, 一个 strike 只有 call =====
            {"price": 0.2100, "T_days": 72, "K": 2.20, "flag": "C", "r": 0.021, "underlying_price": 2.30, "volume": 180},
            {"price": 0.0880, "T_days": 72, "K": 2.20, "flag": "P", "r": 0.021, "underlying_price": 2.30, "volume": 310},
            {"price": 0.1785, "T_days": 72, "K": 2.25, "flag": "C", "r": 0.021, "underlying_price": 2.30, "volume": 302},
            {"price": 0.1111, "T_days": 72, "K": 2.25, "flag": "P", "r": 0.021, "underlying_price": 2.30, "volume": 314},
            {"price": 0.1466, "T_days": 72, "K": 2.30, "flag": "C", "r": 0.021, "underlying_price": 2.30, "volume": 378},
            # 这里故意缺少 2.30 的 put
            {"price": 0.1261, "T_days": 72, "K": 2.35, "flag": "C", "r": 0.021, "underlying_price": 2.30, "volume": 331},
            {"price": 0.1614, "T_days": 72, "K": 2.35, "flag": "P", "r": 0.021, "underlying_price": 2.30, "volume": 284},

            # ===== 135 days, 只有 call，没有任何 put -> implied 失败, 走 spot fallback =====
            {"price": 0.2662, "T_days": 135, "K": 2.20, "flag": "C", "r": 0.022, "underlying_price": 2.31, "volume": 229},
            {"price": 0.2366, "T_days": 135, "K": 2.25, "flag": "C", "r": 0.022, "underlying_price": 2.31, "volume": 114},
            {"price": 0.2105, "T_days": 135, "K": 2.30, "flag": "C", "r": 0.022, "underlying_price": 2.31, "volume": 161},

            # ===== 226 days, 有配对，但 spot 缺失 =====
            {"price": 0.3130, "T_days": 226, "K": 2.20, "flag": "C", "r": 0.023, "underlying_price": np.nan, "volume": 247},
            {"price": 0.1795, "T_days": 226, "K": 2.20, "flag": "P", "r": 0.023, "underlying_price": np.nan, "volume": 63},
            {"price": 0.2787, "T_days": 226, "K": 2.25, "flag": "C", "r": 0.023, "underlying_price": np.nan, "volume": 451},
            {"price": 0.2116, "T_days": 226, "K": 2.25, "flag": "P", "r": 0.023, "underlying_price": np.nan, "volume": 48},

            # ===== 300 days, 特殊情况：只有 put，没有 call，也没有 spot =====
            # 这个期限会失败，并在 fill_missing=True 时尝试用曲线补点
            {"price": 0.3500, "T_days": 300, "K": 2.20, "flag": "P", "r": 0.024, "underlying_price": np.nan, "volume": 30},
            {"price": 0.3900, "T_days": 300, "K": 2.30, "flag": "P", "r": 0.024, "underlying_price": np.nan, "volume": 18},
        ]
    )

    # 先构造一个更灵活的权重：用 volume 做基础权重，并叠加 ATM kernel
    weighted = build_forward_weights(
        sample,
        liquidity_col="volume",
        use_atm_kernel=True,
        out_col="weight",
    )

    res = build_implied_forward_curve(
        weighted,
        weight_col="weight",
        robust_method="weighted_mean",
        min_pairs=1,
        max_rel_dispersion=None,
        fallback_to_spot=True,
        fill_missing=True,
        return_details=True,
    )

    print("===== maturity_table =====")
    print(res.maturity_table)
    print()

    print("===== forward curve =====")
    print(res.curve.summary())
    print()

    attached = attach_forward_to_options(weighted, res.curve, out_col="forward")
    print("===== options with attached forward =====")
    print(attached[["T_days", "K", "flag", "price", "weight", "forward"]].head(20))
    print()

    # 若你想看隐含 carry，可给一个 spot
    carry_df = derive_carry_curve(res.curve, spot=2.30)
    print("===== carry curve =====")
    print(carry_df)

    sample_day1 = pd.DataFrame(
        [
            # ===== 44 days, 正常链 =====
            {"trade_date": "2024-01-02", "price": 0.1826, "T_days": 44, "K": 2.20, "flag": "C", "r": 0.020,
             "underlying_price": 2.30, "volume": 2501},
            {"trade_date": "2024-01-02", "price": 0.0617, "T_days": 44, "K": 2.20, "flag": "P", "r": 0.020,
             "underlying_price": 2.30, "volume": 1674},
            {"trade_date": "2024-01-02", "price": 0.1460, "T_days": 44, "K": 2.25, "flag": "C", "r": 0.020,
             "underlying_price": 2.30, "volume": 1250},
            {"trade_date": "2024-01-02", "price": 0.0777, "T_days": 44, "K": 2.25, "flag": "P", "r": 0.020,
             "underlying_price": 2.30, "volume": 822},
            {"trade_date": "2024-01-02", "price": 0.1225, "T_days": 44, "K": 2.30, "flag": "C", "r": 0.020,
             "underlying_price": 2.30, "volume": 1323},
            {"trade_date": "2024-01-02", "price": 0.0969, "T_days": 44, "K": 2.30, "flag": "P", "r": 0.020,
             "underlying_price": 2.30, "volume": 852},

            # ===== 72 days, 一个 strike 缺 put =====
            {"trade_date": "2024-01-02", "price": 0.2100, "T_days": 72, "K": 2.20, "flag": "C", "r": 0.021,
             "underlying_price": 2.30, "volume": 180},
            {"trade_date": "2024-01-02", "price": 0.0880, "T_days": 72, "K": 2.20, "flag": "P", "r": 0.021,
             "underlying_price": 2.30, "volume": 310},
            {"trade_date": "2024-01-02", "price": 0.1785, "T_days": 72, "K": 2.25, "flag": "C", "r": 0.021,
             "underlying_price": 2.30, "volume": 302},
            {"trade_date": "2024-01-02", "price": 0.1111, "T_days": 72, "K": 2.25, "flag": "P", "r": 0.021,
             "underlying_price": 2.30, "volume": 314},
            {"trade_date": "2024-01-02", "price": 0.1466, "T_days": 72, "K": 2.30, "flag": "C", "r": 0.021,
             "underlying_price": 2.30, "volume": 378},
            # 故意缺 2.30 的 put
            {"trade_date": "2024-01-02", "price": 0.1261, "T_days": 72, "K": 2.35, "flag": "C", "r": 0.021,
             "underlying_price": 2.30, "volume": 331},
            {"trade_date": "2024-01-02", "price": 0.1614, "T_days": 72, "K": 2.35, "flag": "P", "r": 0.021,
             "underlying_price": 2.30, "volume": 284},

            # ===== 135 days, 只有 call，触发 spot fallback =====
            {"trade_date": "2024-01-02", "price": 0.2662, "T_days": 135, "K": 2.20, "flag": "C", "r": 0.022,
             "underlying_price": 2.31, "volume": 229},
            {"trade_date": "2024-01-02", "price": 0.2366, "T_days": 135, "K": 2.25, "flag": "C", "r": 0.022,
             "underlying_price": 2.31, "volume": 114},
            {"trade_date": "2024-01-02", "price": 0.2105, "T_days": 135, "K": 2.30, "flag": "C", "r": 0.022,
             "underlying_price": 2.31, "volume": 161},

            # ===== 226 days, 有配对但没有 spot =====
            {"trade_date": "2024-01-02", "price": 0.3130, "T_days": 226, "K": 2.20, "flag": "C", "r": 0.023,
             "underlying_price": np.nan, "volume": 247},
            {"trade_date": "2024-01-02", "price": 0.1795, "T_days": 226, "K": 2.20, "flag": "P", "r": 0.023,
             "underlying_price": np.nan, "volume": 63},
            {"trade_date": "2024-01-02", "price": 0.2787, "T_days": 226, "K": 2.25, "flag": "C", "r": 0.023,
             "underlying_price": np.nan, "volume": 451},
            {"trade_date": "2024-01-02", "price": 0.2116, "T_days": 226, "K": 2.25, "flag": "P", "r": 0.023,
             "underlying_price": np.nan, "volume": 48},

            # ===== 300 days, 只有 put 且没 spot =====
            {"trade_date": "2024-01-02", "price": 0.3500, "T_days": 300, "K": 2.20, "flag": "P", "r": 0.024,
             "underlying_price": np.nan, "volume": 30},
            {"trade_date": "2024-01-02", "price": 0.3900, "T_days": 300, "K": 2.30, "flag": "P", "r": 0.024,
             "underlying_price": np.nan, "volume": 18},
        ]
    )

    # 第二天，简单模拟价格略有变化
    sample_day2 = sample_day1.copy()
    sample_day2["trade_date"] = "2024-01-03"
    sample_day2["price"] = sample_day2["price"] * 1.01
    sample_day2.loc[sample_day2["T_days"] == 135, "underlying_price"] = 2.32

    df_sample = pd.concat([sample_day1, sample_day2], ignore_index=True)
    df_sample.columns
    print(df_sample.head(10))
    print(df_sample["trade_date"].value_counts())

    weighted_df = build_forward_weights(
        df_sample,
        liquidity_col="volume",
        use_atm_kernel=True,
        out_col="weight",
    )

    multi_res = build_forward_curves_by_date(
        weighted_df,
        date_col="trade_date",
        mode="implied_forward",
        weight_col="weight",
        robust_method="weighted_mean",
        min_pairs=1,
        max_rel_dispersion=None,
        fallback_to_spot=True,
        fill_missing=True,
        n_jobs=1,  # 先单进程试
        show_progress=True,
    )

    print("===== curve_summary =====")
    print(multi_res.curve_summary)
    print()

    print("===== maturity_panel =====")
    print(
        multi_res.maturity_panel[
            ["trade_date", "T_days", "F_raw", "F_final", "method_raw", "method_final", "status_raw"]
        ]
    )
