import time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from .opt_forward_curve import build_implied_forward_curve
from tqdm import tqdm
import multiprocessing as mp
import traceback

def calc_sigma2_and_t(option_data: pd.DataFrame):
    F = option_data["F"].iloc[0]
    r = option_data["r"].iloc[0]
    T = option_data["T_days"].iloc[0] / 365.0

    strike_list = np.sort(option_data["K"].unique())
    K0 = strike_list[strike_list <= F].max()

    qk_list = []
    for K in strike_list:
        strike_data = option_data[option_data["K"] == K]
        call_price = strike_data.loc[strike_data["flag"] == "C", "price"]
        put_price = strike_data.loc[strike_data["flag"] == "P", "price"]

        if K < K0:
            if put_price.empty:
                continue
            qk = put_price.iloc[0]

        elif K > K0:
            if call_price.empty:
                continue
            qk = call_price.iloc[0]

        else:
            if call_price.empty or put_price.empty:
                Warning(f"K0={K0} 处缺少 call 或 put")
                continue
            qk = (call_price.iloc[0] + put_price.iloc[0]) / 2

        qk_list.append((K, qk))

    otm_surface = pd.DataFrame(qk_list, columns=["K", "QK"]).sort_values("K").reset_index(drop=True)

    strike_array = otm_surface["K"].to_numpy()
    qk_array = otm_surface["QK"].to_numpy()

    delta_k = np.zeros(len(strike_array))
    delta_k[0] = strike_array[1] - strike_array[0]
    delta_k[-1] = strike_array[-1] - strike_array[-2]
    delta_k[1:-1] = (strike_array[2:] - strike_array[:-2]) / 2

    sigma2 = (
        2 / T * np.sum(delta_k / (strike_array ** 2) * np.exp(r * T) * qk_array)
        - 1 / T * (F / K0 - 1) ** 2
    )

    return sigma2, T

def cal_single_day_ivx(opt_df):
    try:
        opt_df = opt_df[opt_df["T_days"] > 7].copy()
        res = build_implied_forward_curve(
            opt_df,
            weight_col="weight",
            robust_method="weighted_mean",
            min_pairs=1,
            fallback_to_spot=True,
            fill_missing=True,
            return_details=True,
        )

        forward_curve = res.curve

        opt_df["F"] = opt_df["T_days"].apply(forward_curve.get_forward)
        valid_t_days_list = []
        for t_days in sorted(opt_df["T_days"].unique()):
            single_expiry_option_data = opt_df[opt_df["T_days"] == t_days]
            F = single_expiry_option_data["F"].iloc[0]
            strike_list = np.sort(single_expiry_option_data["K"].unique())

            if np.any(strike_list <= F) and np.any(strike_list >= F):
                valid_t_days_list.append(t_days)

        if len(valid_t_days_list) == 0:
            return np.nan

        if len(valid_t_days_list) == 1:
            only_opt_df = opt_df[opt_df["T_days"] == valid_t_days_list[0]]
            only_sigma2, only_T = calc_sigma2_and_t(only_opt_df)

            if only_T >= 30 / 365:
                return 100 * np.sqrt(only_sigma2)
            else:
                return np.nan

        near_opt_df = opt_df[opt_df["T_days"] == valid_t_days_list[0]]
        next_opt_df = opt_df[opt_df["T_days"] == valid_t_days_list[1]]

        near_sigma2, near_T = calc_sigma2_and_t(near_opt_df)

        if near_T >= 30 / 365:
            return 100 * np.sqrt(near_sigma2)

        next_sigma2, next_T = calc_sigma2_and_t(next_opt_df)

        T_target = 30 / 365
        sigma2_30 = (
            near_T * near_sigma2 * (next_T - T_target)
            + next_T * next_sigma2 * (T_target - near_T)
        ) / ((next_T - near_T) * T_target)

        return 100 * np.sqrt(sigma2_30)
    except Exception as e:
        traceback.print_exc()
        return np.nan

def _calc_one_day_ivx(args):
    date, opt_df = args
    opt_df = opt_df.dropna().copy()
    value = cal_single_day_ivx(opt_df)
    return date, value


def cal_ivx(option_panel, n_jobs=1, show_progress=True):
    """
    计算按日期的 IVX

    参数
    ----
    option_panel : pd.DataFrame
        必须包含列:
        ['date', 'price', 'T_days', 'K', 'flag', 'r']
    n_jobs : int, default 1
        =1 单进程
        >1 多进程
    show_progress : bool, default True
        是否显示 tqdm 进度条

    返回
    ----
    pd.Series
        index 为 date, value 为对应 IVX
    """
    require_cols = ['date', 'price', 'T_days', 'K', 'flag', 'r']
    missing_cols = set(require_cols) - set(option_panel.columns)
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")

    grouped = [(date, df) for date, df in option_panel.groupby('date')]

    if len(grouped) == 0:
        return pd.Series(dtype='float64', name='ivx')

    # 单进程
    if n_jobs == 1:
        iterator = grouped
        if show_progress:
            iterator = tqdm(iterator, total=len(grouped), desc='Calculating IVX')

        results = []
        for item in iterator:
            results.append(_calc_one_day_ivx(item))

    # 多进程
    else:
        n_jobs = max(1, int(n_jobs))
        with mp.Pool(processes=n_jobs) as pool:
            iterator = pool.imap(_calc_one_day_ivx, grouped)
            if show_progress:
                iterator = tqdm(iterator, total=len(grouped), desc=f'Calculating IVX ({n_jobs} proc)')
            results = list(iterator)

    ivx = pd.Series(
        {date: value for date, value in results},
        dtype='float64',
        name='ivx'
    ).sort_index()

    return ivx

if __name__ == '__main__':
    from autotrade.data.ricequant.service import futures as fut
    from autotrade.data.ricequant.service import options as opt
    from autotrade.data.ricequant.base import FetchMode, FetchStatus
    from autotrade.data.ricequant.service import common as common

    fut_price_service = fut.FuturePriceService()
    opt_greek_service = opt.OptionGreeksService()
    opt_price_service = opt.OptionPriceService()
    opt_basic_info_service = opt.OptionInstrumentService()
    underlying_price_service = common.PriceService()

    start_date = '2026-04-01'
    end_date = '2026-04-09'

    opt_price_data = opt_price_service.get(start_date=start_date, end_date=end_date,
                                           mode=FetchMode.DB_ONLY).data
    opt_price_col = ['order_book_id', 'date', 'close']
    opt_price_data = opt_price_data[opt_price_col]

    opt_basic_info = opt_basic_info_service.get().data
    opt_basic_col = ['order_book_id', 'maturity_date', 'strike_price', 'option_type','underlying_order_book_id','underlying_symbol']
    opt_basic_info = opt_basic_info.loc[
        opt_basic_info['order_book_id'].isin(opt_price_data['order_book_id']), opt_basic_col]

    option_panel = pd.merge(left=opt_price_data, right=opt_basic_info, on=['order_book_id'], how='left')
    option_panel['maturity_date'] = pd.to_datetime(option_panel['maturity_date'])
    option_panel['date'] = pd.to_datetime(option_panel['date'])
    option_panel['T_days'] = (option_panel['maturity_date'] - option_panel['date']).dt.days
    option_panel['r'] = 0.035
    option_panel.columns = ['order_book_id', 'date', 'price', 'maturity_date', 'K','flag', 'underlying_order_book_id',
                            'underlying_symbol', 'T_days','r']

    option_panel_copy = option_panel[option_panel['underlying_symbol'] == 'ZN']
    ivx = cal_ivx(option_panel_copy)
