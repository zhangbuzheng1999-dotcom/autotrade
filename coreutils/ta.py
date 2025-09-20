import pandas as pd
import numpy as np
import warnings
import talib as ta

# ====== 输入标准化 ======
def _to_series(data, name="value"):
    """确保输入是 pandas.Series"""
    if isinstance(data, pd.Series):
        return data.astype(float)
    elif isinstance(data, (list, tuple, np.ndarray)):
        return pd.Series(data, name=name, dtype=float)
    elif isinstance(data, pd.DataFrame):
        warnings.warn("输入是 DataFrame，默认取第一列。建议直接传 Series。", UserWarning)
        return data.iloc[:, 0].astype(float)
    else:
        raise TypeError("输入必须是 pandas.Series、list、tuple、ndarray 或 DataFrame")


# ====== EMA ======
def ema(data, span):
    series = _to_series(data)
    result = series.ewm(span=span, adjust=False).mean()
    # 和 TA-Lib 对齐：前 span-1 个位置设为 NaN
    result[:span-1] = np.nan
    return result


# ====== MACD ======
def macd(data, fast=12, slow=26, signal=9):
    series = _to_series(data)

    ema_fast = ema(series, fast)
    ema_slow = ema(series, slow)

    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()

    # 和 TA-Lib 对齐
    dif[:slow-1] = np.nan
    dea[:slow-1] = np.nan
    hist = (dif - dea) * 2  # 注意：TA-Lib 的 MACD 柱子乘以 2

    return pd.DataFrame({
        "DIF": dif,
        "DEA": dea,
        "MACD": hist
    })


# ====== Bollinger Bands ======
def bbands(data, length=20, std=2):
    '''
    series = _to_series(data)

    mid = series.rolling(length).mean()
    stddev = series.rolling(length).std(ddof=0)  # talib 用总体标准差 (ddof=0)

    upper = mid + stddev * std
    lower = mid - stddev * std

    return pd.DataFrame({
        "BBL": lower,
        "BBM": mid,
        "BBU": upper
    })
    '''
    N = length
    alpha = 2
    bbands_mid = np.repeat(np.nan, len(data))
    sigma = np.repeat(np.nan, len(data))

    for i in range(N - 1, len(data)):
        spec_data = data[i - N + 1:i + 1]
        bbands_mid[i] = round(np.mean(spec_data), 2)
        # ddof=1表示计算样本标准差，即除N-1
        sigma[i] = round(np.std(spec_data, ddof=1), 2)

    bbands_up = bbands_mid + alpha * sigma
    bbands_down = bbands_mid - alpha * sigma

    res = pd.DataFrame(
        {'BBU': bbands_up, 'BBL': bbands_down, 'BBM': bbands_mid, 'sigma': sigma})

    return res


# ====== 测试对比 ======
if __name__ == "__main__":
    close = pd.Series(np.arange(1, 101), dtype=float)

    # --- MACD 对比 ---
    my_macd = macd(close)
    ta_macd = pd.DataFrame({
        "DIF": ta.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)[0],
        "DEA": ta.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)[1],
        "MACD": ta.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)[2]
    })

    print("=== MACD 对比 (最后5行) ===")
    print(pd.concat([my_macd.tail(), ta_macd.tail()], axis=1))

    # --- BBANDS 对比 ---
    my_bbands = bbands(close)
    ta_bbands = pd.DataFrame({
        "BBU": ta.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)[0],
        "BBM": ta.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)[1],
        "BBL": ta.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)[2],
    })

    print("\n=== BBANDS 对比 (最后5行) ===")
    print(pd.concat([my_bbands.tail(), ta_bbands.tail()], axis=1))
