from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any
import traceback

import pandas as pd

from autotrade.coreutils.constant import FetchMode, FetchStatus


# =========================
# 结果结构
# =========================
@dataclass
class MissingCheckResult:
    missing_info: list[dict[str, Any]]
    failed_check_list: list[str]
    empty_price_list: list[str]
    stats: dict[str, Any]


# =========================
# 工具函数
# =========================
def chunk_list(items: list[Any], chunk_size: int) -> list[list[Any]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def normalize_trade_calendar(trade_date_calendar) -> pd.DatetimeIndex:
    cal = pd.to_datetime(trade_date_calendar, errors="coerce")
    cal = pd.Series(cal).dropna().drop_duplicates().sort_values()
    return pd.DatetimeIndex(cal)


def build_need_trade_dates(
    trade_calendar: pd.DatetimeIndex,
    list_date: pd.Timestamp,
    delist_date: pd.Timestamp,
) -> set[pd.Timestamp]:
    mask = (trade_calendar >= list_date) & (trade_calendar <= delist_date)
    return set(pd.DatetimeIndex(trade_calendar[mask]))


def _safe_sorted_timestamps(values: set[pd.Timestamp]) -> list[pd.Timestamp]:
    return sorted(pd.to_datetime(list(values)))


def create_price_service(price_service_type: str):
    """
    Windows 多进程不要传函数对象，改为传字符串，在子进程里创建 service。
    """
    from autotrade.data.tushare.service.data_service_tushare import (
        FutDailyService,
        OptionDailyService,
    )

    if price_service_type == "future_daily":
        return FutDailyService()

    if price_service_type == "option_daily":
        return OptionDailyService()

    raise ValueError(f"unsupported price_service_type: {price_service_type}")


def get_trade_date_calendar(price_service, bench_code_list, mode):
    price_result = price_service.get(mode=mode, code_list=bench_code_list)

    if getattr(price_result, "status", None) != FetchStatus.SUCCESS:
        raise Exception(getattr(price_result, "error", None) or "get_trade_date_calendar failed")

    price_df = getattr(price_result, "data", None)
    if price_df is None or price_df.empty:
        return pd.DatetimeIndex([])

    trade_date_calendar = (
        pd.to_datetime(price_df["trade_date"], errors="coerce")
        .dropna()
        .drop_duplicates()
        .sort_values()
    )
    return pd.DatetimeIndex(trade_date_calendar)


# =========================
# 子进程 worker
# =========================
def _check_derivative_missing_chunk_worker(
    chunk_basic_info: pd.DataFrame,
    trade_date_calendar,
    price_service_type: str,
    code_field: str,
    list_date_field: str,
    delist_date_field: str,
    trade_date_field: str,
    extra_price_filters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    单个 chunk 的完整检查逻辑：
    - 子进程内创建 price_service
    - 批量拉取 chunk 内所有合约价格
    - 逐合约计算缺失日期
    - 返回 chunk 结果
    """
    extra_price_filters = extra_price_filters or {}

    chunk_missing_info: list[dict[str, Any]] = []
    chunk_failed_check_list: list[str] = []
    chunk_empty_price_list: list[str] = []

    try:
        trade_calendar = normalize_trade_calendar(trade_date_calendar)

        if chunk_basic_info is None or chunk_basic_info.empty:
            return {
                "missing_info": [],
                "failed_check_list": [],
                "empty_price_list": [],
                "checked_count": 0,
                "error": None,
            }

        local_info = chunk_basic_info.copy()
        local_info[list_date_field] = pd.to_datetime(local_info[list_date_field], errors="coerce")
        local_info[delist_date_field] = pd.to_datetime(local_info[delist_date_field], errors="coerce")

        code_list = local_info[code_field].dropna().drop_duplicates().tolist()
        if not code_list:
            return {
                "missing_info": [],
                "failed_check_list": [],
                "empty_price_list": [],
                "checked_count": 0,
                "error": None,
            }

        chunk_start = local_info[list_date_field].min()
        chunk_end = local_info[delist_date_field].max()

        price_service = create_price_service(price_service_type)

        price_result = price_service.get(
            mode=FetchMode.DB_ONLY,
            code_list=code_list,
            start_date=chunk_start,
            end_date=chunk_end,
            **extra_price_filters,
        )

        if getattr(price_result, "status", None) != FetchStatus.SUCCESS:
            return {
                "missing_info": [],
                "failed_check_list": code_list,
                "empty_price_list": [],
                "checked_count": len(code_list),
                "error": str(getattr(price_result, "error", None)),
            }

        price_df = getattr(price_result, "data", None)
        if price_df is None:
            price_df = pd.DataFrame()

        if not isinstance(price_df, pd.DataFrame):
            price_df = pd.DataFrame(price_df)

        if price_df.empty:
            return {
                "missing_info": [{code: "ALL"} for code in code_list],
                "failed_check_list": [],
                "empty_price_list": code_list,
                "checked_count": len(code_list),
                "error": None,
            }

        if code_field not in price_df.columns:
            return {
                "missing_info": [],
                "failed_check_list": code_list,
                "empty_price_list": [],
                "checked_count": len(code_list),
                "error": f"price_df missing code field: {code_field}",
            }

        if trade_date_field not in price_df.columns:
            return {
                "missing_info": [],
                "failed_check_list": code_list,
                "empty_price_list": [],
                "checked_count": len(code_list),
                "error": f"price_df missing trade date field: {trade_date_field}",
            }

        price_df = price_df.copy()
        price_df[trade_date_field] = pd.to_datetime(price_df[trade_date_field], errors="coerce")
        price_df = price_df.dropna(subset=[trade_date_field])

        grouped_price_dates: dict[str, set[pd.Timestamp]] = {}
        for instrument, group in price_df.groupby(code_field):
            grouped_price_dates[instrument] = set(
                pd.DatetimeIndex(group[trade_date_field].dropna().unique())
            )

        for _, row in local_info.iterrows():
            instrument = row[code_field]
            list_date = row[list_date_field]
            delist_date = row[delist_date_field]

            if pd.isna(instrument) or pd.isna(list_date) or pd.isna(delist_date):
                chunk_failed_check_list.append(instrument)
                continue

            need_trade_dates = build_need_trade_dates(
                trade_calendar=trade_calendar,
                list_date=list_date,
                delist_date=delist_date,
            )

            exist_trade_dates = grouped_price_dates.get(instrument, set())

            if not need_trade_dates:
                continue

            if not exist_trade_dates:
                chunk_missing_info.append({instrument: "ALL"})
                chunk_empty_price_list.append(instrument)
                continue

            missing_dates = need_trade_dates - exist_trade_dates
            if missing_dates:
                chunk_missing_info.append({
                    instrument: _safe_sorted_timestamps(missing_dates)
                })

        return {
            "missing_info": chunk_missing_info,
            "failed_check_list": chunk_failed_check_list,
            "empty_price_list": chunk_empty_price_list,
            "checked_count": len(code_list),
            "error": None,
        }

    except Exception:
        fallback_codes = []
        try:
            if chunk_basic_info is not None and not chunk_basic_info.empty and code_field in chunk_basic_info.columns:
                fallback_codes = chunk_basic_info[code_field].dropna().drop_duplicates().tolist()
        except Exception:
            pass

        return {
            "missing_info": [],
            "failed_check_list": fallback_codes,
            "empty_price_list": [],
            "checked_count": len(fallback_codes),
            "error": traceback.format_exc(),
        }


# =========================
# 主入口：全流程多进程检查
# =========================
def check_derivative_missing_info_parallel(
    basic_info_service,
    price_service_type: str,
    trade_date_calendar,
    *,
    chunk_size: int = 200,
    max_workers: int = 4,
    code_field: str = "ts_code",
    list_date_field: str = "list_date",
    delist_date_field: str = "delist_date",
    trade_date_field: str = "trade_date",
    extra_price_filters: dict[str, Any] | None = None,
) -> MissingCheckResult:
    extra_price_filters = extra_price_filters or {}

    basic_info_result = basic_info_service.get(mode=FetchMode.DB_ONLY)

    if getattr(basic_info_result, "status", None) != FetchStatus.SUCCESS:
        raise Exception(getattr(basic_info_result, "error", None) or "basic_info_service.get failed")

    instrument_info = getattr(basic_info_result, "data", None)
    if instrument_info is None:
        instrument_info = pd.DataFrame()

    if not isinstance(instrument_info, pd.DataFrame):
        instrument_info = pd.DataFrame(instrument_info)

    if instrument_info.empty:
        return MissingCheckResult(
            missing_info=[],
            failed_check_list=[],
            empty_price_list=[],
            stats={
                "total_instruments": 0,
                "valid_instruments": 0,
                "invalid_basic_info_count": 0,
                "checked_instruments": 0,
                "missing_instruments": 0,
                "empty_price_instruments": 0,
                "failed_instruments": 0,
                "chunk_count": 0,
                "max_workers": max_workers,
                "chunk_size": chunk_size,
            },
        )

    required_cols = {code_field, list_date_field, delist_date_field}
    missing_cols = required_cols - set(instrument_info.columns)
    if missing_cols:
        raise ValueError(f"basic_info dataframe missing required columns: {sorted(missing_cols)}")

    instrument_info = instrument_info.copy()
    instrument_info[list_date_field] = pd.to_datetime(instrument_info[list_date_field], errors="coerce")
    instrument_info[delist_date_field] = pd.to_datetime(instrument_info[delist_date_field], errors="coerce")

    invalid_mask = (
        instrument_info[code_field].isna()
        | instrument_info[list_date_field].isna()
        | instrument_info[delist_date_field].isna()
    )

    failed_check_list = (
        instrument_info.loc[invalid_mask, code_field]
        .dropna()
        .drop_duplicates()
        .tolist()
    )

    valid_info = instrument_info.loc[~invalid_mask].copy()
    valid_info = valid_info.drop_duplicates(subset=[code_field])

    if valid_info.empty:
        return MissingCheckResult(
            missing_info=[],
            failed_check_list=failed_check_list,
            empty_price_list=[],
            stats={
                "total_instruments": len(instrument_info),
                "valid_instruments": 0,
                "invalid_basic_info_count": len(failed_check_list),
                "checked_instruments": 0,
                "missing_instruments": 0,
                "empty_price_instruments": 0,
                "failed_instruments": len(failed_check_list),
                "chunk_count": 0,
                "max_workers": max_workers,
                "chunk_size": chunk_size,
            },
        )

    all_codes = valid_info[code_field].dropna().drop_duplicates().tolist()
    code_chunks = chunk_list(all_codes, chunk_size)

    missing_info: list[dict[str, Any]] = []
    empty_price_list: list[str] = []
    checked_instruments = 0

    chunk_payloads = []
    for code_chunk in code_chunks:
        chunk_df = valid_info[valid_info[code_field].isin(code_chunk)].copy()
        chunk_payloads.append(chunk_df)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_chunk_idx = {}

        for idx, chunk_df in enumerate(chunk_payloads):
            future = executor.submit(
                _check_derivative_missing_chunk_worker,
                chunk_df,
                trade_date_calendar,
                price_service_type,
                code_field,
                list_date_field,
                delist_date_field,
                trade_date_field,
                extra_price_filters,
            )
            future_to_chunk_idx[future] = idx

        total_chunks = len(chunk_payloads)
        done_chunks = 0

        for future in as_completed(future_to_chunk_idx):
            result = future.result()

            missing_info.extend(result.get("missing_info", []))
            failed_check_list.extend(result.get("failed_check_list", []))
            empty_price_list.extend(result.get("empty_price_list", []))
            checked_instruments += result.get("checked_count", 0)

            done_chunks += 1
            print(
                f"[PROGRESS] {done_chunks}/{total_chunks} "
                f"({done_chunks / total_chunks:.2%}) | "
                f"checked={checked_instruments}"
            )

    failed_check_list = sorted(set([x for x in failed_check_list if pd.notna(x)]))
    empty_price_list = sorted(set([x for x in empty_price_list if pd.notna(x)]))

    return MissingCheckResult(
        missing_info=missing_info,
        failed_check_list=failed_check_list,
        empty_price_list=empty_price_list,
        stats={
            "total_instruments": len(instrument_info),
            "valid_instruments": len(valid_info),
            "invalid_basic_info_count": int(invalid_mask.sum()),
            "checked_instruments": checked_instruments,
            "missing_instruments": len(missing_info),
            "empty_price_instruments": len(empty_price_list),
            "failed_instruments": len(failed_check_list),
            "chunk_count": len(code_chunks),
            "max_workers": max_workers,
            "chunk_size": chunk_size,
        },
    )


if __name__ == "__main__":
    from autotrade.coreutils.config import load_env
    load_env("d:/.env")

    from autotrade.data.tushare.service.data_service_tushare import (
        OptionBasicService,
        FutDailyService,
    )

    basic_info_service = OptionBasicService()
    date_calender_service = FutDailyService()

    trade_date_calender = get_trade_date_calendar(
        price_service=date_calender_service,
        bench_code_list=["AU.SHF"],
        mode=FetchMode.DB_ONLY,
    )

    print("trade calendar size:", len(trade_date_calender))

    result = check_derivative_missing_info_parallel(
        basic_info_service=basic_info_service,
        price_service_type="option_daily",
        trade_date_calendar=trade_date_calender,
        chunk_size=200,
        max_workers=4,
        code_field="ts_code",
        list_date_field="list_date",
        delist_date_field="delist_date",
        trade_date_field="trade_date",
        extra_price_filters={},
    )

    pd.to_pickle({
        "missing_info": result.missing_info,
        "failed_check_list": result.failed_check_list,
        "empty_price_list": result.empty_price_list,
        "stats": result.stats,
    }, "d:/check_opt.pkl")

    print("\n===== STATS =====")
    print(result.stats)

    print("\n===== FAILED SAMPLE =====")
    print(result.failed_check_list[:10])

    print("\n===== EMPTY PRICE SAMPLE =====")
    print(result.empty_price_list[:10])

    print("\n===== MISSING SAMPLE =====")
    print(result.missing_info[:5])
