import warnings

import rqdatac
from autotrade.data.ricequant.service import common
from autotrade.data.ricequant.service import options as opt
from autotrade.data.ricequant.service import futures as fut
from autotrade.data.ricequant.service import index as ind
from autotrade.data.ricequant.service import etf as etf
from autotrade.data.ricequant.service import cn_stock as stock
from autotrade.data.ricequant.base import FetchStatus,FetchMode
import pandas as pd
from datetime import datetime, timedelta


def fut_healthy_check(start_date, end_date, data_service: fut.BaseRQService, include_contiunes=True):
    basic_info_ser = fut.FutureInstrumentService()
    basic_info_data = basic_info_ser.get(mode=FetchMode.SOURCE_ONLY)
    basic_info = basic_info_data.data
    if basic_info_data.status == FetchStatus.FAILED or basic_info.empty:
        raise Exception('合约信息查询出错')
    basic_info['maturity_date'] = pd.to_datetime(basic_info['maturity_date'], errors='coerce')
    basic_info['listed_date'] = pd.to_datetime(basic_info['listed_date'], errors='coerce')

    trade_dates_ser = common.TradingDatesService()
    trade_dates_data = trade_dates_ser.get(start_date=start_date, end_date=end_date)
    trade_dates = trade_dates_data.data

    # 处理主力合约
    if include_contiunes:
        underlying_symbol_list = basic_info['underlying_symbol'].unique()
        continue_prefix = ['88', '888', '889', '88A2', '99']
        continue_contract = [f'{underlying_symbol}{prefix}' for underlying_symbol in underlying_symbol_list for prefix
                             in continue_prefix]

        # 找到每个标的最早的list_date
        continue_contract_list_date = basic_info.groupby('underlying_symbol')['listed_date'].min().reset_index()
        # 找到每个标的最晚的时间
        continue_contract_maturity_date = basic_info.groupby('underlying_symbol')['maturity_date'].max().reset_index()

        continue_contract_info = basic_info[basic_info['order_book_id'].isin(continue_contract)].copy()

        continue_contract_info['maturity_date'] = continue_contract_info['underlying_symbol'].map(
            continue_contract_maturity_date.set_index('underlying_symbol')['maturity_date']
        )

        continue_contract_info['listed_date'] = continue_contract_info['underlying_symbol'].map(
            continue_contract_list_date.set_index('underlying_symbol')['listed_date']
        )

        cc_map = continue_contract_info.set_index('order_book_id')[['maturity_date', 'listed_date']]

        mask = basic_info['order_book_id'].isin(cc_map.index)

        basic_info.loc[mask, 'maturity_date'] = basic_info.loc[mask, 'order_book_id'].map(cc_map['maturity_date'])
        basic_info.loc[mask, 'listed_date'] = basic_info.loc[mask, 'order_book_id'].map(cc_map['listed_date'])

    if trade_dates_data.status == FetchStatus.FAILED or trade_dates.empty:
        raise Exception('交易查询出错')

    trade_dates = pd.to_datetime(trade_dates['trading_date'])

    missing_list = {}
    for trade_date in trade_dates:
        # 理论应该存在的合约
        # 期货还需要加上主力、连续合约
        ins_should_exist = basic_info[
            ((basic_info['maturity_date'] >= trade_date) & (
                    basic_info['listed_date'] <= trade_date))]['order_book_id'].astype(str).tolist()

        exist_data = data_service.get(
            start_date=trade_date.strftime('%Y-%m-%d'), end_date=trade_date.strftime('%Y-%m-%d'),
            mode=FetchMode.DB_ONLY, frequency='1d')

        if exist_data.status == FetchStatus.FAILED:
            raise Exception('待检查数据查询失败')
        if exist_data.data.empty:
            missing_list[trade_date] = set(ins_should_exist)
            continue

        exist_ids = exist_data.data['order_book_id'].astype(str)

        missing_ids = set(ins_should_exist) - set(exist_ids)
        if len(missing_ids) > 0:
            missing_list[trade_date] = missing_ids

    return missing_list


def opt_healthy_check(start_date, end_date, data_service: opt.BaseRQService):
    basic_info_ser = opt.OptionInstrumentService()
    basic_info_data = basic_info_ser.get(mode=FetchMode.SOURCE_ONLY)
    basic_info = basic_info_data.data
    if basic_info_data.status == FetchStatus.FAILED or basic_info.empty:
        raise Exception('合约信息查询出错')
    basic_info['maturity_date'] = pd.to_datetime(basic_info['maturity_date'])
    basic_info['listed_date'] = pd.to_datetime(basic_info['listed_date'])

    trade_dates_ser = common.TradingDatesService()
    trade_dates_data = trade_dates_ser.get(start_date=start_date, end_date=end_date)
    trade_dates = trade_dates_data.data
    if trade_dates_data.status == FetchStatus.FAILED or trade_dates.empty:
        raise Exception('交易查询出错')

    trade_dates = pd.to_datetime(trade_dates['trading_date'])

    missing_list = {}
    for trade_date in trade_dates:
        # 理论应该存在的合约
        ins_should_exist = basic_info[
            (basic_info['maturity_date'] >= trade_date) & (
                    basic_info['listed_date'] <= trade_date)]['order_book_id'].astype(str).tolist()

        exist_data = data_service.get(
            start_date=trade_date.strftime('%Y-%m-%d'), end_date=trade_date.strftime('%Y-%m-%d'),
            mode=FetchMode.DB_ONLY, frequency='1d')

        if exist_data.status == FetchStatus.FAILED:
            raise Exception('待检查数据查询失败')
        if exist_data.data.empty:
            missing_list[trade_date] = set(ins_should_exist)
            continue

        exist_ids = exist_data.data['order_book_id'].astype(str)

        missing_ids = set(ins_should_exist) - set(exist_ids)
        if len(missing_ids) > 0:
            missing_list[trade_date] = missing_ids

    return missing_list


def index_healthy_check(start_date, end_date, data_service: ind.BaseRQService):
    basic_info_ser = ind.IndexInstrumentService()
    basic_info_data = basic_info_ser.get(mode=FetchMode.SOURCE_ONLY)
    basic_info = basic_info_data.data
    if basic_info_data.status == FetchStatus.FAILED or basic_info.empty:
        raise Exception('合约信息查询出错')
    basic_info['listed_date'] = pd.to_datetime(basic_info['listed_date'])
    basic_info['de_listed_date'] = basic_info['de_listed_date'].fillna(pd.to_datetime(end_date))
    basic_info['de_listed_date'] = pd.to_datetime(basic_info['de_listed_date'], errors='coerce')

    trade_dates_ser = common.TradingDatesService()
    trade_dates_data = trade_dates_ser.get(start_date=start_date, end_date=end_date)
    trade_dates = trade_dates_data.data
    if trade_dates_data.status == FetchStatus.FAILED or trade_dates.empty:
        raise Exception('交易查询出错')

    trade_dates = pd.to_datetime(trade_dates['trading_date'])

    missing_list = {}
    for trade_date in trade_dates:

        # 理论应该存在的合约
        ins_should_exist = basic_info[
            (basic_info['de_listed_date'] >= trade_date) & (
                    basic_info['listed_date'] <= trade_date)]['order_book_id'].astype(str).tolist()

        exist_data = data_service.get(
            start_date=trade_date.strftime('%Y-%m-%d'), end_date=trade_date.strftime('%Y-%m-%d'),
            mode=FetchMode.DB_ONLY, frequency='1d')

        if exist_data.status == FetchStatus.FAILED:
            raise Exception('待检查数据查询失败')
        if exist_data.data.empty:
            missing_list[trade_date] = set(ins_should_exist)
            continue

        exist_ids = exist_data.data['order_book_id'].astype(str)
        missing_ids = set(ins_should_exist) - set(exist_ids)
        if len(missing_ids) > 0:
            missing_list[trade_date] = missing_ids

    return missing_list


def etf_healthy_check(start_date, end_date, data_service: ind.BaseRQService, adjust_type: str | None = None):
    if adjust_type is None:
        warnings.warn('ETF没有指定adjust_type')
    basic_info_ser = etf.ETFInstrumentService()
    basic_info_data = basic_info_ser.get(mode=FetchMode.SOURCE_ONLY)
    basic_info = basic_info_data.data
    if basic_info_data.status == FetchStatus.FAILED or basic_info.empty:
        raise Exception('合约信息查询出错')
    basic_info['listed_date'] = pd.to_datetime(basic_info['listed_date'])
    basic_info['de_listed_date'] = basic_info['de_listed_date'].fillna(pd.to_datetime(end_date))
    basic_info['de_listed_date'] = pd.to_datetime(basic_info['de_listed_date'], errors='coerce')

    trade_dates_ser = common.TradingDatesService()
    trade_dates_data = trade_dates_ser.get(start_date=start_date, end_date=end_date)
    trade_dates = trade_dates_data.data
    if trade_dates_data.status == FetchStatus.FAILED or trade_dates.empty:
        raise Exception('交易查询出错')

    trade_dates = pd.to_datetime(trade_dates['trading_date'])

    missing_list = {}
    for trade_date in trade_dates:
        # 理论应该存在的合约
        ins_should_exist = basic_info[
            (basic_info['de_listed_date'] >= trade_date) & (
                    basic_info['listed_date'] <= trade_date)]['order_book_id'].astype(str).tolist()
        if adjust_type is None:
            exist_data = data_service.get(
                start_date=trade_date.strftime('%Y-%m-%d'), end_date=trade_date.strftime('%Y-%m-%d'),
                mode=FetchMode.DB_ONLY, frequency='1d')
        else:
            exist_data = data_service.get(
                start_date=trade_date.strftime('%Y-%m-%d'), end_date=trade_date.strftime('%Y-%m-%d'),
                mode=FetchMode.DB_ONLY, frequency='1d',adjust_type=adjust_type)

        if exist_data.status == FetchStatus.FAILED:
            raise Exception('待检查数据查询失败')
        if exist_data.data.empty:
            missing_list[trade_date] = set(ins_should_exist)
            continue

        exist_ids = exist_data.data['order_book_id'].astype(str)

        missing_ids = set(ins_should_exist) - set(exist_ids)
        if len(missing_ids) > 0:
            missing_list[trade_date] = missing_ids
    return missing_list


def expand_trading_periods_to_time_rows(periods_df: pd.DataFrame, frequency: str = '1m') -> pd.DataFrame:
    if periods_df is None or periods_df.empty:
        return pd.DataFrame(columns=["date", "time", "order_book_id"])

    if not isinstance(periods_df.index, pd.MultiIndex):
        raise ValueError("periods_df index must be MultiIndex(order_book_id, date)")

    if not frequency.endswith('m'):
        raise ValueError('expand_trading_periods_to_time_rows currently only supports minute frequencies')
    step_minutes = int(frequency[:-1])
    if step_minutes <= 0:
        raise ValueError('frequency step must be positive')

    rows = []
    for (order_book_id, trade_date), row in periods_df.iterrows():
        trading_hours = row["trading_hours"]
        if pd.isna(trading_hours) or not str(trading_hours).strip():
            continue

        for segment in str(trading_hours).split(","):
            start_str, end_str = segment.strip().split("-")
            start_dt = datetime.strptime(start_str, "%H:%M")
            end_dt = datetime.strptime(end_str, "%H:%M")
            start_total_minutes = start_dt.hour * 60 + start_dt.minute
            end_total_minutes = end_dt.hour * 60 + end_dt.minute

            if end_total_minutes < start_total_minutes:
                end_total_minutes += 24 * 60

            first_bar_end = start_total_minutes + step_minutes - 1
            if first_bar_end > end_total_minutes:
                continue

            segment_minute_offsets = list(range(first_bar_end, end_total_minutes + 1, step_minutes))
            if segment_minute_offsets[-1] != end_total_minutes:
                segment_minute_offsets.append(end_total_minutes)

            for minute_offset in segment_minute_offsets:
                time_dt = datetime.strptime("00:00", "%H:%M") + timedelta(minutes=minute_offset % (24 * 60))
                rows.append(
                    {
                        "date": pd.to_datetime(trade_date).date(),
                        "time": time_dt.strftime("%H:%M"),
                        "order_book_id": str(order_book_id),
                    }
                )

    return pd.DataFrame(rows, columns=["date", "time", "order_book_id"])


def expand_future_60m_trading_periods_to_time_rows(periods_df: pd.DataFrame, frequency: str = '60m') -> pd.DataFrame:
    if frequency != '60m':
        return expand_trading_periods_to_time_rows(periods_df, frequency=frequency)

    if periods_df is None or periods_df.empty:
        return pd.DataFrame(columns=["date", "time", "order_book_id"])

    if not isinstance(periods_df.index, pd.MultiIndex):
        raise ValueError("periods_df index must be MultiIndex(order_book_id, date)")

    rows = []
    for (order_book_id, trade_date), row in periods_df.iterrows():
        trading_hours = row["trading_hours"]
        if pd.isna(trading_hours) or not str(trading_hours).strip():
            continue

        for segment in str(trading_hours).split(","):
            start_str, end_str = segment.strip().split("-")
            start_dt = datetime.strptime(start_str, "%H:%M")
            end_dt = datetime.strptime(end_str, "%H:%M")
            start_total_minutes = start_dt.hour * 60 + start_dt.minute
            end_total_minutes = end_dt.hour * 60 + end_dt.minute

            if end_total_minutes < start_total_minutes:
                end_total_minutes += 24 * 60

            segment_minute_offsets = []
            first_hour_boundary = ((start_total_minutes // 60) + 1) * 60
            segment_minute_offsets.extend(range(first_hour_boundary, end_total_minutes + 1, 60))

            if end_total_minutes % 60 == 30 and end_total_minutes not in segment_minute_offsets:
                segment_minute_offsets.append(end_total_minutes)

            for minute_offset in segment_minute_offsets:
                time_dt = datetime.strptime("00:00", "%H:%M") + timedelta(minutes=minute_offset % (24 * 60))
                rows.append(
                    {
                        "date": pd.to_datetime(trade_date).date(),
                        "time": time_dt.strftime("%H:%M"),
                        "order_book_id": str(order_book_id),
                    }
                )

    return pd.DataFrame(rows, columns=["date", "time", "order_book_id"])


def _minute_healthy_check(
    start_date,
    end_date,
    data_service,
    instrument_service_cls,
    use_trading_date: bool,
    order_book_ids: list[str] | None = None,
    frequency: str = '1m',
    batch_size: int = 200,
    expand_fn=expand_trading_periods_to_time_rows,
    **service_filters,
):
    if order_book_ids is None:
        basic_info_ser = instrument_service_cls()
        basic_info_data = basic_info_ser.get(mode=FetchMode.SOURCE_ONLY)
        basic_info = basic_info_data.data
        if basic_info_data.status == FetchStatus.FAILED or basic_info.empty:
            raise Exception('合约信息查询出错')
        order_book_ids = basic_info['order_book_id'].astype(str).tolist()
    else:
        order_book_ids = [str(order_book_id) for order_book_id in order_book_ids]

    missing_list = {}
    for batch_start in range(0, len(order_book_ids), batch_size):
        batch_ids = order_book_ids[batch_start: batch_start + batch_size]
        periods = rqdatac.get_trading_periods(
            order_book_ids=batch_ids,
            frequency='1m',
            start_date=start_date,
            end_date=end_date,
        )

        expected_rows = expand_fn(periods, frequency=frequency)
        if expected_rows.empty:
            continue

        exist_data = data_service.get(
            order_book_ids=batch_ids,
            start_date=start_date,
            end_date=end_date,
            mode=FetchMode.DB_ONLY,
            frequency=frequency,
            **service_filters,
        )
        if exist_data.status == FetchStatus.FAILED:
            raise Exception('待检查数据查询失败')

        db_df = exist_data.data.copy()
        if db_df.empty:
            for (trade_date, order_book_id), _part in expected_rows.groupby(["date", "order_book_id"]):
                missing_list.setdefault(pd.Timestamp(trade_date), set()).add(order_book_id)
            continue

        db_df['datetime'] = pd.to_datetime(db_df['datetime'])
        db_df['time'] = db_df['datetime'].dt.strftime('%H:%M')
        if use_trading_date:
            db_df['group_date'] = pd.to_datetime(db_df['trading_date']).dt.date
        else:
            db_df['group_date'] = db_df['datetime'].dt.date

        actual_time_map = (
            db_df.groupby(['group_date', 'order_book_id'])['time']
            .agg(lambda s: set(s.astype(str)))
            .to_dict()
        )

        for (trade_date, order_book_id), part in expected_rows.groupby(["date", "order_book_id"]):
            expected_times = set(part['time'].astype(str))
            actual_times = actual_time_map.get((trade_date, order_book_id), set())
            if expected_times - actual_times:
                missing_list.setdefault(pd.Timestamp(trade_date), set()).add(order_book_id)

    return missing_list


def fut_1m_healthy_check(
    start_date,
    end_date,
    data_service: fut.BaseRQService,
    order_book_ids: list[str] | None = None,
    frequency: str = '1m',
    batch_size: int = 200,
):
    return _minute_healthy_check(
        start_date=start_date,
        end_date=end_date,
        data_service=data_service,
        instrument_service_cls=fut.FutureInstrumentService,
        use_trading_date=True,
        order_book_ids=order_book_ids,
        frequency=frequency,
        batch_size=batch_size,
        expand_fn=expand_future_60m_trading_periods_to_time_rows,
    )


def opt_1m_healthy_check(
    start_date,
    end_date,
    data_service: opt.BaseRQService,
    order_book_ids: list[str] | None = None,
    frequency: str = '1m',
    batch_size: int = 200,
):
    return _minute_healthy_check(
        start_date=start_date,
        end_date=end_date,
        data_service=data_service,
        instrument_service_cls=opt.OptionInstrumentService,
        use_trading_date=True,
        order_book_ids=order_book_ids,
        frequency=frequency,
        batch_size=batch_size,
    )


def stock_1m_healthy_check(
    start_date,
    end_date,
    data_service: stock.BaseRQService,
    order_book_ids: list[str] | None = None,
    frequency: str = '1m',
    batch_size: int = 200,
    adjust_type: str | None = None,
):
    service_filters = {}
    if adjust_type is not None:
        service_filters['adjust_type'] = adjust_type

    return _minute_healthy_check(
        start_date=start_date,
        end_date=end_date,
        data_service=data_service,
        instrument_service_cls=stock.CNStockInstrumentService,
        use_trading_date=False,
        order_book_ids=order_book_ids,
        frequency=frequency,
        batch_size=batch_size,
        **service_filters,
    )


def index_1m_healthy_check(
    start_date,
    end_date,
    data_service: ind.BaseRQService,
    order_book_ids: list[str] | None = None,
    frequency: str = '1m',
    batch_size: int = 200,
):
    return _minute_healthy_check(
        start_date=start_date,
        end_date=end_date,
        data_service=data_service,
        instrument_service_cls=ind.IndexInstrumentService,
        use_trading_date=False,
        order_book_ids=order_book_ids,
        frequency=frequency,
        batch_size=batch_size,
    )


def etf_1m_healthy_check(
    start_date,
    end_date,
    data_service: etf.BaseRQService,
    order_book_ids: list[str] | None = None,
    frequency: str = '1m',
    batch_size: int = 200,
    adjust_type: str | None = None,
):
    service_filters = {}
    if adjust_type is not None:
        service_filters['adjust_type'] = adjust_type

    return _minute_healthy_check(
        start_date=start_date,
        end_date=end_date,
        data_service=data_service,
        instrument_service_cls=etf.ETFInstrumentService,
        use_trading_date=False,
        order_book_ids=order_book_ids,
        frequency=frequency,
        batch_size=batch_size,
        **service_filters,
    )
