import warnings

from autotrade.data.ricequant.service import common
from autotrade.data.ricequant.service import options as opt
from autotrade.data.ricequant.service import futures as fut
from autotrade.data.ricequant.service import index as ind
from autotrade.data.ricequant.service import etf as etf
from autotrade.data.ricequant.base import FetchStatus,FetchMode
import pandas as pd


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
