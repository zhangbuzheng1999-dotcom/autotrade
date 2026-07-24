# autotrade/service/option_basic_service.py
import pandas as pd
from autotrade.data.tushare.service.base import BaseService, FetchMode
from autotrade.data.tushare.datasource.data_source_tushare import (
    TushareOptBasicSource, TushareOptDailySource,
    TushareEtfBasicSource, TushareEtfFundDaily, TushareEtfFundAdj,
    TushareFutBasicSource,TushareFutDaily,TushareIndexBasicSource,TushareIndexDaily)
from autotrade.data.tushare.repository.repo_tushare import (
    OptionBasicRepository, OptionDailyRepository,
    EtfBasicRepository, EtfFundDailyRepository, EtfFundAdjRepository,
    FutBasicRepository, FutDailyRepository,IndexBasicRepository,IndexDailyRepository)


class OptionDailyService(BaseService):

    def __init__(self):
        self.source = TushareOptDailySource()
        self.repo = OptionDailyRepository()


class OptionBasicService(BaseService):

    def __init__(self):
        self.source = TushareOptBasicSource()
        self.repo = OptionBasicRepository()


class EtfBasicService(BaseService):

    def __init__(self):
        self.source = TushareEtfBasicSource()
        self.repo = EtfBasicRepository()


class EtfFundDailyService(BaseService):

    def __init__(self):
        self.source = TushareEtfFundDaily()
        self.repo = EtfFundDailyRepository()


class EtfFundAdjService(BaseService):

    def __init__(self):
        self.source = TushareEtfFundAdj()
        self.repo = EtfFundAdjRepository()

class FutBasicService(BaseService):

    def __init__(self):
        self.source = TushareFutBasicSource()
        self.repo = FutBasicRepository()


class FutDailyService(BaseService):

    def __init__(self):
        self.source = TushareFutDaily()
        self.repo = FutDailyRepository()

class IndexBasicService(BaseService):

    def __init__(self):
        self.source = TushareIndexBasicSource()
        self.repo = IndexBasicRepository()


class IndexDailyService(BaseService):

    def __init__(self):
        self.source = TushareIndexDaily()
        self.repo = IndexDailyRepository()

if __name__ == "__main__":
    from autotrade.coreutils.config import load_env

    load_env("d:/.env")

    basic_service = IndexBasicService()
    daily_service = IndexDailyService()
    basic_info = basic_service.get(mode=FetchMode.DB_THEN_SOURCE, persist=True).data
    price_df = daily_service.get(mode=FetchMode.DB_THEN_SOURCE, persist=True,code='000016.SH',start_date='20260201',end_date='20260401').data
