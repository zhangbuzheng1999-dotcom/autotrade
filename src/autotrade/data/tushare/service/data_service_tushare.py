# autotrade/service/option_basic_service.py
import pandas as pd
from autotrade.data.tushare.service.base import BaseService, FetchMode
from autotrade.data.tushare.datasource.data_source_tushare import (
    TushareOptBasicSource, TushareOptDailySource,
    TushareEtfBasicSource, TushareEtfFundDaily, TushareEtfFundAdj,
    TushareFutBasicSource,TushareFutDaily)
from autotrade.data.tushare.repository.repo_tushare import (
    OptionBasicRepository, OptionDailyRepository,
    EtfBasicRepository, EtfFundDailyRepository, EtfFundAdjRepository,
    FutBasicRepository, FutDailyRepository)


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

if __name__ == "__main__":
    from autotrade.coreutils.config import load_env

    load_env("d:/.env")

    etf_basic = EtfFundAdjService()
    a = etf_basic.get(mode=FetchMode.SOURCE_ONLY, ts_code="159238.SZ", persist=True,exchange='a')

