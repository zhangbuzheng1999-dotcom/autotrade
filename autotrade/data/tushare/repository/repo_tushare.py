# autotrade/repository/option_basic_repo.py
import pandas as pd
from autotrade.data.tushare.repository.base import BaseRepository


class OptionBasicRepository(BaseRepository):
    DATABASE = "option_data"
    TABLE = "option_basic"

    TS_CODE_FIELD = "ts_code"
    EXCHANGE_FIELD = "exchange"
    DATE_FIELD = "list_date"

    def query(
            self,
            *,
            ts_code=None,
            exchange=None,
            date=None,
            start_date=None,
            end_date=None,
    ):
        if start_date or end_date:
            raise ValueError(
                "option_basic does not support date range query; "
                "use `date` (list_date) instead"
            )

        return super().query(
            ts_code=ts_code,
            exchange=exchange,
            date=date,
            start_date=start_date,
            end_date=end_date,
        )


class OptionDailyRepository(BaseRepository):
    DATABASE = "option_data"
    TABLE = "option_daily"

    TS_CODE_FIELD = "ts_code"
    EXCHANGE_FIELD = "exchange"
    DATE_FIELD = "trade_date"


class EtfBasicRepository(BaseRepository):
    DATABASE = "etf_data"
    TABLE = "etf_basic"

    TS_CODE_FIELD = "ts_code"
    EXCHANGE_FIELD = "exchange"
    DATE_FIELD = "list_date"

    def query(
            self,
            *,
            ts_code=None,
            exchange=None,
            date=None,
            start_date=None,
            end_date=None,
    ):
        if start_date or end_date:
            raise ValueError(
                "etf_basic does not support date range query; "
                "use `date` (list_date) instead"
            )

        return super().query(
            ts_code=ts_code,
            exchange=exchange,
            date=date,
            start_date=start_date,
            end_date=end_date,
        )


class EtfFundDailyRepository(BaseRepository):
    DATABASE = "etf_data"
    TABLE = "fund_daily"

    TS_CODE_FIELD = "ts_code"
    DATE_FIELD = "trade_date"

    def query(
            self,
            *,
            ts_code=None,
            exchange=None,
            date=None,
            start_date=None,
            end_date=None,
    ):
        if exchange:
            raise ValueError(
                "EtfFundDailyRepository does not support exchange query; "

            )

        return super().query(
            ts_code=ts_code,
            date=date,
            start_date=start_date,
            end_date=end_date,
        )


class EtfFundAdjRepository(BaseRepository):
    DATABASE = "etf_data"
    TABLE = "fund_adj"

    TS_CODE_FIELD = "ts_code"
    DATE_FIELD = "trade_date"

    def query(
            self,
            *,
            ts_code=None,
            exchange=None,
            date=None,
            start_date=None,
            end_date=None,
    ):
        if exchange:
            raise ValueError(
                "EtfFundAdjRepository does not support exchange query; "

            )

        return super().query(
            ts_code=ts_code,
            date=date,
            start_date=start_date,
            end_date=end_date,
        )


if __name__ == "__main__":
    from autotrade.coreutils.config import load_env

    load_env("d:/.env")
    from autotrade.data.tushare.init_tushare_db import create_etf_data

    create_etf_data()

    etf = EtfFundDailyRepository()
    etf.query()
