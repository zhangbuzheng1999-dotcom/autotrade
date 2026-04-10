# autotrade/data/ricequant/init_rq_db.py

from __future__ import annotations

from contextlib import contextmanager

import pymysql

from autotrade.coreutils.config import DatabaseInfo

# ============================================================
# Constants
# ============================================================

DAILY_FREQUENCIES = ["1d", "1w"]
MINUTE_FREQUENCIES = ["1m", "5m", "15m", "30m", "60m"]

RQ_DATABASES = {
    "rq_data",
    "rq_stock_data",
    "rq_etf_data",
    "rq_future_data",
    "rq_option_data",
    "rq_index_data",
}


# ============================================================
# Base helpers
# ============================================================

@contextmanager
def get_conn(database: str | None = None):
    conn = pymysql.connect(
        host=DatabaseInfo.host,
        port=DatabaseInfo.port,
        user=DatabaseInfo.user,
        passwd=DatabaseInfo.password,
        database=database,
        charset="utf8mb4",
        autocommit=True,
    )
    try:
        yield conn
    finally:
        conn.close()


def execute_sql(sql: str, database: str | None = None) -> None:
    with get_conn(database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)


def create_database_if_not_exists(database_name: str) -> None:
    sql = f"""
    CREATE DATABASE IF NOT EXISTS `{database_name}`
    DEFAULT CHARACTER SET utf8mb4
    """
    execute_sql(sql)


def create_rq_base_databases() -> None:
    for db in RQ_DATABASES:
        create_database_if_not_exists(db)


# ============================================================
# Reusable price table builders
# ============================================================

def build_daily_price_table_sql(table_name: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `order_book_id` VARCHAR(64) NOT NULL,
        `date` DATE NOT NULL,
        `type` VARCHAR(32) NOT NULL,
        `frequency` VARCHAR(8) NOT NULL,
        `market` VARCHAR(16) NOT NULL DEFAULT 'cn',

        `open` DOUBLE NULL,
        `close` DOUBLE NULL,
        `high` DOUBLE NULL,
        `low` DOUBLE NULL,
        `limit_up` DOUBLE NULL,
        `limit_down` DOUBLE NULL,
        `total_turnover` DOUBLE NULL,
        `volume` DOUBLE NULL,
        `num_trades` DOUBLE NULL,
        `prev_close` DOUBLE NULL,
        `settlement` DOUBLE NULL,
        `prev_settlement` DOUBLE NULL,
        `open_interest` DOUBLE NULL,
        `dominant_id` VARCHAR(64) NULL,
        `strike_price` DOUBLE NULL,
        `contract_multiplier` DOUBLE NULL,
        `iopv` DOUBLE NULL,
        `day_session_open` DOUBLE NULL,

        `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

        PRIMARY KEY (`order_book_id`, `date`),
        KEY `idx_date` (`date`),
        KEY `idx_type_date` (`type`, `date`),
        KEY `idx_frequency_date` (`frequency`, `date`),
        KEY `idx_market_date` (`market`, `date`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """


def build_minute_price_table_sql(table_name: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `order_book_id` VARCHAR(64) NOT NULL,
        `datetime` DATETIME NOT NULL,
        `type` VARCHAR(32) NOT NULL,
        `frequency` VARCHAR(8) NOT NULL,
        `market` VARCHAR(16) NOT NULL DEFAULT 'cn',

        `trading_date` DATE NULL,

        `open` DOUBLE NULL,
        `close` DOUBLE NULL,
        `high` DOUBLE NULL,
        `low` DOUBLE NULL,
        `limit_up` DOUBLE NULL,
        `limit_down` DOUBLE NULL,
        `total_turnover` DOUBLE NULL,
        `volume` DOUBLE NULL,
        `num_trades` DOUBLE NULL,
        `prev_close` DOUBLE NULL,
        `settlement` DOUBLE NULL,
        `prev_settlement` DOUBLE NULL,
        `open_interest` DOUBLE NULL,
        `dominant_id` VARCHAR(64) NULL,
        `strike_price` DOUBLE NULL,
        `contract_multiplier` DOUBLE NULL,
        `iopv` DOUBLE NULL,
        `day_session_open` DOUBLE NULL,

        `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

        PRIMARY KEY (`order_book_id`, `datetime`),
        KEY `idx_datetime` (`datetime`),
        KEY `idx_type_datetime` (`type`, `datetime`),
        KEY `idx_frequency_datetime` (`frequency`, `datetime`),
        KEY `idx_trading_date` (`trading_date`),
        KEY `idx_market_datetime` (`market`, `datetime`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """


def create_price_tables_for_database(database_name: str, table_prefix: str) -> None:
    for freq in DAILY_FREQUENCIES:
        table_name = f"{table_prefix}_{freq}"
        execute_sql(build_daily_price_table_sql(table_name), database=database_name)

    for freq in MINUTE_FREQUENCIES:
        table_name = f"{table_prefix}_{freq}"
        execute_sql(build_minute_price_table_sql(table_name), database=database_name)


# ============================================================
# Futures-specific tables
# ============================================================

def build_future_instruments_table_sql(table_name: str = "future_instruments") -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `order_book_id` VARCHAR(32) NOT NULL COMMENT '期货合约代码',

        `symbol` VARCHAR(64) NULL COMMENT '合约简称',
        `trading_code` VARCHAR(64) NULL COMMENT '交易代码',

        `exchange` VARCHAR(16) NULL COMMENT '交易所',
        `product` VARCHAR(32) NULL COMMENT '合约种类',
        `industry_name` VARCHAR(64) NULL COMMENT '行业分类名称',

        `underlying_order_book_id` VARCHAR(32) NULL COMMENT '标的代码',
        `underlying_symbol` VARCHAR(32) NULL COMMENT '标的名称',

        `contract_multiplier` DOUBLE NULL COMMENT '合约乘数',
        `margin_rate` DOUBLE NULL COMMENT '最低保证金率',
        `round_lot` DOUBLE NULL COMMENT '最小交易单位',

        `listed_date` DATE NULL COMMENT '上市日期',
        `de_listed_date` DATE NULL COMMENT '退市日期',
        `maturity_date` DATE NULL COMMENT '到期日',
        `start_delivery_date` DATE NULL COMMENT '开始交割日',
        `end_delivery_date` DATE NULL COMMENT '结束交割日',

        `market_tplus` VARCHAR(8) NULL COMMENT '交易制度',
        `trading_hours` VARCHAR(128) NULL COMMENT '交易时间',

        `type` VARCHAR(16) NOT NULL DEFAULT 'Future' COMMENT '合约类型',
        `market` VARCHAR(8) NOT NULL DEFAULT 'cn' COMMENT '市场',

        `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

        PRIMARY KEY (`order_book_id`),

        KEY `idx_symbol` (`symbol`),
        KEY `idx_trading_code` (`trading_code`),
        KEY `idx_underlying_symbol` (`underlying_symbol`),
        KEY `idx_underlying_obid` (`underlying_order_book_id`),
        KEY `idx_product` (`product`),
        KEY `idx_exchange` (`exchange`),
        KEY `idx_industry` (`industry_name`),
        KEY `idx_listed_date` (`listed_date`),
        KEY `idx_delisted_date` (`de_listed_date`),
        KEY `idx_maturity_date` (`maturity_date`),
        KEY `idx_underlying_product` (`underlying_symbol`, `product`),
        KEY `idx_exchange_product` (`exchange`, `product`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='期货合约基础信息表';
    """


def create_future_specific_tables(database_name: str = "rq_future_data") -> None:
    execute_sql(build_future_instruments_table_sql(), database=database_name)


# ============================================================
# Asset-group entrypoints
# ============================================================

def create_rq_futures_data(database_name: str = "rq_future_data") -> None:
    """
    创建 futures 相关全部表：
    - 通用价格表 future_price_*
    - futures 专属表 future_instruments
    """
    create_database_if_not_exists(database_name)

    # 通用可复用 price 表
    create_price_tables_for_database(
        database_name=database_name,
        table_prefix="future_price",
    )

    # futures 专属表
    create_future_specific_tables(database_name=database_name)


def build_option_instruments_table_sql(table_name: str = "option_instruments") -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `order_book_id` VARCHAR(32) NOT NULL COMMENT '期权合约代码',
        `symbol` VARCHAR(128) NULL COMMENT '合约简称',
        `round_lot` DOUBLE NULL COMMENT '最小下单手数',
        `listed_date` DATE NULL COMMENT '上市日期',
        `type` VARCHAR(16) NOT NULL DEFAULT 'Option' COMMENT '合约类型',
        `contract_multiplier` DOUBLE NULL COMMENT '合约乘数',
        `underlying_order_book_id` VARCHAR(32) NULL COMMENT '标的代码',
        `underlying_symbol` VARCHAR(32) NULL COMMENT '所属品种',
        `maturity_date` DATE NULL COMMENT '到期日',
        `exchange` VARCHAR(16) NULL COMMENT '交易所',
        `strike_price` DOUBLE NULL COMMENT '行权价',
        `option_type` VARCHAR(8) NULL COMMENT 'C认购 / P认沽',
        `exercise_type` VARCHAR(8) NULL COMMENT 'E欧式 / A美式',
        `market_tplus` VARCHAR(8) NULL COMMENT '交易制度',
        `product_name` VARCHAR(64) NULL COMMENT 'ETF期权字母简称',
        `market` VARCHAR(8) NOT NULL DEFAULT 'cn' COMMENT '市场',

        `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

        PRIMARY KEY (`order_book_id`),

        KEY `idx_symbol` (`symbol`),
        KEY `idx_underlying_symbol` (`underlying_symbol`),
        KEY `idx_exchange` (`exchange`),
        KEY `idx_option_type` (`option_type`),
        KEY `idx_exercise_type` (`exercise_type`),
        KEY `idx_product_name` (`product_name`),
        KEY `idx_listed_date` (`listed_date`),
        KEY `idx_maturity_date` (`maturity_date`),
        KEY `idx_underlying_option_type` (`underlying_symbol`, `option_type`),
        KEY `idx_exchange_product_name` (`exchange`, `product_name`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='期权合约基础信息表';
    """

def build_option_greeks_daily_table_sql(table_name: str = "option_greeks_1d") -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `order_book_id` VARCHAR(32) NOT NULL,
        `trading_date` DATE NOT NULL,
        `model` VARCHAR(32) NOT NULL DEFAULT 'implied_forward',
        `price_type` VARCHAR(16) NOT NULL DEFAULT 'close',
        `frequency` VARCHAR(8) NOT NULL DEFAULT '1d',
        `market` VARCHAR(8) NOT NULL DEFAULT 'cn',

        `iv` DOUBLE NULL,
        `delta` DOUBLE NULL,
        `gamma` DOUBLE NULL,
        `vega` DOUBLE NULL,
        `theta` DOUBLE NULL,
        `rho` DOUBLE NULL,

        `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

        PRIMARY KEY (`order_book_id`, `trading_date`, `model`, `price_type`),
        KEY `idx_trading_date` (`trading_date`),
        KEY `idx_model_trading_date` (`model`, `trading_date`),
        KEY `idx_price_type_trading_date` (`price_type`, `trading_date`),
        KEY `idx_market_trading_date` (`market`, `trading_date`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='期权Greek日表';
    """


def build_option_greeks_minute_table_sql(table_name: str = "option_greeks_1m") -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `order_book_id` VARCHAR(32) NOT NULL,
        `datetime` DATETIME NOT NULL,
        `model` VARCHAR(32) NOT NULL DEFAULT 'implied_forward',
        `price_type` VARCHAR(16) NOT NULL DEFAULT 'close',
        `frequency` VARCHAR(8) NOT NULL DEFAULT '1m',
        `market` VARCHAR(8) NOT NULL DEFAULT 'cn',

        `iv` DOUBLE NULL,
        `delta` DOUBLE NULL,
        `gamma` DOUBLE NULL,
        `vega` DOUBLE NULL,
        `theta` DOUBLE NULL,
        `rho` DOUBLE NULL,

        `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

        PRIMARY KEY (`order_book_id`, `datetime`, `model`, `price_type`),
        KEY `idx_datetime` (`datetime`),
        KEY `idx_model_datetime` (`model`, `datetime`),
        KEY `idx_price_type_datetime` (`price_type`, `datetime`),
        KEY `idx_market_datetime` (`market`, `datetime`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='期权Greek分钟表';
    """

def create_option_greeks_tables(database_name: str = "rq_option_data") -> None:
    execute_sql(build_option_greeks_daily_table_sql(), database=database_name)
    execute_sql(build_option_greeks_minute_table_sql(), database=database_name)


def create_option_specific_tables(database_name: str = "rq_option_data") -> None:
    execute_sql(build_option_instruments_table_sql(), database=database_name)
    create_option_greeks_tables(database_name=database_name)


def create_rq_options_data(database_name: str = "rq_option_data") -> None:
    """
    创建 options 相关全部表：
    - 通用价格表 option_price_*
    - options 专属表 option_instruments
    """
    create_database_if_not_exists(database_name)

    # 通用 price 表
    create_price_tables_for_database(
        database_name=database_name,
        table_prefix="option_price",
    )

    # option 专属表
    create_option_specific_tables(database_name=database_name)


def build_index_instruments_table_sql(table_name: str = "index_instruments") -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `order_book_id` VARCHAR(32) NOT NULL COMMENT '指数代码',
        `symbol` VARCHAR(128) NULL COMMENT '简称',
        `abbrev_symbol` VARCHAR(64) NULL COMMENT '名称缩写',
        `round_lot` BIGINT NULL COMMENT '一手数量',
        `sector_code` VARCHAR(64) NULL COMMENT '板块代码',
        `sector_code_name` VARCHAR(128) NULL COMMENT '板块名称',
        `industry_code` VARCHAR(64) NULL COMMENT '行业代码',
        `industry_name` VARCHAR(128) NULL COMMENT '行业名称',
        `listed_date` DATE NULL COMMENT '上市日期',
        `issue_price` DOUBLE NULL COMMENT '发行价',
        `de_listed_date` DATE NULL COMMENT '退市日期',
        `type` VARCHAR(16) NOT NULL DEFAULT 'INDX' COMMENT '合约类型',
        `underlying_order_book_id` VARCHAR(32) NULL COMMENT '已废弃',
        `underlying_name` VARCHAR(128) NULL COMMENT '已废弃',
        `concept_names` TEXT NULL COMMENT '已废弃',
        `exchange` VARCHAR(16) NULL COMMENT '交易所',
        `board_type` VARCHAR(32) NULL COMMENT '板块类别',
        `status` VARCHAR(32) NULL COMMENT '状态',
        `special_type` VARCHAR(32) NULL COMMENT '特别处理状态',
        `trading_hours` VARCHAR(128) NULL COMMENT '交易时间',
        `least_redeem` VARCHAR(64) NULL COMMENT '最低申赎份额',
        `cross_market` VARCHAR(16) NULL COMMENT '沪深港通标识',
        `market_tplus` VARCHAR(8) NULL COMMENT '交易制度',
        `purchasedate` DATE NULL COMMENT '申购日期',
        `base_date` DATE NULL COMMENT '基日',
        `base_point` VARCHAR(64) NULL COMMENT '基点',
        `market` VARCHAR(8) NOT NULL DEFAULT 'cn' COMMENT '市场',

        `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

        PRIMARY KEY (`order_book_id`),

        KEY `idx_symbol` (`symbol`),
        KEY `idx_industry_code` (`industry_code`),
        KEY `idx_industry_name` (`industry_name`),
        KEY `idx_board_type` (`board_type`),
        KEY `idx_exchange` (`exchange`),
        KEY `idx_status` (`status`),
        KEY `idx_special_type` (`special_type`),
        KEY `idx_listed_date` (`listed_date`),
        KEY `idx_delisted_date` (`de_listed_date`),
        KEY `idx_exchange_status` (`exchange`, `status`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指数基础信息表';
    """


def create_index_specific_tables(database_name: str = "rq_index_data") -> None:
    execute_sql(build_index_instruments_table_sql(), database=database_name)


def create_rq_index_data(database_name: str = "rq_index_data") -> None:
    """
    创建 index 相关全部表：
    - 通用价格表 stock_price_*
    - index 专属表 index_instruments

    注意：
    指数价格目前复用 rq_stock_data 下的 stock_price_*，
    通过 type='INDX' 区分。
    """
    create_database_if_not_exists(database_name)

    # 通用可复用 price 表（股票库共用）
    create_price_tables_for_database(
        database_name=database_name,
        table_prefix="stock_price",
    )

    # index 专属表
    create_index_specific_tables(database_name=database_name)


def build_cn_stock_instruments_table_sql(table_name: str = "cn_stock_instruments") -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `order_book_id` VARCHAR(32) NOT NULL COMMENT 'A股代码',
        `symbol` VARCHAR(128) NULL COMMENT '简称',
        `abbrev_symbol` VARCHAR(64) NULL COMMENT '名称缩写',
        `round_lot` BIGINT NULL COMMENT '一手数量',
        `sector_code` VARCHAR(64) NULL COMMENT '板块代码',
        `sector_code_name` VARCHAR(128) NULL COMMENT '板块名称',
        `industry_code` VARCHAR(64) NULL COMMENT '行业代码',
        `industry_name` VARCHAR(128) NULL COMMENT '行业名称',
        `listed_date` DATE NULL COMMENT '上市日期',
        `issue_price` DOUBLE NULL COMMENT '发行价',
        `de_listed_date` DATE NULL COMMENT '退市日期',
        `type` VARCHAR(16) NOT NULL DEFAULT 'CS' COMMENT '合约类型',
        `underlying_order_book_id` VARCHAR(32) NULL COMMENT '已废弃',
        `underlying_name` VARCHAR(128) NULL COMMENT '已废弃',
        `concept_names` TEXT NULL COMMENT '已废弃',
        `exchange` VARCHAR(16) NULL COMMENT '交易所',
        `board_type` VARCHAR(32) NULL COMMENT '板块类别',
        `status` VARCHAR(32) NULL COMMENT '状态',
        `special_type` VARCHAR(32) NULL COMMENT '特别处理状态',
        `trading_hours` VARCHAR(128) NULL COMMENT '交易时间',
        `least_redeem` VARCHAR(64) NULL COMMENT '最低申赎份额',
        `cross_market` VARCHAR(16) NULL COMMENT '沪深港通标识',
        `market_tplus` VARCHAR(8) NULL COMMENT '交易制度',
        `purchasedate` DATE NULL COMMENT '申购日期',
        `base_date` DATE NULL COMMENT '基日',
        `base_point` VARCHAR(64) NULL COMMENT '基点',
        `market` VARCHAR(8) NOT NULL DEFAULT 'cn' COMMENT '市场',

        `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

        PRIMARY KEY (`order_book_id`),

        KEY `idx_symbol` (`symbol`),
        KEY `idx_industry_code` (`industry_code`),
        KEY `idx_industry_name` (`industry_name`),
        KEY `idx_board_type` (`board_type`),
        KEY `idx_exchange` (`exchange`),
        KEY `idx_status` (`status`),
        KEY `idx_special_type` (`special_type`),
        KEY `idx_listed_date` (`listed_date`),
        KEY `idx_delisted_date` (`de_listed_date`),
        KEY `idx_exchange_status` (`exchange`, `status`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='A股基础信息表';
    """


def create_cn_stock_specific_tables(database_name: str = "rq_stock_data") -> None:
    execute_sql(build_cn_stock_instruments_table_sql(), database=database_name)


def create_rq_cn_stock_data(database_name: str = "rq_stock_data") -> None:
    create_database_if_not_exists(database_name)

    create_price_tables_for_database(
        database_name=database_name,
        table_prefix="stock_price",
    )

    create_cn_stock_specific_tables(database_name=database_name)


def build_etf_instruments_table_sql(table_name: str = "etf_instruments") -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `order_book_id` VARCHAR(32) NOT NULL COMMENT 'ETF代码',
        `symbol` VARCHAR(128) NULL COMMENT '简称',
        `abbrev_symbol` VARCHAR(64) NULL COMMENT '名称缩写',
        `round_lot` BIGINT NULL COMMENT '一手数量',
        `sector_code` VARCHAR(64) NULL COMMENT '板块代码',
        `sector_code_name` VARCHAR(128) NULL COMMENT '板块名称',
        `industry_code` VARCHAR(64) NULL COMMENT '行业代码',
        `industry_name` VARCHAR(128) NULL COMMENT '行业名称',
        `listed_date` DATE NULL COMMENT '上市日期',
        `issue_price` DOUBLE NULL COMMENT '发行价',
        `de_listed_date` DATE NULL COMMENT '退市日期',
        `type` VARCHAR(16) NOT NULL DEFAULT 'ETF' COMMENT '合约类型',
        `underlying_order_book_id` VARCHAR(32) NULL COMMENT '跟踪基准代码',
        `underlying_name` VARCHAR(128) NULL COMMENT '跟踪基准名称',
        `concept_names` TEXT NULL COMMENT '已废弃',
        `exchange` VARCHAR(16) NULL COMMENT '交易所',
        `board_type` VARCHAR(32) NULL COMMENT '板块类别',
        `status` VARCHAR(32) NULL COMMENT '状态',
        `special_type` VARCHAR(32) NULL COMMENT '特别处理状态',
        `trading_hours` VARCHAR(128) NULL COMMENT '交易时间',
        `least_redeem` VARCHAR(64) NULL COMMENT '最低申赎份额',
        `cross_market` VARCHAR(16) NULL COMMENT '沪深港通标识',
        `market_tplus` VARCHAR(8) NULL COMMENT '交易制度',
        `purchasedate` DATE NULL COMMENT '申购日期',
        `base_date` DATE NULL COMMENT '基日',
        `base_point` VARCHAR(64) NULL COMMENT '基点',
        `market` VARCHAR(8) NOT NULL DEFAULT 'cn' COMMENT '市场',

        `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

        PRIMARY KEY (`order_book_id`),

        KEY `idx_symbol` (`symbol`),
        KEY `idx_industry_code` (`industry_code`),
        KEY `idx_industry_name` (`industry_name`),
        KEY `idx_board_type` (`board_type`),
        KEY `idx_exchange` (`exchange`),
        KEY `idx_status` (`status`),
        KEY `idx_special_type` (`special_type`),
        KEY `idx_listed_date` (`listed_date`),
        KEY `idx_delisted_date` (`de_listed_date`),
        KEY `idx_exchange_status` (`exchange`, `status`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ETF基础信息表';
    """


def create_etf_specific_tables(database_name: str = "rq_etf_data") -> None:
    execute_sql(build_etf_instruments_table_sql(), database=database_name)


def create_rq_etf_data(database_name: str = "rq_etf_data") -> None:
    create_database_if_not_exists(database_name)

    create_price_tables_for_database(
        database_name=database_name,
        table_prefix="etf_price",
    )

    create_etf_specific_tables(database_name=database_name)


# ============================================================
# Global init
# ============================================================

def init_rq_db() -> None:
    create_rq_base_databases()
    create_rq_futures_data()
    create_rq_options_data()
    create_rq_index_data()
    create_rq_cn_stock_data()
    create_rq_etf_data()


if __name__ == "__main__":
    from autotrade.coreutils.config import load_env

    load_env("d:/.env")

    init_rq_db()
    print("RiceQuant futures database initialized successfully.")
