# autotrade/data/ricequant/init_rq_db.py

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from autotrade.coreutils.config import DatabaseInfo, load_env
from autotrade.data.ricequant._clickhouse import ClickHouseClient


def _import_pymysql():
    try:
        import pymysql
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pymysql is required for RiceQuant database initialization. "
            "Install it in the active environment, for example: pip install pymysql"
        ) from exc
    return pymysql

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

CLICKHOUSE_CLIENT = ClickHouseClient()


# ============================================================
# Base helpers
# ============================================================

@contextmanager
def get_conn(database: str | None = None):
    pymysql = _import_pymysql()
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


def execute_clickhouse_sql(sql: str, database: str | None = None) -> None:
    CLICKHOUSE_CLIENT.execute(sql, database=database)


def create_mysql_database_if_not_exists(database_name: str) -> None:
    sql = f"""
    CREATE DATABASE IF NOT EXISTS `{database_name}`
    DEFAULT CHARACTER SET utf8mb4
    """
    execute_sql(sql)


def create_clickhouse_database_if_not_exists(database_name: str) -> None:
    execute_clickhouse_sql(f"CREATE DATABASE IF NOT EXISTS `{database_name}`")


def drop_mysql_database_if_exists(database_name: str) -> None:
    execute_sql(f"DROP DATABASE IF EXISTS `{database_name}`")


def drop_clickhouse_database_if_exists(database_name: str) -> None:
    execute_clickhouse_sql(f"DROP DATABASE IF EXISTS `{database_name}`")


def create_rq_base_databases() -> None:
    for db in RQ_DATABASES:
        create_mysql_database_if_not_exists(db)
        create_clickhouse_database_if_not_exists(db)

def build_year_range_partitions_sql(
    column_name: str,
    start_year: int = 2005,
    end_year: int | None = None,
) -> str:
    """
    生成 MySQL RANGE COLUMNS 年分区定义。
    例如：
        PARTITION BY RANGE COLUMNS(`date`) (
            PARTITION p2005 VALUES LESS THAN ('2006-01-01'),
            ...
            PARTITION pmax VALUES LESS THAN (MAXVALUE)
        )

    说明：
    - end_year 表示“最后一个显式年份分区”
    - 超过 end_year 的数据进入 pmax
    """
    if end_year is None:
        end_year = datetime.now().year + 3

    parts = []
    for year in range(start_year, end_year + 1):
        less_than = f"{year + 1}-01-01"
        parts.append(
            f"PARTITION p{year} VALUES LESS THAN ('{less_than}')"
        )

    parts.append("PARTITION pmax VALUES LESS THAN (MAXVALUE)")

    return (
        f"PARTITION BY RANGE COLUMNS(`{column_name}`) (\n        "
        + ",\n        ".join(parts)
        + "\n    )"
    )


def build_month_range_partitions_sql(
    column_name: str,
    start_year: int = 2005,
    start_month: int = 1,
    end_year: int | None = None,
    end_month: int = 12,
) -> str:
    if end_year is None:
        end_year = datetime.now().year + 2

    parts = []
    year = start_year
    month = start_month

    while (year, month) <= (end_year, end_month):
        next_year = year + (1 if month == 12 else 0)
        next_month = 1 if month == 12 else month + 1
        less_than = f"{next_year:04d}-{next_month:02d}-01"
        parts.append(
            f"PARTITION p{year:04d}{month:02d} VALUES LESS THAN ('{less_than}')"
        )
        year, month = next_year, next_month

    parts.append("PARTITION pmax VALUES LESS THAN (MAXVALUE)")

    return (
        f"PARTITION BY RANGE COLUMNS(`{column_name}`) (\n        "
        + ",\n        ".join(parts)
        + "\n    )"
    )


# ============================================================
# Internal shared price table builders
# ============================================================

def build_daily_price_table_sql(table_name: str) -> str:
    partition_sql = build_month_range_partitions_sql("date")

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
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    {partition_sql};
    """


def build_minute_price_table_sql(table_name: str) -> str:
    partition_sql = build_month_range_partitions_sql("datetime")

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
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    {partition_sql};
    """

def create_price_tables_for_database(database_name: str, table_prefix: str) -> None:
    for freq in DAILY_FREQUENCIES:
        table_name = f"{table_prefix}_{freq}"
        execute_sql(build_daily_price_table_sql(table_name), database=database_name)

    for freq in MINUTE_FREQUENCIES:
        table_name = f"{table_prefix}_{freq}"
        execute_sql(build_minute_price_table_sql(table_name), database=database_name)


def _build_clickhouse_daily_price_table_sql(database_name: str, table_name: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS `{database_name}`.`{table_name}` (
        `order_book_id` String,
        `date` Date,
        `type` String,
        `frequency` String,
        `market` String,
        `open` Nullable(Float64),
        `close` Nullable(Float64),
        `high` Nullable(Float64),
        `low` Nullable(Float64),
        `limit_up` Nullable(Float64),
        `limit_down` Nullable(Float64),
        `total_turnover` Nullable(Float64),
        `volume` Nullable(Float64),
        `num_trades` Nullable(Float64),
        `prev_close` Nullable(Float64),
        `settlement` Nullable(Float64),
        `prev_settlement` Nullable(Float64),
        `open_interest` Nullable(Float64),
        `dominant_id` Nullable(String),
        `strike_price` Nullable(Float64),
        `contract_multiplier` Nullable(Float64),
        `iopv` Nullable(Float64),
        `day_session_open` Nullable(Float64),
        `ingest_time` DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(ingest_time)
    PARTITION BY toYYYYMM(`date`)
    ORDER BY (`date`, `order_book_id`)
    """


def _build_clickhouse_daily_adjusted_price_table_sql(database_name: str, table_name: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS `{database_name}`.`{table_name}` (
        `order_book_id` String,
        `date` Date,
        `adjust_type` String,
        `type` String,
        `frequency` String,
        `market` String,
        `open` Nullable(Float64),
        `close` Nullable(Float64),
        `high` Nullable(Float64),
        `low` Nullable(Float64),
        `limit_up` Nullable(Float64),
        `limit_down` Nullable(Float64),
        `total_turnover` Nullable(Float64),
        `volume` Nullable(Float64),
        `num_trades` Nullable(Float64),
        `prev_close` Nullable(Float64),
        `settlement` Nullable(Float64),
        `prev_settlement` Nullable(Float64),
        `open_interest` Nullable(Float64),
        `dominant_id` Nullable(String),
        `strike_price` Nullable(Float64),
        `contract_multiplier` Nullable(Float64),
        `iopv` Nullable(Float64),
        `day_session_open` Nullable(Float64),
        `ingest_time` DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(ingest_time)
    PARTITION BY toYYYYMM(`date`)
    ORDER BY (`date`, `order_book_id`, `adjust_type`)
    """


def _build_clickhouse_minute_price_table_sql(database_name: str, table_name: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS `{database_name}`.`{table_name}` (
        `order_book_id` String,
        `datetime` DateTime,
        `type` String,
        `frequency` String,
        `market` String,
        `open` Nullable(Float64),
        `close` Nullable(Float64),
        `high` Nullable(Float64),
        `low` Nullable(Float64),
        `limit_up` Nullable(Float64),
        `limit_down` Nullable(Float64),
        `total_turnover` Nullable(Float64),
        `volume` Nullable(Float64),
        `num_trades` Nullable(Float64),
        `prev_close` Nullable(Float64),
        `settlement` Nullable(Float64),
        `prev_settlement` Nullable(Float64),
        `open_interest` Nullable(Float64),
        `dominant_id` Nullable(String),
        `strike_price` Nullable(Float64),
        `contract_multiplier` Nullable(Float64),
        `iopv` Nullable(Float64),
        `day_session_open` Nullable(Float64),
        `ingest_time` DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(ingest_time)
    PARTITION BY toYYYYMM(`datetime`)
    ORDER BY (`datetime`, `order_book_id`)
    """


def _build_clickhouse_minute_price_with_trading_date_table_sql(database_name: str, table_name: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS `{database_name}`.`{table_name}` (
        `order_book_id` String,
        `datetime` DateTime,
        `trading_date` Nullable(Date),
        `type` String,
        `frequency` String,
        `market` String,
        `open` Nullable(Float64),
        `close` Nullable(Float64),
        `high` Nullable(Float64),
        `low` Nullable(Float64),
        `limit_up` Nullable(Float64),
        `limit_down` Nullable(Float64),
        `total_turnover` Nullable(Float64),
        `volume` Nullable(Float64),
        `num_trades` Nullable(Float64),
        `prev_close` Nullable(Float64),
        `settlement` Nullable(Float64),
        `prev_settlement` Nullable(Float64),
        `open_interest` Nullable(Float64),
        `dominant_id` Nullable(String),
        `strike_price` Nullable(Float64),
        `contract_multiplier` Nullable(Float64),
        `iopv` Nullable(Float64),
        `day_session_open` Nullable(Float64),
        `ingest_time` DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(ingest_time)
    PARTITION BY toYYYYMM(`datetime`)
    ORDER BY (`datetime`, `order_book_id`)
    """


def _build_clickhouse_minute_adjusted_price_table_sql(database_name: str, table_name: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS `{database_name}`.`{table_name}` (
        `order_book_id` String,
        `datetime` DateTime,
        `adjust_type` String,
        `type` String,
        `frequency` String,
        `market` String,
        `open` Nullable(Float64),
        `close` Nullable(Float64),
        `high` Nullable(Float64),
        `low` Nullable(Float64),
        `limit_up` Nullable(Float64),
        `limit_down` Nullable(Float64),
        `total_turnover` Nullable(Float64),
        `volume` Nullable(Float64),
        `num_trades` Nullable(Float64),
        `prev_close` Nullable(Float64),
        `settlement` Nullable(Float64),
        `prev_settlement` Nullable(Float64),
        `open_interest` Nullable(Float64),
        `dominant_id` Nullable(String),
        `strike_price` Nullable(Float64),
        `contract_multiplier` Nullable(Float64),
        `iopv` Nullable(Float64),
        `day_session_open` Nullable(Float64),
        `ingest_time` DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(ingest_time)
    PARTITION BY toYYYYMM(`datetime`)
    ORDER BY (`datetime`, `order_book_id`, `adjust_type`)
    """


def _create_clickhouse_price_tables_for_database(database_name: str, table_prefix: str) -> None:
    for freq in DAILY_FREQUENCIES:
        table_name = f"{table_prefix}_{freq}"
        execute_clickhouse_sql(
            _build_clickhouse_daily_price_table_sql(database_name, table_name),
            database=database_name,
        )

    for freq in MINUTE_FREQUENCIES:
        table_name = f"{table_prefix}_{freq}"
        execute_clickhouse_sql(
            _build_clickhouse_minute_price_table_sql(database_name, table_name),
            database=database_name,
        )


# ============================================================
# Futures price tables
# ============================================================

def build_future_daily_price_table_sql(table_name: str) -> str:
    return _build_clickhouse_daily_price_table_sql("rq_future_data", table_name)


def build_future_minute_price_table_sql(table_name: str) -> str:
    return _build_clickhouse_minute_price_with_trading_date_table_sql("rq_future_data", table_name)


def create_future_price_tables(database_name: str = "rq_future_data") -> None:
    for freq in DAILY_FREQUENCIES:
        table_name = f"future_price_{freq}"
        execute_clickhouse_sql(
            build_future_daily_price_table_sql(table_name),
            database=database_name,
        )

    for freq in MINUTE_FREQUENCIES:
        table_name = f"future_price_{freq}"
        execute_clickhouse_sql(
            build_future_minute_price_table_sql(table_name),
            database=database_name,
        )


# ============================================================
# Options price tables
# ============================================================

def build_option_daily_price_table_sql(table_name: str) -> str:
    return _build_clickhouse_daily_price_table_sql("rq_option_data", table_name)


def build_option_minute_price_table_sql(table_name: str) -> str:
    return _build_clickhouse_minute_price_with_trading_date_table_sql("rq_option_data", table_name)


def create_option_price_tables(database_name: str = "rq_option_data") -> None:
    for freq in DAILY_FREQUENCIES:
        table_name = f"option_price_{freq}"
        execute_clickhouse_sql(
            build_option_daily_price_table_sql(table_name),
            database=database_name,
        )

    for freq in MINUTE_FREQUENCIES:
        table_name = f"option_price_{freq}"
        execute_clickhouse_sql(
            build_option_minute_price_table_sql(table_name),
            database=database_name,
        )


# ============================================================
# Index price tables
# ============================================================

def build_index_daily_price_table_sql(table_name: str) -> str:
    return _build_clickhouse_daily_price_table_sql("rq_index_data", table_name)


def build_index_minute_price_table_sql(table_name: str) -> str:
    return _build_clickhouse_minute_price_table_sql("rq_index_data", table_name)


def create_index_price_tables(database_name: str = "rq_index_data") -> None:
    _create_clickhouse_price_tables_for_database(database_name, "index_price")


# ============================================================
# CN stock price tables
# ============================================================

def build_cn_stock_daily_price_table_sql(table_name: str) -> str:
    return _build_clickhouse_daily_adjusted_price_table_sql("rq_stock_data", table_name)


def build_cn_stock_minute_price_table_sql(table_name: str) -> str:
    return _build_clickhouse_minute_adjusted_price_table_sql("rq_stock_data", table_name)


def create_cn_stock_price_tables(database_name: str = "rq_stock_data") -> None:
    for freq in DAILY_FREQUENCIES:
        table_name = f"stock_price_{freq}"
        execute_clickhouse_sql(
            build_cn_stock_daily_price_table_sql(table_name),
            database=database_name,
        )

    for freq in MINUTE_FREQUENCIES:
        table_name = f"stock_price_{freq}"
        execute_clickhouse_sql(
            build_cn_stock_minute_price_table_sql(table_name),
            database=database_name,
        )


# ============================================================
# ETF price tables
# ============================================================

def build_etf_daily_price_table_sql(table_name: str) -> str:
    return _build_clickhouse_daily_adjusted_price_table_sql("rq_etf_data", table_name)


def build_etf_minute_price_table_sql(table_name: str) -> str:
    return _build_clickhouse_minute_adjusted_price_table_sql("rq_etf_data", table_name)


def create_etf_price_tables(database_name: str = "rq_etf_data") -> None:
    for freq in DAILY_FREQUENCIES:
        table_name = f"etf_price_{freq}"
        execute_clickhouse_sql(
            build_etf_daily_price_table_sql(table_name),
            database=database_name,
        )

    for freq in MINUTE_FREQUENCIES:
        table_name = f"etf_price_{freq}"
        execute_clickhouse_sql(
            build_etf_minute_price_table_sql(table_name),
            database=database_name,
        )


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
    create_mysql_database_if_not_exists(database_name)
    create_clickhouse_database_if_not_exists(database_name)

    # 高频 / 时序 price 表走 ClickHouse
    create_future_price_tables(database_name=database_name)

    # futures 元数据表走 MySQL
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
        `order_book_id` String,
        `trading_date` Date,
        `model` String,
        `price_type` String,
        `frequency` String,
        `market` String,
        `iv` Nullable(Float64),
        `delta` Nullable(Float64),
        `gamma` Nullable(Float64),
        `vega` Nullable(Float64),
        `theta` Nullable(Float64),
        `rho` Nullable(Float64),
        `ingest_time` DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(ingest_time)
    PARTITION BY toYYYYMM(`trading_date`)
    ORDER BY (`trading_date`, `order_book_id`)
    """


def build_option_greeks_minute_table_sql(table_name: str = "option_greeks_1m") -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `order_book_id` String,
        `datetime` DateTime,
        `model` String,
        `price_type` String,
        `frequency` String,
        `market` String,
        `iv` Nullable(Float64),
        `delta` Nullable(Float64),
        `gamma` Nullable(Float64),
        `vega` Nullable(Float64),
        `theta` Nullable(Float64),
        `rho` Nullable(Float64),
        `ingest_time` DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(ingest_time)
    PARTITION BY toYYYYMM(`datetime`)
    ORDER BY (`datetime`, `order_book_id`)
    """



def create_option_greeks_tables(database_name: str = "rq_option_data") -> None:
    execute_clickhouse_sql(build_option_greeks_daily_table_sql(), database=database_name)
    execute_clickhouse_sql(build_option_greeks_minute_table_sql(), database=database_name)


def build_calculated_option_greeks_daily_table_sql(
    table_name: str = "calculated_option_greeks_1d",
) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `order_book_id` String,
        `date` Date,
        `opt_symbol` String,
        `underlying_order_book_id` String,
        `maturity_date` Date,
        `strike_price` Float64,
        `option_type` String,
        `option_price` Nullable(Float64),
        `forward_price` Nullable(Float64),
        `risk_free_rate` Float64,
        `t_days` Int32,
        `iv` Nullable(Float64),
        `delta` Nullable(Float64),
        `gamma` Nullable(Float64),
        `vega` Nullable(Float64),
        `theta` Nullable(Float64),
        `rho` Nullable(Float64),
        `vanna` Nullable(Float64),
        `vomma` Nullable(Float64),
        `charm` Nullable(Float64),
        `forward_method` String,
        `price_type` String,
        `frequency` String,
        `market` String,
        `model_id` String,
        `model_version` String,
        `ingest_time` DateTime64(3) DEFAULT now64(3)
    )
    ENGINE = ReplacingMergeTree(ingest_time)
    PARTITION BY toYYYYMM(`date`)
    ORDER BY (`date`, `order_book_id`, `model_id`, `model_version`)
    """


def create_calculated_option_greeks_tables(
    database_name: str = "rq_option_data",
) -> None:
    execute_clickhouse_sql(
        build_calculated_option_greeks_daily_table_sql(),
        database=database_name,
    )


def create_option_specific_tables(database_name: str = "rq_option_data") -> None:
    execute_sql(build_option_instruments_table_sql(), database=database_name)


def create_rq_options_data(database_name: str = "rq_option_data") -> None:
    """
    创建 options 相关全部表：
    - 通用价格表 option_price_*
    - options 专属表 option_instruments
    """
    create_mysql_database_if_not_exists(database_name)
    create_clickhouse_database_if_not_exists(database_name)

    # 高频 / 时序 price 表走 ClickHouse
    create_option_price_tables(database_name=database_name)
    create_calculated_option_greeks_tables(database_name=database_name)

    create_option_greeks_tables(database_name=database_name)

    # option 元数据表走 MySQL
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
    create_mysql_database_if_not_exists(database_name)
    create_clickhouse_database_if_not_exists(database_name)

    # 时序 price 表走 ClickHouse
    create_index_price_tables(database_name=database_name)

    # index 元数据表走 MySQL
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
    create_mysql_database_if_not_exists(database_name)
    create_clickhouse_database_if_not_exists(database_name)

    create_cn_stock_price_tables(database_name=database_name)

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
    create_mysql_database_if_not_exists(database_name)
    create_clickhouse_database_if_not_exists(database_name)

    create_etf_price_tables(database_name=database_name)

    create_etf_specific_tables(database_name=database_name)

def build_trading_dates_table_sql(table_name: str = "trading_dates") -> str:
    partition_sql = build_year_range_partitions_sql("trading_date")

    return f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `trading_date` DATE NOT NULL COMMENT '交易日',
        `market` VARCHAR(8) NOT NULL DEFAULT 'cn' COMMENT '市场',

        `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

        PRIMARY KEY (`market`, `trading_date`),
        KEY `idx_trading_date` (`trading_date`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    {partition_sql};
    """
def create_rq_data_common_tables(database_name: str = "rq_data") -> None:
    create_mysql_database_if_not_exists(database_name)
    execute_sql(build_trading_dates_table_sql(), database=database_name)
# ============================================================
# Global init
# ============================================================

def init_rq_db() -> None:
    create_rq_base_databases()
    create_rq_data_common_tables()
    create_rq_futures_data()
    create_rq_options_data()
    create_rq_index_data()
    create_rq_cn_stock_data()
    create_rq_etf_data()


def rebuild_rq_databases() -> None:
    for db in sorted(RQ_DATABASES):
        drop_clickhouse_database_if_exists(db)
        drop_mysql_database_if_exists(db)

    init_rq_db()

def rebuild_all_clickhouse_tables() -> None:
    """
    删除并重建所有 ClickHouse 表。

    注意：
    1. 会删除原有 ClickHouse 表及其中全部数据
    2. 不影响 MySQL 的 instruments / metadata 表
    3. 依赖前面的 ClickHouse 建表函数已经改成 ReplacingMergeTree(ingest_time)
    """

    # 确保数据库存在
    for db in RQ_DATABASES:
        create_clickhouse_database_if_not_exists(db)

    # 需要重建的 ClickHouse price 表
    table_groups = [
        ("rq_future_data", "future_price", create_future_price_tables),
        ("rq_option_data", "option_price", create_option_price_tables),
        ("rq_index_data", "index_price", create_index_price_tables),
        ("rq_stock_data", "stock_price", create_cn_stock_price_tables),
        ("rq_etf_data", "etf_price", create_etf_price_tables),
    ]

    # 先删除所有 price 表
    for database_name, table_prefix, _create_tables in table_groups:
        for freq in DAILY_FREQUENCIES:
            table_name = f"{table_prefix}_{freq}"
            execute_clickhouse_sql(
                f"DROP TABLE IF EXISTS `{table_name}`",
                database=database_name,
            )

        for freq in MINUTE_FREQUENCIES:
            table_name = f"{table_prefix}_{freq}"
            execute_clickhouse_sql(
                f"DROP TABLE IF EXISTS `{table_name}`",
                database=database_name,
            )

    # 删除 option greeks 表
    execute_clickhouse_sql(
        "DROP TABLE IF EXISTS `option_greeks_1d`",
        database="rq_option_data",
    )
    execute_clickhouse_sql(
        "DROP TABLE IF EXISTS `option_greeks_1m`",
        database="rq_option_data",
    )

    # 重建所有 price 表
    for database_name, _table_prefix, create_tables in table_groups:
        create_tables(database_name=database_name)

    # 重建 option greeks 表
    create_option_greeks_tables(database_name="rq_option_data")

if __name__ == "__main__":
    load_env()
    init_rq_db()
    print("RiceQuant MySQL metadata and ClickHouse timeseries databases initialized successfully.")
