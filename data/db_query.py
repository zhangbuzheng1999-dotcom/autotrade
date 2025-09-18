# 此代码用于从本地数据库中查询数据
# get_engine 用于缓存数据库连接池，避免重复创建连接。
# 新增数据源时，无需修改 get_engine 的逻辑，只需在 db_dict 中配置对应的库名和表名映射即可。
import pandas as pd
from sqlalchemy import create_engine
from coreutils.config import DatabaseInfo
_engine_cache = {}


def get_engine(user, passwd, host, port, db_name):
    key = f"{host}:{port}/{db_name}"
    if key not in _engine_cache:
        print(f"[连接池创建] {key}")
        engine = create_engine(
            f"mysql+pymysql://{user}:{passwd}@{host}:{port}/{db_name}",
            pool_size=15,
            pool_recycle=1600,
            pool_pre_ping=True,
            max_overflow=5
        )
        _engine_cache[key] = engine
    return _engine_cache[key]


def fetch_kline(db_name, table_name, start_date=None, end_date=None, limit=10, sql_code=None):
    """
    主动从数据库查询指定证券代码的 K线数据

    参数:
    --------
    code: str             # 证券代码，例如 'HK.MHImain'
    ktype: str             # K线周期，例如 '15m'
    start_date: str       # 起始时间（可选）
    end_date: str         # 截止时间（可选）
    limit: int            # 当start_date和end_date为空，默认从当前日期往前获取limit条数据
    sql_code: str         # 可选，自定义 SQL 查询语句

    返回:
    --------
    pd.DataFrame: 查询结果，按 trade_time 升序
    """
    engine = get_engine(DatabaseInfo.user, DatabaseInfo.password,
                        DatabaseInfo.host, DatabaseInfo.port, db_name)

    # 用户自定义 SQL 优先
    if sql_code:
        return pd.read_sql_query(sql_code, engine)

    # 构建动态 WHERE 语句
    where_clauses = []
    params = {}

    if start_date:
        where_clauses.append("trade_time >= %(start)s")
        params['start'] = start_date
    if end_date:
        where_clauses.append("trade_time <= %(end)s")
        params['end'] = end_date

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    query_code = f"""
        SELECT * FROM (
            SELECT * FROM {table_name}
            {where_sql}
            ORDER BY trade_time DESC
            LIMIT {limit}
        ) AS da
        ORDER BY trade_time ASC;
    """

    return pd.read_sql_query(query_code, engine, params=params)

if __name__ == '__main__':
    data = fetch_kline(db_name='HKEX', table_name='mhi_15m', end_date='2025-01-01', limit=10)
    print(data)
