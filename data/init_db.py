# 此文档用于创造数据库和表
import pymysql
from basis import constant as cs

# 获取数据库的信息，包括用户名、密码等
db = cs.Database_info()
conn = pymysql.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd)
cursor = conn.cursor()


# 用于创造股票基础数据库和里面的表
# 用于创造股票基础数据库和里面的表
def create_HKEX():
    cursor.execute("CREATE DATABASE IF NOT EXISTS HKEX")  # 创建基础数据库
    cursor.execute("use HKEX")

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS mhi_tick
        (code VARCHAR(20),time TIMESTAMP,price DECIMAL(17,6),volume DECIMAL(8,2),turnover DECIMAL(10,2),
            ticker_direction VARCHAR(7),type VARCHAR(20),push_data_type VARCHAR(20)
            )
        """
    )

    k_tables = ["1m", "5m", "15m", "30m", "1h", "2h", "3h", "4h"]

    for table in k_tables:
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {table}
            (code VARCHAR(20),trade_time TIMESTAMP,open	DECIMAL(17,6),close	DECIMAL(17,6),high DECIMAL(17,6),
                low DECIMAL(17,6),volume DECIMAL(15,2),	turnover DECIMAL(15,2),last_close DECIMAL(17,6)
                )
                """
        )

    conn.commit()


if __name__ == '__main__':
    create_HKEX()
    '''
    cursor.execute("drop database log_info ")
    cursor.execute("drop database  HKEX")


    '''
