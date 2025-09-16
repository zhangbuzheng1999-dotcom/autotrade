from types import SimpleNamespace

# ===================== 数据库信息 =====================
DatabaseInfo = SimpleNamespace(
    host='127.0.0.1',
    port=3306,
    user='root',
    password='xxxxx'  # 建议生产环境不要明文存储密码
)

# ===================== 常量设置 =====================
serverjiang = SimpleNamespace(
    proToken="xxxxx",
    dbPath="xxxxx",
    retry_times=2,
    retry_gap=61,
    serverjiang="xxx"
)

# ===================== Linux 服务器配置 =====================
LinuxServer = SimpleNamespace(
    myHostname="xxx",
    myUsername="root",
    myPassword="xxx"  # 建议用 .env 或 secrets.json 存放敏感信息
)

# ===================== Futu OpenD 配置 =====================
FutuInfo = SimpleNamespace(
    host='127.0.0.1',
    port=11111,  # 注意此端口应为 Futu OpenD 的端口（常见为 11111）
    pwd_unlock='xxxx'
)




