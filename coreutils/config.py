from types import SimpleNamespace
from dotenv import load_dotenv
import os

# 加载 .env 文件
load_dotenv()

# ===================== 数据库信息 =====================
DatabaseInfo = SimpleNamespace(
    host=os.getenv("DB_HOST", "127.0.0.1"),
    port=int(os.getenv("DB_PORT", 3306)),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASSWORD", "")
)

# ===================== 常量设置 =====================
serverjiang = SimpleNamespace(
    proToken=os.getenv("SERVERJIANG_PROTOKEN", ""),
    dbPath=os.getenv("SERVERJIANG_DBPATH", ""),
    retry_times=int(os.getenv("SERVERJIANG_RETRY_TIMES", 2)),
    retry_gap=int(os.getenv("SERVERJIANG_RETRY_GAP", 61)),
    serverjiang=os.getenv("SERVERJIANG_SERVERJIANG", "")
)

# ===================== Linux 服务器配置 =====================
LinuxServer = SimpleNamespace(
    myHostname=os.getenv("LINUX_HOSTNAME", ""),
    myUsername=os.getenv("LINUX_USERNAME", "root"),
    myPassword=os.getenv("LINUX_PASSWORD", "")
)

# ===================== Futu OpenD 配置 =====================
FutuInfo = SimpleNamespace(
    host=os.getenv("FUTU_HOST", "127.0.0.1"),
    port=int(os.getenv("FUTU_PORT", 11111)),
    pwd_unlock=os.getenv("FUTU_PWD_UNLOCK", "")
)
