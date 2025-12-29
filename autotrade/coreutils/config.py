from types import SimpleNamespace
from dotenv import load_dotenv
import os

# 加载 .env 文件
load_dotenv()

def load_env(env_path: str | Path | None = None) -> None:
    """
    显式加载 .env（只加载一次）
    """
    global _ENV_LOADED
    if _ENV_LOADED:
        return

    if env_path is not None:
        env_file = Path(env_path).expanduser().resolve()
        if not env_file.exists():
            raise FileNotFoundError(f".env not found: {env_file}")
        load_dotenv(env_file)
    else:
        # 默认：项目根目录 .env
        base_dir = Path(__file__).resolve().parents[2]
        load_dotenv(base_dir / ".env")

    _ENV_LOADED = True


def _ensure_env_loaded():
    """
    兜底：如果用户没手动 load_env，就自动加载默认 .env
    """
    if not _ENV_LOADED:
        load_env()
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
