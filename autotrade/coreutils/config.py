from pathlib import Path
from dotenv import load_dotenv
import os

# ===================== 内部状态 =====================
_ENV_LOADED = False


def load_env(env_path=None):
    """
    Load environment variables.

    Priority:
    1. Explicit env_path (override=True)
    2. Default project root .env (override=False)
    """
    global _ENV_LOADED

    if env_path is not None:
        env_file = Path(env_path).expanduser().resolve()
        if not env_file.exists():
            raise FileNotFoundError(f".env not found: {env_file}")
        load_dotenv(env_file, override=True)
        _ENV_LOADED = True
        return

    if not _ENV_LOADED:
        base_dir = Path(__file__).resolve().parents[2]
        load_dotenv(base_dir / ".env", override=False)
        _ENV_LOADED = True


class _DatabaseInfoProxy:
    """
    Dynamic proxy for database config.
    Always reflects latest environment variables.
    """

    def __getattr__(self, name: str):
        load_env()

        if name == "host":
            return os.getenv("DB_HOST", "127.0.0.1")
        if name == "port":
            return int(os.getenv("DB_PORT", 3306))
        if name == "user":
            return os.getenv("DB_USER", "root")
        if name == "password":
            return os.getenv("DB_PASSWORD", "")

        raise AttributeError(name)


class _ServerJiangProxy:
    def __getattr__(self, name: str):
        load_env()

        mapping = {
            "proToken": "SERVERJIANG_PROTOKEN",
            "dbPath": "SERVERJIANG_DBPATH",
            "retry_times": "SERVERJIANG_RETRY_TIMES",
            "retry_gap": "SERVERJIANG_RETRY_GAP",
            "serverjiang": "SERVERJIANG_SERVERJIANG",
        }

        if name in mapping:
            value = os.getenv(mapping[name], "")
            if name in ("retry_times", "retry_gap"):
                return int(value or 0)
            return value

        raise AttributeError(name)


class _LinuxServerProxy:
    def __getattr__(self, name: str):
        load_env()

        mapping = {
            "myHostname": "LINUX_HOSTNAME",
            "myUsername": "LINUX_USERNAME",
            "myPassword": "LINUX_PASSWORD",
        }

        if name in mapping:
            return os.getenv(mapping[name], "")

        raise AttributeError(name)


class _FutuInfoProxy:
    def __getattr__(self, name: str):
        load_env()

        mapping = {
            "host": "FUTU_HOST",
            "port": "FUTU_PORT",
            "pwd_unlock": "FUTU_PWD_UNLOCK",
        }

        if name in mapping:
            value = os.getenv(mapping[name], "")
            if name == "port":
                return int(value or 11111)
            return value

        raise AttributeError(name)

class _TushareProxy:
    """
    Dynamic proxy for Tushare configuration.
    """

    def __getattr__(self, name: str):
        load_env()

        if name == "token":
            return os.getenv("TUSHARE_TOKEN", "")

        raise AttributeError(name)

# ===================== 对外稳定 API（关键） =====================
DatabaseInfo = _DatabaseInfoProxy()
serverjiang = _ServerJiangProxy()
LinuxServer = _LinuxServerProxy()
FutuInfo = _FutuInfoProxy()
TushareInfo = _TushareProxy()
