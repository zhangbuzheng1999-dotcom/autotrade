from pathlib import Path
from dotenv import load_dotenv, find_dotenv
import os
import warnings

# ===================== 内部状态 =====================
_ENV_LOADED = False


def load_env(env_path=None):
    """
    Load environment variables with priority:

    1. Explicit env_path (override=True)
    2. Project .env auto-discovered from current working directory upward
    3. Global APP_ENV_FILE
    4. Fallback to system environment variables only

    Notes:
    - Explicit env_path will override existing env vars.
    - Auto-discovered/project/global .env will NOT override existing env vars.
    """
    global _ENV_LOADED

    if _ENV_LOADED and env_path is None:
        return

    # 1) Explicit env_path
    if env_path is not None:
        env_file = Path(env_path).expanduser().resolve()
        if not env_file.exists():
            raise FileNotFoundError(f".env not found: {env_file}")
        load_dotenv(env_file, override=True)
        _ENV_LOADED = True
        return

    # 2) Auto find .env from current working directory upward
    env_file = find_dotenv(usecwd=True)
    if env_file:
        load_dotenv(env_file, override=False)
        _ENV_LOADED = True
        return

    # 3) Global env file via APP_ENV_FILE
    global_env = os.getenv("APP_ENV_FILE")
    if global_env:
        env_file = Path(global_env).expanduser().resolve()
        if env_file.exists():
            load_dotenv(env_file, override=False)
            _ENV_LOADED = True
            return

    # 4) Warning only
    warnings.warn(
        "[config] No .env file found. "
        "Please place a .env file in the project root directory, "
        "or explicitly call:\n"
        "    from autotrade.coreutils.config import load_env\n"
        "    load_env('xxx/.env')\n"
        "or set system environment variable APP_ENV_FILE.",
        RuntimeWarning,
    )
    _ENV_LOADED = True


def _get_int_env(key: str, default: int) -> int:
    value = os.getenv(key, "")
    if value == "":
        return default
    try:
        return int(value)
    except ValueError as e:
        raise ValueError(f"Environment variable {key!r} must be an integer, got {value!r}") from e


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
            return _get_int_env("DB_PORT", 3306)
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
            if name in ("retry_times", "retry_gap"):
                return _get_int_env(mapping[name], 0)
            return os.getenv(mapping[name], "")

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
            if name == "port":
                return _get_int_env(mapping[name], 11111)
            return os.getenv(mapping[name], "")

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


# ===================== 对外稳定 API（兼容旧代码） =====================
DatabaseInfo = _DatabaseInfoProxy()
serverjiang = _ServerJiangProxy()
LinuxServer = _LinuxServerProxy()
FutuInfo = _FutuInfoProxy()
TushareInfo = _TushareProxy()

__all__ = [
    "load_env",
    "DatabaseInfo",
    "serverjiang",
    "LinuxServer",
    "FutuInfo",
    "TushareInfo",
]
