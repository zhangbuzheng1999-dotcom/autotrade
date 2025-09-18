"""
日志模块封装：get_logger()

功能特性：
-----------
- 每天自动切分日志文件，保留最近 30 天
- 同时支持写入文件和控制台输出
- 自定义日志名、日志等级
- 扩展了 logger.wechat(...) 方法，可将日志主动推送到微信（如 Server酱）

用法示例：
-----------
# 初始化日志器（一次即可）
from logger import get_logger

logger = get_logger("trade", logfile="trade.log")

# 只记录日志
logger.info("策略启动")
logger.warning("持仓较重")

# 记录日志+推送微信通知
logger.wechat("仓位过高", "当前持仓为 92%，建议减仓")
# 也可省略 content，直接 title 显示
logger.wechat("策略启动成功")

"""

import logging
import os
import requests
from coreutils.config import serverjiang
from logging.handlers import TimedRotatingFileHandler
from engine.event_engine import EVENT_LOG, Event, EventEngine
from coreutils.constant import LogLevel
from coreutils.object import LogData
from pathlib import Path


def get_logger(name: str = 'main', logfile: str = 'run.log',
               level=logging.INFO, to_console: bool = True):
    """
    获取一个 logger 实例，支持日志文件切分，并扩展 .wechat(title, content) 推送功能。

    参数:
    ----------
    name : str
        日志器名称，建议传模块名（默认 'main'）

    logfile : str
        日志文件名（保存在 logs/ 目录下），默认 'run.log'

    level : int
        日志等级，默认 logging.INFO。可选：
        - logging.DEBUG
        - logging.INFO
        - logging.WARNING
        - logging.ERROR
        - logging.CRITICAL

    to_console : bool
        是否同步输出到控制台，默认 True

    返回:
    ----------
    logging.Logger 实例，包含额外扩展方法 logger.wechat(title, content)
    """

    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    logfile_path = os.path.join(log_dir, logfile)

    logger = logging.getLogger(name)

    # 防止重复添加 handler
    if logger.handlers:
        return logger

    logger.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    # 日志文件 handler（按天切分，保留30天）
    file_handler = TimedRotatingFileHandler(
        filename=logfile_path,
        when='D',
        interval=1,
        backupCount=30,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 控制台输出 handler（可选）
    if to_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # 添加扩展方法 logger.wechat(...)
    def wechat_log(self, title=None, content=None, level="info", channel=1):
        """
        推送一条日志到微信（Server酱）+ 写入日志文件

        参数:
        ----------
        title : str
            微信推送标题（必须），将显示为通知标题

        content : str
            推送正文内容（如果为空则使用 title 代替）

        level : str
            本地日志等级，可为 "info", "warning", "error", "debug" 等

        channel : int
            推送渠道编号（用于支持多通道服务，默认 1）
        """
        if title is None:
            title = " "
        msg = content if content else title

        # 写入日志
        if hasattr(self, level):
            getattr(self, level)(msg)

        # 推送微信
        try:
            data = {
                "title": title,
                "desp": msg,
                "channel": channel
            }
            requests.post(serverjiang.serverjiang, data=data, timeout=5)
        except Exception as e:
            self.error(f"[微信推送失败] {e}", exc_info=True)

    # 动态绑定方法到 logger 实例
    logger.wechat = wechat_log.__get__(logger)

    return logger


class LoggerEngine:
    def __init__(self, event_engine, engine_id, LOG_DIR=None,to_console=True):
        self.event_engine = event_engine
        if LOG_DIR is None:
            BASE_DIR = self.get_base_dir()
            LOG_DIR = str((BASE_DIR / "logs" / f'{engine_id}.log').resolve())  # 日志最好放在/autotrade/logs里面，方便app读取
            print(f'LOG_DIR: {LOG_DIR}')

        self.logger = get_logger(name=engine_id, logfile=LOG_DIR,to_console=to_console)
        self.event_engine.register(EVENT_LOG, self._on_log)

    # 日志模块
    def _on_log(self, event: Event):
        log_data: LogData = event.data
        log_level = log_data.level
        msg = log_data.msg
        if log_level == LogLevel.DEBUG:
            self.process_debug(msg)
        elif log_level == LogLevel.INFO:
            self.process_info(msg)
        elif log_level == LogLevel.WARNING:
            self.process_warning(msg)
        elif log_level == LogLevel.ERROR:
            self.process_error(msg)

    def process_debug(self, msg: str):
        self.logger.debug(msg)

    def process_info(self, msg: str):
        self.logger.info(msg)

    def process_warning(self, msg: str):
        self.logger.warning(msg)

    def process_error(self, msg: str):
        self.logger.error(msg)

    @staticmethod
    def get_base_dir():
        if "__file__" in globals():
            # 普通脚本运行: __file__ 存在
            return Path(__file__).resolve().parent.parent
        else:
            # 交互模式 (Jupyter/IPython): 用当前工作目录
            return Path().resolve()
