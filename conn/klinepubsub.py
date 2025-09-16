"""
该模块用于 K 线数据的发布与订阅，适用于需要模块间传输自撮合后的 K 线的场景。
例如：query_mhimain 模块等。
- 使用 ZeroMQ 的 PUB/SUB 模式实现轻量级的进程间通信
- 发布与接收的数据格式为 pandas.DataFrame（JSON 序列化）
- 支持多频率（如 '5m', '15m', '1h' 等）的 Topic 区分
"""
import traceback
from threading import Thread
from queue import Queue
import zmq
import pandas as pd
from io import StringIO
import threading
from coreutils.constant import Interval

class PubKline(Thread):
    """
    K线发布器模块（线程实现）：

    - 用于将处理好的 K 线数据以指定 topic 的形式发布出去（如 "1m", "5m"）
    - 使用 ZeroMQ 的 PUB socket 作为传输通道，适用于跨模块推送数据
    - 支持多频率的 K 线同时发布（例如 '1m' 与 '5m' 可并发推送）

    注意：
    - ZeroMQ 的 PUB socket **不是线程安全的**，不能多个线程同时调用 `.send_string()`
    - 为了解决这个问题，我们使用 `queue.Queue` 作为安全的消息缓冲队列
      所有待发送数据先放入队列，由单独线程串行处理，避免冲突或数据丢失
    """

    def __init__(self, ip: str, port: int):
        """
        初始化发布器线程，并绑定到指定 IP 和端口

        :param ip: 绑定的 IP 地址（如 "127.0.0.1"）
        :param port: 端口号（如 5555）
        """
        super().__init__(name=f"PubKline-{ip}:{port}", daemon=True)

        # 创建 ZeroMQ PUB socket 并绑定地址
        context = zmq.Context()
        self.publisher = context.socket(zmq.PUB)
        self.publisher.bind(f"tcp://{ip}:{port}")

        # 消息发布队列，支持并发写入
        self._pubqueue = Queue()

        # 启动线程，异步处理消息发送
        self.start()

    def run(self):
        """
        后台线程运行逻辑：
        - 持续从队列中取出待发布消息
        - 将其转换为 JSON 格式并通过 ZMQ 发布
        """
        while True:
            try:
                mes = self._pubqueue.get()  # 阻塞等待新任务
                data = mes['data']
                ktype = mes['ktype']

                # 转换 DataFrame 为 JSON 字符串，格式为 topic + payload
                json_str = data.to_json(orient='records', indent=2)
                self.publisher.send_string(f"{ktype} {json_str}")

            except Exception as e:
                print(f"[Publisher] Error publishing K-line: {e}")

    def pubkline(self, data: pd.DataFrame, ktype: Interval):
        """
        发送一条 K 线数据（非阻塞，立即放入队列）

        :param data: 要发布的 K 线 pandas.DataFrame 数据
        :param ktype: K 线周期（如 '1m'、'5m'、'1h' 等），作为 ZeroMQ 的 topic
        """
        # 将数据打包为 dict，加入队列等待发布线程处理
        self._pubqueue.put({'data': data, 'ktype': ktype.value})


class SubKline:
    """
    【模块用途】
    - 这是系统最底层的数据接收模块之一，用于从行情推送服务(ZMQ PUB端)订阅指定频率的K线数据。
    - 实现了事件驱动的数据分发机制：当新的K线数据到达时，会触发注册的回调函数。
    - 还维护了最近一条K线数据，可通过 get() 方法主动获取。

    【设计特点】
    - 使用 ZeroMQ (ZMQ) 的 SUB 模式，订阅服务端的K线推送数据。
    - 基于多线程：数据接收在后台线程运行，不会阻塞主线程。
    - 支持“回调注册 + 异步触发”，是策略层事件驱动设计的核心基础。
    """

    def __init__(self, ip: str, port: int, ktype: str):
        """
        初始化订阅器，连接ZMQ数据源并启动接收线程。

        参数:
        ----------
        ip : str
            ZMQ 服务端的 IP 地址，例如 '127.0.0.1'
        port : int
            ZMQ 服务端端口号
        ktype : str
            订阅的K线周期，例如 '1m'、'5m'、'15m'、'1h'

        内部逻辑:
        ----------
        - 初始化 ZeroMQ SUB 套接字，并连接到指定地址。
        - 设置订阅过滤器，只接收指定 ktype 的数据。
        - 初始化回调列表和最近数据缓存。
        - 启动接收线程，监听数据推送并分发给回调函数。
        """

        # 保存K线类型，用于日志打印和标识
        self.ktype = ktype

        # 最近一次接收到的DataFrame数据，供 get() 方法访问
        self.last_df = None

        # 回调函数列表，当有新数据时依次调用这些函数
        self.callbacks = []

        # 创建ZMQ上下文对象（通信环境）
        context = zmq.Context()

        # 创建订阅socket（SUB模式）
        self.socket = context.socket(zmq.SUB)

        # 设置断线重连间隔（毫秒），确保连接断开后自动重连
        self.socket.setsockopt(zmq.RECONNECT_IVL, 2000)

        # 连接到ZMQ发布端
        self.socket.connect(f"tcp://{ip}:{port}")

        # 设置订阅主题过滤，只接收指定k线类型的数据
        self.socket.setsockopt_string(zmq.SUBSCRIBE, ktype)

        # 启动后台线程，异步接收数据
        self._start_thread()

    def subscribe(self, callback):
        """
        注册回调函数，当接收到新的K线数据时调用。

        参数:
        ----------
        callback : function
            策略或上层逻辑提供的回调函数，函数签名必须是 callback(df)，
            其中 df 是最新一条K线数据（pandas.DataFrame）。
        """
        self.callbacks.append(callback)

    def get(self):
        """
        获取最近一次接收到的K线数据（DataFrame）。
        注意:
        ----------
        - 这是主动拉取模式，通常用于调试或在特定逻辑中临时取值。
        - 如果系统采用回调机制，则通常不需要频繁调用此方法。
        """
        return self.last_df

    def _start_thread(self):
        """
        启动后台线程，负责持续监听ZMQ推送的K线数据。
        接收逻辑:
        ----------
        - 使用 socket.recv_string() 接收字符串消息，格式为: "<ktype> <payload>"。
        - 通过 split() 分离出 payload（JSON格式）。
        - 将JSON解析为 pandas.DataFrame 对象。
        - 更新 last_df 缓存，并触发所有注册的回调函数。
        - 任何异常都会被捕获并打印，不会导致线程退出。
        """

        def run():
            print(f"[INFO] {self.ktype} 订阅器已启动...")
            while True:
                try:
                    # 接收一条ZMQ消息（阻塞等待）
                    msg = self.socket.recv_string()

                    # 按空格拆分，去掉主题部分
                    _, payload = msg.split(" ", 1)

                    # 将JSON字符串转换为DataFrame
                    df = pd.read_json(StringIO(payload), orient="records")
                    df['ktype'] = self.ktype
                    # 缓存最新的DataFrame
                    self.last_df = df

                    # 遍历回调列表，逐个调用（事件分发）
                    for cb in self.callbacks:
                        cb(df)

                except Exception as e:
                    # 出现任何异常都打印错误，但不中断线程
                    print(f"[ERROR] 接收数据异常: {traceback.format_exc()}")

        # 启动守护线程，保证程序退出时线程自动结束
        threading.Thread(target=run, daemon=True).start()


class MultiKlineSubscriber:
    """
    单线程+分发版：一个ZMQ SUB socket订阅多个周期，并根据topic分发数据
    """

    def __init__(self, ip: str, port: int, ktypes: list[Interval]):
        ktypes = [str(k.value) for k in ktypes]
        self.callbacks = {k: [] for k in ktypes}
        self.last_data = {}
        # 创建 ZMQ 上下文和 socket
        context = zmq.Context()
        self.socket = context.socket(zmq.SUB)
        self.socket.setsockopt(zmq.RECONNECT_IVL, 2000)
        self.socket.connect(f"tcp://{ip}:{port}")
        # 订阅多个topic（周期）
        for k in ktypes:
            self.socket.setsockopt_string(zmq.SUBSCRIBE, k)

        # 启动一个线程监听所有topic
        self._start_thread()

    def subscribe(self, ktype: Interval, callback):
        ktype = str(ktype.value)
        """
        注册回调函数
        """
        if ktype not in self.callbacks:
            raise ValueError(f"未订阅的周期: {ktype}")
        self.callbacks[ktype].append(callback)

    def get(self, ktype: str):
        """
        获取最近一次K线数据
        """
        return self.last_data.get(ktype)

    def _start_thread(self):
        def run():
            print("[INFO] 多周期订阅器已启动...")
            while True:
                try:
                    msg = self.socket.recv_string()
                    topic, payload = msg.split(" ", 1)
                    df = pd.read_json(StringIO(payload), orient="records")

                    # 缓存最新数据
                    self.last_data[topic] = df
                    # 触发回调
                    for cb in self.callbacks.get(topic, []):
                        cb(df)
                except Exception as e:
                    print(f"[ERROR] 接收数据异常: {e}")

        threading.Thread(target=run, daemon=True).start()


