# data/subscribe_kline.py
class SubscribeKline:
    """
    SubscribeKline
    ================================
    【作用】
    - 数据层封装，策略通过该类订阅指定数据源（如 FUTU）的 K 线或 Tick 数据。
    - 将策略层与具体数据网关解耦，提供统一的订阅接口。

    【为什么这么设计】
    - 避免策略直接依赖 gateway 实现（如 TickStream 或 KlineGatewaySub）。
    - 后续如果更换数据源（如 ZMQ、CTP），只需修改 data 层逻辑，而策略代码保持不变。

    【怎么用】
    >>> def on_data(df):
    >>>     print("[回调] 最新数据:", df)
    >>> # 订阅 15m K 线
    >>> sub_kline = SubscribeKline(code="HK.MHImian", data_source="FUTU", ktypes="15m")
    >>> sub_kline.subscribe(on_data)
    >>> # 订阅 Tick
    >>> sub_tick = SubscribeKline(code="HK.MHImian", data_source="FUTU", ktypes="tick")
    >>> sub_tick.subscribe(on_data)
    """

    def __init__(self, code, data_source, ktypes):
        """
        初始化 SubscribeKline 实例。

        参数:
        ----------
        code : str
            标的代码，例如 "HK.MHImian"
        data_source : str
            数据源标识，目前仅支持 "FUTU"
        ktypes : str
            数据类型或周期：
            - 'tick' 表示订阅逐笔成交数据
            - 其他如 '15m'、'1h' 表示订阅对应周期的 K 线数据
        """
        self.code = code
        self.ktypes = ktypes

        # 根据数据源选择对应 gateway 实现（目前仅支持 FUTU）
        if data_source == "FUTU":
            from gateway.futu_gateway import futu_data as gateway

            # 如果订阅 Tick，则使用 TickStream
            if ktypes == "tick":
                self.gateway = gateway.TickStream(self.code)
            else:
                # 否则订阅 K 线数据（通过 KlineGatewaySub）
                self.gateway = gateway.KlineGatewaySub(self.code, self.ktypes)
        else:
            raise Exception("data_source目前只有FUTU")

    def subscribe(self, callback):
        """
        注册策略回调函数。

        参数:
        ----------
        callback : function
            回调函数，签名 callback(df)，df 是最新的数据（pandas.DataFrame）。
        """
        self.gateway.subscribe(callback)
