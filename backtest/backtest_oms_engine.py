from coreutils.object import TradeData, AccountData, PositionData
from coreutils.constant import Direction, OrderStatus
from backtest.backtest_event_engine import BacktestEventEngine
from engine.event_engine import Event
from engine.oms_engine import OmsBase
from copy import deepcopy


class BacktestOms(OmsBase):
    """
    回测专用 OMS（含保证金逻辑）
    - 事件驱动：接收 Trade 事件更新仓位和资金
    - 支持卖空、翻仓、均价计算、保证金管理
    """

    def __init__(self, event_engine: BacktestEventEngine = BacktestEventEngine(), initial_cash: float = 1_000_000):
        super().__init__(event_engine)

        self.gateway_name = 'BACKTEST'

        # 账户情况
        self.accounts['BACKTEST'] = AccountData(self.gateway_name, accountid='BACKTEST')
        self.accounts['BACKTEST'].cash = initial_cash
        self.accounts['BACKTEST'].available = initial_cash
        self.accounts['BACKTEST'].equity = initial_cash

        # 仓位与资金
        self.contracts_log = {}
        self.trade_log: list[TradeData] = []

        # 合约参数
        self.sizes: dict[str, float] = {}
        self.long_rates: dict[str, float] = {}
        self.short_rates: dict[str, float] = {}
        self.margin_rates: dict[str, float] = {}  # 保证金率

    def set_contract_params(
            self, symbol: str, size: float = 1, long_rate: float = 0, short_rate: float = 0, margin_rate: float = 0
    ):
        """设置合约参数（含保证金率）"""
        self.sizes[symbol] = size
        self.long_rates[symbol] = long_rate
        self.short_rates[symbol] = short_rate
        self.margin_rates[symbol] = margin_rate

    def process_trade_event(self, event: Event):
        # 原生oms
        trade: TradeData = event.data
        self.trades[trade.vt_tradeid] = trade

        symbol = trade.symbol
        size = self.sizes.get(symbol, 1)
        margin_rate = self.margin_rates.get(symbol, 0.1)
        cost = trade.price * trade.volume * size
        exchange = trade.exchange
        commission = cost * (
            self.long_rates.get(symbol, 0) if trade.direction == Direction.LONG else self.short_rates.get(symbol,
                                                                                                          0))

        pos = self.positions.setdefault(symbol,
                                        PositionData(gateway_name=self.gateway_name, symbol=symbol, exchange=exchange,
                                                     direction=Direction.NET, volume=0, price=0, margin=0))

        # 无论开平都要扣手续费
        self.accounts['BACKTEST'].cash -= commission

        # 获取旧仓位
        old_volume, old_price, old_margin = pos.volume, pos.price, pos.margin

        # 新交易信息
        new_volume = abs(trade.volume) if trade.direction == Direction.LONG else -abs(trade.volume)
        new_price = trade.price
        turnover = abs(new_volume) * new_price * size

        realized_pnl = 0.0

        # 情况一：方向一致（加仓）
        if old_volume * new_volume > 0:
            volume = old_volume + new_volume
            price = (old_price * abs(old_volume) + new_price * abs(new_volume)) / abs(volume)

        # 情况二：方向相反（平仓或反手）
        else:
            close_qty = min(abs(old_volume), abs(new_volume))
            if old_volume > 0:
                realized_pnl = (new_price - old_price) * close_qty * size
            else:
                realized_pnl = (old_price - new_price) * close_qty * size

            self.accounts['BACKTEST'].cash += realized_pnl
            volume = old_volume + new_volume
            if abs(new_volume) < abs(old_volume):  # 部分平仓，保持原均价
                price = old_price
            else:  # 完全反手，新开仓
                price = new_price

        # 更新仓位
        if volume != 0:
            margin = abs(volume) * trade.price * size * margin_rate
            pos.margin = margin
            pos.volume = volume
            pos.price = price

        else:
            margin = 0
            self.positions.pop(symbol, None)

        # 重新计算总保证金
        self.accounts['BACKTEST'].margin = sum(p.margin for p in self.positions.values())

        # 更新账户指标
        self.accounts['BACKTEST'].realized_pnl += realized_pnl
        self.accounts['BACKTEST'].equity = self.accounts['BACKTEST'].cash  # 暂不加浮盈
        self.accounts['BACKTEST'].available = self.accounts['BACKTEST'].cash + self.accounts[
            'BACKTEST'].unrealized_pnl - self.accounts['BACKTEST'].margin

        # 更新contract指标
        contract_info = self.contracts_log.setdefault(symbol, {"volume": 0, "margin": 0, "realized_pnl": 0.0,
                                                               "unrealized_pnl": 0.0,
                                                               "cost": 0.0,
                                                               "turnover": 0.0, })
        contract_info["volume"] = volume
        contract_info["margin"] = margin
        contract_info["realized_pnl"] += realized_pnl
        contract_info["cost"] += cost
        contract_info["turnover"] += turnover
        # 如果平仓重置浮盈
        if volume == 0:
            contract_info["unrealized_pnl"] = 0
        # 记录交易
        self.trade_log.append(trade)

    def get_contract_log(self):
        return deepcopy(self.contracts_log)

    def renew_unrealized_pnl(self, last_prices: dict[str, float]):
        """总权益 = 可用现金 + 占用保证金 + 持仓浮盈"""
        self.accounts['BACKTEST'].unrealized_pnl = 0.0
        self.accounts['BACKTEST'].equity = self.accounts['BACKTEST'].cash
        for symbol, pos in self.positions.items():
            if pos.volume != 0:
                size = self.sizes.get(symbol, 1)
                last_price = last_prices.get(symbol, pos.price)
                float_pnl = (last_price - pos.price) * pos.volume * size
                self.accounts['BACKTEST'].unrealized_pnl += float_pnl
                self.accounts['BACKTEST'].equity += float_pnl
                self.accounts['BACKTEST'].available = (self.accounts['BACKTEST'].cash +
                                                       self.accounts['BACKTEST'].unrealized_pnl - self.accounts[
                                                           "BACKTEST"].margin)

                # 更新contract指标
                contract_info = self.contracts_log.get(symbol)
                contract_info['unrealized_pnl'] = float_pnl

    def get_trades(self) -> list[TradeData]:
        return self.trade_log
