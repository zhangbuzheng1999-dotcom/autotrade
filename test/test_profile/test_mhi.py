
# === 冒烟测试脚本（放到一个独立 py 里直接跑）===
# === 冒烟测试脚本（放到一个独立 py 里直接跑）===
from datetime import datetime
import time
from coreutils.constant import Interval, Exchange, Direction, OrderType, OrderStatus
from coreutils.object import BarData, OrderData, TradeData, OrderRequest, ModifyRequest, CancelRequest, PositionData


# ====== 1) 轻量 Engine 模拟（只做回调） ======
class Engine:
    def __init__(self):
        self.gateway_name = "backtest"
        self._id = 0
        self.order = {}  # orderid -> OrderData
        self._strategy = None

    def bind(self, strategy):
        self._strategy = strategy

    def _next_id(self):
        self._id += 1
        return f"o{self._id}"

    # 策略在 _execute 里会调到它们
    def send_order(self, req: OrderRequest):
        oid = self._next_id()
        od = OrderData(
            symbol=req.symbol, exchange=req.exchange, orderid=oid,
            direction=req.direction, price=req.price, volume=req.volume,
            status=OrderStatus.PENDING, reference=getattr(req, "reference", ""),
            gateway_name=self.gateway_name
        )
        # STP_MKT 也放个触发价字段
        setattr(od, "trigger_price", getattr(req, "trigger_price", req.price))
        self.order[oid] = od
        # 立即回调给策略（模拟柜台异步回报）
        self._strategy.on_order(od)
        return oid

    def modify_order(self, req: ModifyRequest):
        od = self.order.get(req.orderid)
        if not od:
            return
        od.price = req.price
        setattr(od, "trigger_price", getattr(req, "trigger_price", req.price))
        od.volume = req.qty
        od.status = OrderStatus.MODIFIED
        self._strategy.on_order(od)

    def cancel_order(self, req: CancelRequest):
        od = self.order.get(req.orderid)
        if not od:
            return
        od.status = OrderStatus.ALLCANCELLED
        self._strategy.on_order(od)


# ====== 2) 帮助函数：构造回报 ======
def create_order_data(orderid: str, order_book: dict, status: OrderStatus,
                      *, symbol: str, exchange=Exchange.HKFE,
                      direction=Direction.SHORT, price=100, volume=3,
                      reference="virgin_sell"):
    od = OrderData(
        symbol=symbol, exchange=exchange, orderid=orderid, direction=direction,
        price=price, volume=volume, status=status, reference=reference, gateway_name="backtest"
    )
    setattr(od, "trigger_price", price)
    order_book[orderid] = od
    return od


def create_trade_data(order: OrderData, status: OrderStatus, *, volume=None, price=None):
    td = TradeData(
        symbol=order.symbol, exchange=order.exchange, orderid=order.orderid,
        direction=order.direction, price=price if price is not None else order.price,
        volume=volume if volume is not None else order.volume, tradeid=order.orderid,
        reference=getattr(order, "reference", ""), status=status, gateway_name="backtest"
    )
    return td


def drain(strategy, timeout=1.0):
    """等待事件引擎把对齐处理完"""
    t0 = time.time()
    while time.time() - t0 < timeout:
        if not strategy._realign_pending and not strategy._reconciling:
            break
        time.sleep(0.01)


def print_books(mhi, tag=""):
    eb = {k: (v.orderid, v.status.name, v.direction.name, v.volume, v.price) for k, v in mhi.entry_book.items()}
    sb = {k: (v.orderid, v.status.name, v.direction.name, v.volume, getattr(v, "trigger_price", v.price)) for k, v in
          mhi.stop_book.items()}
    print(
        f"\n[{tag}] entry_book={eb}\n[{tag}] stop_book={sb}\nposition vol={mhi.position.volume}, side={getattr(mhi.position.direction, 'name', None)}")


# ====== 3) 引入你的策略 ======

code = "HK.MHImain"

# ====== 4) 初始化 ======
# ====== 4) 初始化 ======
engine = Engine()
mhi = ProStrategy(main_engine=engine)
engine.bind(mhi)

mhi.initialize()
mhi.ee.start()  # 启动事件线程

# 先来一根 1m，避免 on_2h 里日志 pre_tick 为空
mhi.on_bar(BarData(symbol=code, exchange=Exchange.HKFE, interval=Interval.K_1M,
                   gateway_name='backtest', open_price=100, close_price=100, high_price=100, low_price=100,
                   volume=100, datetime=datetime.now()))

# 进 15m / 2h，形成首仓窗口 → 策略会自动下 virgin_buy/virgin_sell
mhi.on_bar(BarData(symbol=code, exchange=Exchange.HKFE, interval=Interval.K_15M,
                   gateway_name='backtest', open_price=100, close_price=100, high_price=100, low_price=100,
                   volume=100, datetime=datetime.now()))
mhi.on_bar(BarData(symbol=code, exchange=Exchange.HKFE, interval=Interval.K_2H,
                   gateway_name='backtest', open_price=100, close_price=100, high_price=100, low_price=100,
                   volume=100, datetime=datetime.now()))
drain(mhi)
print_books(mhi, "初始首仓下单后")

# ====== 1) 首仓一边成交（触发撤另一边 + 种子止损） ======
# 找到 virgin_sell 那张单
sell_live = None
for ref, od in mhi.entry_book.items():
    if ref == "virgin_sell":
        sell_live = od
        break

# 模拟“卖出首仓”成交
td = create_trade_data(sell_live, OrderStatus.ALLTRADED)
mhi.on_trade(td)
drain(mhi)
print_books(mhi, "首仓卖出成交后（应撤另一边 + 有止损）")

# ====== 1.1 首仓更新（2h 更新价位；此时只会影响止损/或后续逻辑） ======
mhi.on_bar(BarData(symbol=code, exchange=Exchange.HKFE, interval=Interval.K_2H,
                   gateway_name='backtest', open_price=100, close_price=32000, high_price=32000, low_price=32000,
                   volume=100, datetime=datetime.now()))
drain(mhi)
print_books(mhi, "2H 更新后")

# ====== 2) 15m 更新止损（抬线/改单） ======
mhi.on_bar(BarData(
    symbol=code, exchange=Exchange.HKFE, interval=Interval.K_15M,
    gateway_name='backtest',
    open_price=100, close_price=100, low_price=100,
    high_price=24000,  # ★ 关键：足够高
    volume=100, datetime=datetime.now()
))
drain(mhi)
print_books(mhi, "15m 抬线后")

# ====== 2.1 止损成交 → 归零持仓 ======
# 找到 stop_order
stop_live = mhi.stop_book.get("stop_order", None)
if stop_live:
    td_stop = create_trade_data(stop_live, OrderStatus.ALLTRADED, volume=abs(mhi.position.volume))
    mhi.on_trade(td_stop)
    drain(mhi)
    print_books(mhi, "止损成交后（应清空止损，持仓归零）")

# ====== 3) 测试“非首仓部分成交”场景 ======
# 手动造一张“非首仓 entry”回报（pending）
o_entry = create_order_data("manu-3.1", engine.order, OrderStatus.PENDING,
                            symbol=code, direction=Direction.SHORT, price=200, volume=3, reference="entry")
mhi.on_order(o_entry)

# 3.1 部分成交
td_part = create_trade_data(o_entry, OrderStatus.PARTTRADED, volume=1, price=200)
mhi.on_trade(td_part)
drain(mhi)
print_books(mhi, "非首仓部分成交后（应有止损，entry 仍在）")

# 3.2 又来 15m：应撤掉“没持仓那边”的 entry，并更新止损
mhi.on_bar(BarData(symbol=code, exchange=Exchange.HKFE, interval=Interval.K_15M,
                   gateway_name='backtest', open_price=100, close_price=0, high_price=200, low_price=2000,
                   volume=100, datetime=datetime.now()))
drain(mhi)
print_books(mhi, "部分成交后 15m（应撤剩余 entry 并调止损）")

# 3.3 再成一手（汇合成 2 手持仓）
td_part2 = create_trade_data(o_entry, OrderStatus.PARTTRADED, volume=2, price=200)
mhi.on_trade(td_part2)
drain(mhi)
print_books(mhi, "部分成交追加后（止损应被增量修正）")

# ====== 4) on_tick 触发“切回首仓”（撤非首仓 entry + 挂 virgin 双边，若空仓）=====
# 先清空（模拟止损全成）
if mhi.position.volume != 0 and "stop_order" in mhi.stop_book:
    td_stop2 = create_trade_data(mhi.stop_book["stop_order"], OrderStatus.ALLTRADED, volume=abs(mhi.position.volume))
    mhi.on_trade(td_stop2)
    drain(mhi)
mhi.on_bar(BarData(symbol=code, exchange=Exchange.HKFE, interval=Interval.K_15M,
                   gateway_name='backtest', open_price=100, close_price=0, high_price=200, low_price=2000,
                   volume=100, datetime=datetime.now()))
print_books(mhi, "15m")

# tick 触发 virgin=True（你的“洗牌”规则里会置 True 并 _force_virgin_entry）
# 这里直接手动置：
print('===============')
mhi.pre_tick.high_price = 30000
mhi.virgin = False

print(f'{mhi._force_virgin_entry},{mhi.virgin},{mhi.position}')
mhi.on_bar(BarData(symbol=code, exchange=Exchange.HKFE, interval=Interval.K_1M,
                   gateway_name='backtest', open_price=100, close_price=100, high_price=100, low_price=100,
                   volume=100, datetime=datetime.now()))
print(f'{mhi._force_virgin_entry},{mhi.virgin},{mhi.position}')

drain(mhi)
print_books(mhi, "on_tick 切回首仓后（应撤非首仓 entry 并挂 virgin 双边）")

# ====== 收尾 ======
mhi.ee.stop()
print("\n测试完成。")
