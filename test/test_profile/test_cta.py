# tests/test_cta.py
import time
from typing import Optional, List
import pytest

from engine.oms_engine import OmsBase
from engine.event_engine import (
    Event, EventEngine,
    EVENT_ROLLOVER,
    EVENT_ORDER, EVENT_POSITION,
    EVENT_ORDER_REQ, EVENT_CANCEL_REQ, EVENT_MODIFY_REQ,
)
from engine.rollover_manager import RolloverManager
from coreutils.object import OrderRequest, CancelRequest, OrderData, PositionData
from coreutils.constant import Direction, OrderType, Exchange, OrderStatus

# ---------- 交易所选择 ----------
def pick_hk_exchange() -> Exchange:
    for name in ("HKFE", "SEHK", "GLOBAL", "LOCAL"):
        if hasattr(Exchange, name):
            return getattr(Exchange, name)
    return list(Exchange)[0]

EX = pick_hk_exchange()
GW = "SIM"  # 测试用 gateway 名

# ---------- 捕获 *REQ 事件 ----------
class Probe:
    def __init__(self, ee: EventEngine):
        self.order_reqs: List[OrderRequest] = []
        self.cancel_reqs: List[CancelRequest] = []
        self.modify_reqs: List = []
        ee.register(EVENT_ORDER_REQ, self._on_order_req)
        ee.register(EVENT_CANCEL_REQ, self._on_cancel_req)
        ee.register(EVENT_MODIFY_REQ, self._on_modify_req)
    def _on_order_req(self, e: Event): self.order_reqs.append(e.data)
    def _on_cancel_req(self, e: Event): self.cancel_reqs.append(e.data)
    def _on_modify_req(self, e: Event): self.modify_reqs.append(e.data)

# ---------- 常用动作 ----------
def emit_rollover(ee: EventEngine, symg: str, old: str, new: str, mode: str = "hedged"):
    ee.put(Event(EVENT_ROLLOVER, {"symbol_group": symg, "old": old, "new": new, "mode": mode}))

def post_order(ee: EventEngine, symbol: str, exchange: Exchange, orderid: str,
               status: OrderStatus, reference: str = "", direction: Optional[Direction]=None,
               price: float = 0.0, volume: float = 0.0):
    """发布订单回报（带 gateway_name）。"""
    od = OrderData(
        symbol=symbol, exchange=exchange, orderid=orderid,
        status=status, reference=reference, direction=direction,
        price=price, volume=volume, gateway_name=GW
    )
    ee.put(Event(EVENT_ORDER, od))
    return od

def post_position(ee: EventEngine, symbol: str, exchange: Optional[Exchange],
                  direction: Direction, volume: float):
    """发布持仓回报（带 gateway_name）。exchange 需为有效枚举，避免 __post_init__ 报错。"""
    pos = PositionData(symbol=symbol, exchange=exchange, direction=direction, volume=volume, gateway_name=GW)
    ee.put(Event(EVENT_POSITION, pos))
    return pos

def wait_until(predicate, timeout=1.5, interval=0.01, reason: str = ""):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate(): return True
        time.sleep(interval)
    raise AssertionError(f"Timeout waiting for condition: {reason}")

# ---------- 基础夹具 ----------
@pytest.fixture
def ctx():
    ee = EventEngine()
    try: ee.start()
    except Exception: pass
    oms = OmsBase(ee)
    class Logger:
        def info(self, m): print("INFO:", m)
        def warning(self, m): print("WARN:", m)
        def error(self, m): print("ERR :", m)
    rm = RolloverManager(oms, ee, Logger())
    pb = Probe(ee)
    class Ctx:
        def __init__(self): self.ee, self.oms, self.rm, self.pb = ee, oms, rm, pb
    return Ctx()

# ===================== 场景测试 =====================

def test_s1_no_orders_no_position_done(ctx):
    """S1: 无任何挂单、无旧仓 → 直接 DONE（all cancelled & no position）"""
    symg, old, new = "HK.MHImain", "HK.MHI2507", "HK.MHI2508"
    emit_rollover(ctx.ee, symg, old, new, mode="hedged")
    post_position(ctx.ee, old, EX, Direction.LONG, 0)
    wait_until(lambda: ctx.rm.tasks.get(symg) and ctx.rm.tasks[symg].phase.name == "DONE", reason="S1 finished")
    assert not ctx.pb.cancel_reqs and not ctx.pb.order_reqs

def test_s2_active_nonroll_cancel_allcancelled_done(ctx):
    """S2: 有普通挂单 → 发 CANCEL_REQ；回报 ALLCANCELLED + 无旧仓 → DONE"""
    symg, old, new = "HK.MHImain", "HK.MHI2507", "HK.MHI2508"
    post_order(ctx.ee, old, EX, orderid="U1", status=OrderStatus.NOTTRADED, reference="")
    emit_rollover(ctx.ee, symg, old, new)
    wait_until(lambda: any(cr.orderid == "U1" for cr in ctx.pb.cancel_reqs), reason="CANCEL_REQ U1")
    post_order(ctx.ee, old, EX, orderid="U1", status=OrderStatus.ALLCANCELLED, reference="")
    post_position(ctx.ee, old, EX, Direction.LONG, 0)
    wait_until(lambda: ctx.rm.tasks[symg].phase.name == "DONE", reason="S2 finished")
    assert not ctx.pb.order_reqs

def test_s3_cancel_alltraded_then_issue_hedged_ack_ok_done(ctx):
    """S3: 清场看见成交（ALLTRADED）→ 有旧仓 → hedged: OPEN 后 CLOSE；两腿 ACK 非 REJECT → DONE"""
    symg, old, new = "HK.MHImain", "HK.MHI2507", "HK.MHI2508"
    post_order(ctx.ee, old, EX, orderid="U2", status=OrderStatus.NOTTRADED, reference="")
    emit_rollover(ctx.ee, symg, old, new)
    post_order(ctx.ee, old, EX, orderid="U2", status=OrderStatus.ALLTRADED, reference="")
    post_position(ctx.ee, old, EX, Direction.LONG, 3)
    wait_until(lambda: len(ctx.pb.order_reqs) >= 2, reason="legs issued")
    refs = [r.reference for r in ctx.pb.order_reqs]
    assert any(ref.endswith(":OPEN") for ref in refs) and any(ref.endswith(":CLOSE") for ref in refs)
    post_order(ctx.ee, new, EX, orderid="ROpen", status=OrderStatus.SUBMITTING,
               reference=f"ROLL:{symg}:{old}->{new}:OPEN")
    post_order(ctx.ee, old, EX, orderid="RClose", status=OrderStatus.SUBMITTING,
               reference=f"ROLL:{symg}:{old}->{new}:CLOSE")
    wait_until(lambda: ctx.rm.tasks[symg].phase.name == "DONE", reason="S3 finished")

def test_s4_flat_mode_close_then_open_ack_ok_done(ctx):
    """S4: flat 模式：先 CLOSE 再 OPEN；两腿 ACK 非 REJECT → DONE"""
    symg, old, new = "HK.MHImain", "HK.MHI2507", "HK.MHI2508"
    post_position(ctx.ee, old, EX, Direction.SHORT, 2)
    emit_rollover(ctx.ee, symg, old, new, mode="flat")
    wait_until(lambda: len(ctx.pb.order_reqs) >= 2, reason="flat legs issued")
    # 回 ACK
    post_order(ctx.ee, old, EX, orderid="RClose", status=OrderStatus.SUBMITTING,
               reference=f"ROLL:{symg}:{old}->{new}:CLOSE")
    post_order(ctx.ee, new, EX, orderid="ROpen", status=OrderStatus.SUBMITTING,
               reference=f"ROLL:{symg}:{old}->{new}:OPEN")
    wait_until(lambda: ctx.rm.tasks[symg].phase.name == "DONE", reason="S4 finished")

def test_s5_leg_rejected_fail(ctx):
    """S5: 任一腿 REJECT → FAILED"""
    symg, old, new = "HK.MHImain", "HK.MHI2507", "HK.MHI2508"
    post_position(ctx.ee, old, EX, Direction.LONG, 1)
    emit_rollover(ctx.ee, symg, old, new)
    wait_until(lambda: len(ctx.pb.order_reqs) >= 2, reason="legs issued")
    post_order(ctx.ee, new, EX, orderid="ROpen", status=OrderStatus.REJECTED,
               reference=f"ROLL:{symg}:{old}->{new}:OPEN")
    post_order(ctx.ee, old, EX, orderid="RClose", status=OrderStatus.SUBMITTING,
               reference=f"ROLL:{symg}:{old}->{new}:CLOSE")
    wait_until(lambda: ctx.rm.tasks[symg].phase.name == "FAILED", reason="S5 finished")

def test_s6_no_exchange_done_without_orders(ctx, monkeypatch):
    """S6: 无法推断交易所 -> 直接 DONE（不强下单）"""
    symg, old, new = "HK.MHImain", "HK.MHI2507", "HK.MHI2508"
    # 模拟无法推断交易所
    monkeypatch.setattr(ctx.rm, "_infer_ex", lambda t: None)
    post_position(ctx.ee, old, EX, Direction.LONG, 5)  # position 构造仍需合法的 exchange
    emit_rollover(ctx.ee, symg, old, new)
    wait_until(lambda: ctx.rm.tasks.get(symg) and ctx.rm.tasks[symg].phase.name == "DONE",
               reason="S6 finished")
    assert len(ctx.pb.order_reqs) == 0

def test_s7_out_of_order_position_arrive_late(ctx):
    """S7: 订单先 ALLTRADED，position 晚到 -> 等 position 再发腿"""
    symg, old, new = "HK.MHImain", "HK.MHI2507", "HK.MHI2508"
    post_order(ctx.ee, old, EX, orderid="U3", status=OrderStatus.NOTTRADED, reference="")
    emit_rollover(ctx.ee, symg, old, new)
    post_order(ctx.ee, old, EX, orderid="U3", status=OrderStatus.ALLTRADED, reference="")
    time.sleep(0.05)
    assert len(ctx.pb.order_reqs) == 0
    post_position(ctx.ee, old, EX, Direction.LONG, 2)
    wait_until(lambda: len(ctx.pb.order_reqs) >= 2, reason="legs after pos")
    post_order(ctx.ee, new, EX, orderid="ROpen", status=OrderStatus.SUBMITTING,
               reference=f"ROLL:{symg}:{old}->{new}:OPEN")
    post_order(ctx.ee, old, EX, orderid="RClose", status=OrderStatus.SUBMITTING,
               reference=f"ROLL:{symg}:{old}->{new}:CLOSE")
    wait_until(lambda: ctx.rm.tasks[symg].phase.name == "DONE", reason="S7 finished")

def test_s8_do_not_cancel_roll_orders(ctx):
    """S8: 清场阶段只撤非 ROLL 挂单，不撤已经存在的 ROLL 订单"""
    symg, old, new = "HK.MHImain", "HK.MHI2507", "HK.MHI2508"
    post_order(ctx.ee, old, EX, orderid="RLEG", status=OrderStatus.NOTTRADED,
               reference=f"ROLL:{symg}:{old}->{new}:OPEN")  # 既有 ROLL 单
    post_order(ctx.ee, old, EX, orderid="U4", status=OrderStatus.NOTTRADED, reference="")  # 普通单
    emit_rollover(ctx.ee, symg, old, new)
    wait_until(lambda: len(ctx.pb.cancel_reqs) >= 1, reason="cancel issued")
    targets = {cr.orderid for cr in ctx.pb.cancel_reqs}
    assert "U4" in targets and "RLEG" not in targets
    post_order(ctx.ee, old, EX, orderid="U4", status=OrderStatus.ALLCANCELLED, reference="")
    post_position(ctx.ee, old, EX, Direction.LONG, 1)
    wait_until(lambda: len(ctx.pb.order_reqs) >= 2, reason="legs issued")
    post_order(ctx.ee, new, EX, orderid="ROpen", status=OrderStatus.SUBMITTING,
               reference=f"ROLL:{symg}:{old}->{new}:OPEN")
    post_order(ctx.ee, old, EX, orderid="RClose", status=OrderStatus.SUBMITTING,
               reference=f"ROLL:{symg}:{old}->{new}:CLOSE")
    wait_until(lambda: ctx.rm.tasks[symg].phase.name == "DONE", reason="S8 finished")

# ---------- 可选：直接运行 ----------
if __name__ == "__main__":
    class Dummy: pass
    from inspect import getmembers, isfunction
    this = __import__(__name__)
    _ctx = ctx()  # type: ignore
    for name, fn in getmembers(this, isfunction):
        if name.startswith("test_s"):
            try:
                fn(_ctx)  # type: ignore
                print(f"{name} OK")
            except AssertionError as e:
                print(f"{name} FAILED:", e)
                raise
            _ctx = ctx()  # type: ignore
