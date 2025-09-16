# cta_engine.py
from dataclasses import dataclass
from enum import Enum, auto
from typing import Set, Dict, Optional, Tuple

from coreutils.constant import LogLevel, Direction, OrderType, Exchange, OrderStatus
from coreutils.object import (
    OrderRequest, CancelRequest, ModifyRequest,
    OrderData, PositionData, LogData
)
from engine.event_engine import (
    Event, EventEngine,
    EVENT_ORDER, EVENT_POSITION,
    EVENT_ORDER_REQ, EVENT_CANCEL_REQ, EVENT_MODIFY_REQ, EVENT_COMMAND, EVENT_LOG, EVENT_ROLLOVER
)

CMD_ENGINE_MUTE = "engine.mute"  # data: {"symbols":[...], "on":True/False, "reason":""}
CMD_ENGINE_SWITCH = "engine.switch"  # data: {"on":True/False}

class CtaEngine:
    """
    职责极简：
    - 作为唯一“订单入口”：接收 EVENT_*_REQ -> 防火墙(mute) -> gateway
    - 作为策略事件转发器：tick/bar/order/trade/position 转给 strategy
    - 仅处理两个命令：engine.mute / engine.switch
    """

    def __init__(self, oms, event_engine: EventEngine, gateway, logger):
        self.oms = oms
        self.ee = event_engine
        self.gateway = gateway
        self.logger = logger

        self.strategy = None
        self.active = True
        self._muted_symbols: Set[str] = set()
        self._register()

    # ---- 注册所有事件（行情/订单/持仓/命令 + 三个“请求事件”） ----
    def _register(self):
        self.ee.register(EVENT_COMMAND, self._on_cmd)
        self.ee.register(EVENT_ORDER_REQ, self._on_order_req)
        self.ee.register(EVENT_CANCEL_REQ, self._on_cancel_req)
        self.ee.register(EVENT_MODIFY_REQ, self._on_modify_req)
        self.ee.register(EVENT_LOG, self._on_log)

    # ---- 白名单（模块内单：ROLL:/RISK:/ENGINE:） ----
    @staticmethod
    def _is_internal_ref(ref: Optional[str]) -> bool:
        return bool(ref and (ref.startswith("ENGINE:") or ref.startswith("ROLL:") or ref.startswith("RISK:")))

    # ---- 三个“请求事件”入口：统一防火墙+下发 ----
    def _on_order_req(self, e: Event):
        # 如果引擎暂停mute request
        if not self.active:
            return
        req: OrderRequest = e.data
        # mute 防火墙（策略/外部模块均受控），内部单白名单放行
        if req.symbol in self._muted_symbols and not self._is_internal_ref(req.reference):
            self.logger.warning(f"[FW] send blocked: {req.symbol} ref={req.reference}")
            return
        self.gateway.send_order(req)

    def _on_cancel_req(self, e: Event):
        # 如果引擎暂停mute request
        if not self.active:
            return
        req: CancelRequest = e.data
        # 撤单默认不拦，避免清场被卡；如需风控可在此加策略
        self.gateway.cancel_order(req)

    def _on_modify_req(self, e: Event):
        # 如果引擎暂停mute request
        if not self.active:
            return
        req: ModifyRequest = e.data
        # ModifyRequest 通常没有 reference，mute 期间一律拦截（简单稳妥）
        if req.symbol in self._muted_symbols:
            self.logger.warning(f"[FW] modify blocked: {req.symbol} {req.orderid}")
            return
        self.gateway.modify_order(req)

    # 日志模块
    def _on_log(self, event: Event):
        log_data: LogData = event.data
        if log_data.level == LogLevel.DEBUG:
            self.logger.debug(log_data.msg)
        elif log_data.level == LogLevel.INFO:
            self.logger.info(log_data.msg)
        elif log_data.level == LogLevel.WARNING:
            self.logger.warning(log_data.msg)
        elif log_data.level == LogLevel.ERROR:
            self.logger.error(log_data.msg)

    # ---- 命令：mute 开关 & 引擎开关 ----
    def _on_cmd(self, e: Event):
        data = e.data or {}
        cmd = data.get("cmd")
        payload = data.get("data") or {}
        try:
            if cmd == CMD_ENGINE_MUTE:
                syms = set(payload.get("symbols") or [])
                on = bool(payload.get("on"))
                reason = payload.get("reason", "")
                if on:
                    self._muted_symbols |= syms
                    self.logger.info(f"[ENGINE] mute ON {syms} reason={reason}")
                else:
                    self._muted_symbols -= syms
                    self.logger.info(f"[ENGINE] mute OFF {syms} reason={reason}")
            elif cmd == CMD_ENGINE_SWITCH:
                self.active = bool(payload.get("on"))
                self.logger.info(f"[ENGINE] active={self.active}")

        except Exception as ex:
            self.logger.error(f"[ENGINE CMD] err {cmd}: {ex}")



class Phase(Enum):
    IDLE = auto()
    CANCEL = auto()
    WAIT_CANCEL = auto()
    AWAIT_POS = auto()
    ISSUE = auto()
    WAIT_ACKS = auto()
    DONE = auto()
    FAILED = auto()


@dataclass
class Task:
    symbol_group: str
    old_symbol: str
    new_symbol: str
    mode: str = "hedged"
    phase: Phase = Phase.IDLE

    target_volume: float = 0.0
    target_direction: Optional[Direction] = None
    exchange: Optional[Exchange] = None

    seen_non_allcancelled: bool = False
    need_open: bool = False
    need_close: bool = False
    open_acked: bool = False
    close_acked: bool = False
    open_rejected: bool = False
    close_rejected: bool = False

    pos_seen: bool = False  # 是否见过 old_symbol 的 position


class RolloverManager:
    """
    事件解耦的移仓模块：
    - 订阅 EVENT_ROLLOVER 启动；
    - 仅通过 EVENT_*_REQ 发撤单/发单请求，不直连 gateway；
    - 清场只撤非 ROLL；发腿只等 ACK（非 REJECT 即 OK，不等成交）。
    """
    def __init__(self, oms, event_engine: EventEngine, logger):
        self.oms = oms
        self.ee = event_engine
        self.logger = logger
        self.tasks: Dict[str, Task] = {}
        self._register()

    def _register(self):
        self.ee.register(EVENT_ROLLOVER, self._on_rollover)
        self.ee.register(EVENT_ORDER, self._on_order)
        self.ee.register(EVENT_POSITION, self._on_position)

    # ---------- commands ----------
    def _on_rollover(self, e: Event):
        d = e.data or {}
        symg, old, new = d["symbol_group"], d["old"], d["new"]
        mode = d.get("mode", "hedged")

        t = self.tasks.get(symg)
        if t and t.phase not in (Phase.DONE, Phase.FAILED):
            self.logger.info(f"[ROLL] exists: {symg}")
            return

        t = Task(symbol_group=symg, old_symbol=old, new_symbol=new, mode=mode, phase=Phase.CANCEL)
        self.tasks[symg] = t
        self._advance(t)

    # ---------- events ----------
    def _on_order(self, e: Event):
        od: OrderData = e.data
        for t in list(self.tasks.values()):
            if t.phase in (Phase.DONE, Phase.FAILED):
                continue
            if od.symbol not in (t.old_symbol, t.new_symbol, t.symbol_group):
                continue

            # 清场阶段：订单结果
            if t.phase == Phase.WAIT_CANCEL and not od.is_active():
                if od.status != OrderStatus.ALLCANCELLED:
                    t.seen_non_allcancelled = True
                if not self._has_active_nonroll(t):
                    t.phase = Phase.AWAIT_POS
                    if t.exchange is None:
                        t.exchange = self._infer_ex(t)
                    self._decide_after_clear(t)

            # 腿 ACK / REJECT
            ref = (od.reference or "")
            if ref.startswith(f"ROLL:{t.symbol_group}:{t.old_symbol}->{t.new_symbol}:"):
                rej = (od.status == OrderStatus.REJECTED)
                if ref.endswith(":OPEN"):
                    t.open_acked, t.open_rejected = True, rej
                elif ref.endswith(":CLOSE"):
                    t.close_acked, t.close_rejected = True, rej
                self._check_finish(t)

    def _on_position(self, e: Event):
        pos: PositionData = e.data
        for t in list(self.tasks.values()):
            if t.phase not in (Phase.AWAIT_POS, Phase.WAIT_CANCEL):
                continue
            if pos.symbol not in (t.old_symbol, t.new_symbol):
                continue

            if pos.symbol == t.old_symbol:
                t.pos_seen = True
            if t.exchange is None and pos.exchange:
                t.exchange = pos.exchange

            if t.phase == Phase.AWAIT_POS or (t.phase == Phase.WAIT_CANCEL and not self._has_active_nonroll(t)):
                if t.phase != Phase.AWAIT_POS:
                    t.phase = Phase.AWAIT_POS
                self._decide_after_clear(t)

    # ---------- advance ----------
    def _advance(self, t: Task):
        if t.phase == Phase.CANCEL:
            # 1) 发撤单（只撤非 ROLL）
            self._cancel_nonroll_orders(t)
            # 2) 进入 WAIT_CANCEL
            t.phase = Phase.WAIT_CANCEL
            # 3) 若此刻已无非 ROLL 活动单，立即进入 AWAIT_POS 并决策（避免卡住）
            if not self._has_active_nonroll(t):
                t.phase = Phase.AWAIT_POS
                if t.exchange is None:
                    t.exchange = self._infer_ex(t)
                self._decide_after_clear(t)

    # ---------- core decisions ----------
    def _decide_after_clear(self, t: Task):
        # 若还没拿到旧合约的 position，则等待
        pos = self.oms.get_position(t.old_symbol)
        if pos is None:
            t.phase = Phase.AWAIT_POS
            return

        vol, dire = self._calc_net_pos(t.old_symbol)
        have_old = (vol > 0 and dire is not None)

        # A: 未见成交且明确无旧仓 -> DONE
        if (not t.seen_non_allcancelled) and (not have_old):
            self._finish(t, True, "all cancelled & no position")
            return

        # B: 见过成交但当前无旧仓 -> 再等等 position（可能延迟）
        if t.seen_non_allcancelled and not have_old:
            t.phase = Phase.AWAIT_POS
            return

        # C: 有旧仓 -> 发腿
        if t.exchange is None:
            self._finish(t, True, "no exchange")
            return

        t.target_volume, t.target_direction = vol, dire
        t.need_open = t.need_close = False
        t.open_acked = t.close_acked = False
        t.open_rejected = t.close_rejected = False

        if t.mode == "hedged":
            if vol > 0:
                t.need_open = True
                self._send_open_new(t, vol, dire)
            if vol > 0:
                t.need_close = True
                self._send_close_old(t)
        else:  # flat
            if vol > 0:
                t.need_close = True
                self._send_close_old(t)
                t.need_open = True
                self._send_open_new(t, vol, dire)

        t.phase = Phase.WAIT_ACKS
        self._check_finish(t)

    # ---------- legs ----------
    def _send_open_new(self, t: Task, vol: float, dire: Direction):
        ref = f"ROLL:{t.symbol_group}:{t.old_symbol}->{t.new_symbol}:OPEN"
        req = OrderRequest(
            symbol=t.new_symbol, exchange=t.exchange, direction=dire,
            type=OrderType.MARKET, price=0, volume=float(vol),
            trigger_price=0, reference=ref
        )
        self.ee.put(Event(EVENT_ORDER_REQ, req))

    def _send_close_old(self, t: Task):
        vol, dire = self._calc_net_pos(t.old_symbol)
        if vol <= 0 or dire is None:
            return
        close_dir = Direction.LONG if dire == Direction.SHORT else Direction.SHORT
        ref = f"ROLL:{t.symbol_group}:{t.old_symbol}->{t.new_symbol}:CLOSE"
        req = OrderRequest(
            symbol=t.old_symbol, exchange=t.exchange, direction=close_dir,
            type=OrderType.MARKET, price=0, volume=float(vol),
            trigger_price=0, reference=ref
        )
        self.ee.put(Event(EVENT_ORDER_REQ, req))

    # ---------- finish ----------
    def _check_finish(self, t: Task):
        open_ok = (not t.need_open) or (t.open_acked and not t.open_rejected)
        close_ok = (not t.need_close) or (t.close_acked and not t.close_rejected)
        open_bad = t.need_open and t.open_acked and t.open_rejected
        close_bad = t.need_close and t.close_acked and t.close_rejected
        if open_bad or close_bad:
            self._finish(t, False, "leg rejected")
        elif open_ok and close_ok:
            self._finish(t, True)

    def _finish(self, t: Task, success: bool, reason: str = ""):
        t.phase = Phase.DONE if success else Phase.FAILED
        self.logger.info(f"[ROLL] {t.symbol_group} {t.old_symbol}->{t.new_symbol} "
                         f"{'DONE' if success else 'FAILED'} {reason}")

    # ---------- utils ----------
    def _cancel_nonroll_orders(self, t: Task):
        impacted = {t.symbol_group, t.old_symbol, t.new_symbol}
        for o in self.oms.get_all_active_orders():
            ref = o.reference or ""
            if (o.symbol in impacted) and (not ref.startswith("ROLL:")):
                self.ee.put(Event(EVENT_CANCEL_REQ, CancelRequest(
                    orderid=o.orderid, symbol=o.symbol, exchange=o.exchange
                )))

    def _has_active_nonroll(self, t: Task) -> bool:
        impacted = {t.symbol_group, t.old_symbol, t.new_symbol}
        for o in self.oms.get_all_active_orders():
            ref = o.reference or ""
            if o.symbol in impacted and (not ref.startswith("ROLL:")) and o.is_active():
                return True
        return False

    def _calc_net_pos(self, symbol: str) -> Tuple[float, Optional[Direction]]:
        pos = self.oms.get_position(symbol)
        if not pos or pos.volume <= 0:
            return 0.0, None
        return float(pos.volume), pos.direction

    def _infer_ex(self, t: Task) -> Optional[Exchange]:
        p = self.oms.get_position(t.old_symbol)
        if p and p.exchange:
            return p.exchange
        p = self.oms.get_position(t.new_symbol)
        if p and p.exchange:
            return p.exchange
        return None
