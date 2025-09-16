# conn/engine_mes_adapter.py
from __future__ import annotations

import os
import re
import json
import threading
import time
from datetime import datetime
from queue import Queue
from typing import Dict, Any, Optional, Tuple, List
import zmq
from engine.event_engine import (Event, EventEngine, EVENT_ORDER, EVENT_POSITION,EVENT_ORDER_REQ,EVENT_CANCEL_REQ,
                                 EVENT_MODIFY_REQ,EVENT_LOG)
from engine.oms_engine import OmsBase
from engine.event_engine import EVENT_COMMAND
from coreutils.constant import Direction,OrderType,LogLevel
from coreutils.object import OrderRequest,ModifyRequest,CancelRequest,LogData


def _now_ts() -> int:
    return int(time.time())


class EngineMesAdapter:
    """
    Trade_Engine ↔ 消息中心 适配器（ZMQ↔WS Hub）：
      - 上行：只发布“事件”（不发命令），topic=order:<engine>
              payload 原样：{"type","engine","ts","epoch","seq","data"}
      - 下行：只接收“命令”，topic=cmd:<engine> / cmd:all
              payload: {"cmd": "...", "data": {...}, "ts": ...}
      - 快照/对齐：收到 cmd=snapshot -> epoch+=1, seq=0 -> 发送一条 snapshot（seq=0）

    新增：
      - cmd="log.query" 时，读取日志并回传：
        data 示例：
          {
            "path": "logs/engineA.log",     # 可选；不传则按 engine_id + 日期推断
            "date": "2025-08-31",           # 可选，优先级高于 start 的日期
            "start": "2025-08-31 09:00:00", # 可选
            "end":   "2025-08-31 10:00:00", # 可选
            "limit": 500,                   # 返回最多条数（默认 500）
            "include": ["ERROR","订单"],     # 可选，任意一个命中即可
            "level":  ["ERROR","WARNING"]   # 可选，按 [LEVEL] 过滤
          }
        回传事件：
          {"type":"log","engine":..., "ts":..., "epoch":E, "seq":S,
           "data":{"path": "...", "lines": [...], "count": N, "range": {...}}}
    """

    # 默认每次返回的日志行上限（可按需调整）
    _default_log_limit = 500

    def __init__(
            self,
            engine_id: str,
            event_engine: EventEngine,
            oms: OmsBase,
            pub_endpoint: str = "tcp://127.0.0.1:7001",  # 引擎 -> 中心 (Hub SUB.bind)
            sub_endpoint: str = "tcp://127.0.0.1:7002",  # 中心 -> 引擎 (Hub PUB.bind)
    ) -> None:
        self.engine_id = engine_id
        self.event_engine = event_engine
        self.oms = oms
        self.pub_endpoint = pub_endpoint
        self.sub_endpoint = sub_endpoint

        self._ctx = zmq.Context.instance()
        # 队列元素：(topic, payload_dict, enq_epoch)
        self._send_q: "Queue[Tuple[str, Dict[str, Any], int]]" = Queue()

        self._running = threading.Event()
        self._pub_thread: Optional[threading.Thread] = None
        self._sub_thread: Optional[threading.Thread] = None
        self._registered = False

        # 序列控制
        self._epoch = 1
        self._seq = 0
        self._epoch_lock = threading.Lock()  # 切换/读取 epoch/seq 的互斥

        # 日志行解析用的正则：匹配
        # "YYYY-MM-DD HH:MM:SS,mmm [LEVEL] logger: message"
        # 或 "YYYY-MM-DD HH:MM:SS [LEVEL] logger: message"
        self._log_re = re.compile(
            r"^\s*(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:[.,](\d{1,3}))?\s*\[(\w+)\]\s*(.*)$"
        )

    # ---------- 生命周期 ----------
    def start(self) -> None:
        if self._running.is_set():
            return
        self._running.set()

        if not self._registered:
            self.event_engine.register(EVENT_ORDER, self.on_order)
            self.event_engine.register(EVENT_POSITION, self.on_position)
            self._registered = True

        self._pub_thread = threading.Thread(target=self._pub_loop, name=f"{self.engine_id}-pub", daemon=True)
        self._sub_thread = threading.Thread(target=self._sub_loop, name=f"{self.engine_id}-sub", daemon=True)
        self._pub_thread.start()
        self._sub_thread.start()
        self.event_engine.put(Event(EVENT_LOG,LogData(msg='EngineMesAdapter启动',level=LogLevel.INFO)))

    def stop(self) -> None:
        self._running.clear()
        if self._registered:
            self.event_engine.unregister(EVENT_ORDER, self.on_order)
            self.event_engine.unregister(EVENT_POSITION, self.on_position)
            self._registered = False

        with self._epoch_lock:
            current_epoch = self._epoch
        self._send_q.put(("__quit__", {}, current_epoch))
        if self._pub_thread:
            self._pub_thread.join(timeout=2.0)
        if self._sub_thread:
            self._sub_thread.join(timeout=2.0)

    # ---------- vn.py 事件回调（只 to_dict + 入队，打 enq_epoch） ----------
    def on_order(self, evt: Event) -> None:
        d = evt.data.to_dict()
        self._enqueue("order", d)

    def on_position(self, evt: Event) -> None:
        d = evt.data.to_dict()
        self._enqueue("position", d)

    def _enqueue(self, event_type: str, data: Dict[str, Any]) -> None:
        topic = f"order:{self.engine_id}"
        payload = {"type": event_type, "engine": self.engine_id, "ts": _now_ts(), "data": data}
        with self._epoch_lock:
            enq_epoch = self._epoch
        self._send_q.put((topic, payload, enq_epoch))

    # ---------- 发送线程：丢旧纪元、打 (epoch,seq) 并发送 ----------
    def _pub_loop(self) -> None:
        pub = self._ctx.socket(zmq.PUB)
        pub.setsockopt(zmq.LINGER, 0)  # 关闭时不阻塞
        pub.connect(self.pub_endpoint)
        time.sleep(0.2)  # 等握手
        try:
            while self._running.is_set():
                topic, payload, enq_epoch = self._send_q.get()
                if topic == "__quit__":
                    break

                # 快照控制消息：直接发 snapshot（seq=0），epoch = enq_epoch（新纪元）
                if topic == "__snapshot__":
                    out = {
                        "type": "snapshot",
                        "engine": self.engine_id,
                        "ts": _now_ts(),
                        "epoch": enq_epoch,
                        "seq": 0,
                        **payload,  # 这里通常是 {"data": {...}}
                    }
                    pub.send_multipart([
                        f"order:{self.engine_id}".encode("utf-8"),
                        json.dumps(out, ensure_ascii=False).encode("utf-8"),
                    ])
                    continue

                # 在同一把锁内：读当前纪元、判断是否丢弃、递增 seq、回填 epoch/seq
                with self._epoch_lock:
                    current_epoch = self._epoch
                    if enq_epoch < current_epoch:
                        continue
                    self._seq += 1
                    payload["epoch"] = current_epoch
                    payload["seq"] = self._seq

                pub.send_multipart([
                    topic.encode("utf-8"),
                    json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"),
                ])
        finally:
            pub.close(0)

    # ---------- 命令线程：snapshot / log.query / 其他命令 ----------
    def _sub_loop(self) -> None:
        sub = self._ctx.socket(zmq.SUB)
        sub.setsockopt(zmq.LINGER, 0)
        sub.setsockopt(zmq.RCVTIMEO, 200)  # 避免 stop 时永久阻塞
        sub.connect(self.sub_endpoint)
        sub.setsockopt_string(zmq.SUBSCRIBE, f"cmd:{self.engine_id}")
        sub.setsockopt_string(zmq.SUBSCRIBE, "cmd:all")  # 支持广播
        try:
            while self._running.is_set():
                try:
                    topic_b, data_b = sub.recv_multipart()  # 阻塞（带超时）
                except zmq.Again:
                    continue

                # 载荷：{"cmd": "...", "data": {...}, "ts": ...}
                try:
                    cmd_msg = json.loads(data_b.decode("utf-8"))
                except Exception as e:
                    self.event_engine.put(Event(EVENT_LOG, LogData(msg=f"[{self.engine_id}] bad command payload: {e}",
                                                                   level=LogLevel.ERROR)))
                    continue

                log_data = LogData(msg=f"[EngineMesAdapter] {self.engine_id} : {cmd_msg}",
                                                               level=LogLevel.INFO)
                self.event_engine.put(Event(EVENT_LOG,log_data))
                cmd = (cmd_msg.get("cmd") or "").strip()
                data = cmd_msg.get("data") or {}

                if cmd == "snapshot":
                    self._do_snapshot()
                elif cmd == 'order.query':
                    self._handle_order_query(data)
                elif cmd == "log.query":
                    self._handle_log_query(data)
                elif cmd == 'order.modify':
                    vt_orderid = data.get('vt_orderid')
                    order = self.oms.get_order(vt_orderid)
                    if not order:
                        return

                    qty = data.get('qty')
                    trigger_price = data.get('trigger_price')
                    price = data.get('price')
                    if not price and not qty and not trigger_price:
                        return

                    if not qty:
                        qty = order.volume
                    if not trigger_price:
                        trigger_price = order.trigger_price
                    if not price:
                        price = order.price

                    req_modify = ModifyRequest(orderid=order.orderid, symbol=order.symbol, qty=qty,
                                               trigger_price=trigger_price, price=price, exchange=order.exchange, )
                    self.event_engine.put(Event(EVENT_MODIFY_REQ,req_modify))

                elif cmd == 'order.cancel':
                    vt_orderid = data.get('vt_orderid')
                    order = self.oms.get_order(vt_orderid)
                    if not order:
                        return
                    req_cancel = CancelRequest(orderid=order.orderid, symbol=order.symbol, exchange=order.exchange, )
                    self.event_engine.put(Event(EVENT_CANCEL_REQ,req_cancel))

                elif cmd == 'position.close':
                    vt_positionid = data.get('vt_positionid')
                    l_position = [p for p in self.oms.get_all_positions() if p.vt_positionid == vt_positionid]
                    if not l_position:
                        return
                    position = l_position[0]
                    direction = Direction.LONG if position.direction == Direction.SHORT else Direction.SHORT

                    req_order = OrderRequest(symbol=position.symbol, exchange=position.exchange, direction=direction,
                                             volume=position.volume,
                                             type=OrderType.MARKET, reference='Engine_Close')
                    self.event_engine.put(Event(EVENT_ORDER_REQ,req_order))



                else:
                    # 其它命令：转投 CTA 事件引擎（由上层注册处理）
                    self.event_engine.put(Event(EVENT_COMMAND, {"cmd": cmd, "data": data}))
        finally:
            sub.close(0)

    # ---------- 快照：切换 epoch、发 snapshot ----------
    def _do_snapshot(self) -> None:
        # 1) 准备快照数据（直接用 OMS；None/空表都能返回空快照）
        snap_payload = self._make_snapshot_payload()
        # 2) 原子切换到新纪元（并重置 seq）
        with self._epoch_lock:
            self._epoch += 1
            self._seq = 0
            new_epoch = self._epoch
        # 3) 把“快照控制消息”放入队列（enq_epoch = 新纪元）
        self._send_q.put(("__snapshot__", snap_payload, new_epoch))

    # ---------- 快照数据：使用 OMS ----------
    def _make_snapshot_payload(self) -> Dict[str, Any]:
        """
        从 OMS 提取当前完整快照；返回 {"data": {...}}。
        Any 为 None/空都按空列表处理。
        """
        try:
            orders_raw = self.oms.get_all_active_orders() if self.oms else []
            positions_raw = self.oms.get_all_positions() if self.oms else []
            orders = [o.to_dict() for o in (orders_raw or [])]
            positions = [p.to_dict() for p in (positions_raw or [])]
            return {"data": {"orders": orders, "positions": positions, "snapshot_at": _now_ts()}}
        except Exception as e:
            return {"data": {"orders": [], "positions": [], "snapshot_at": _now_ts(),
                             "error": f"snapshot build failed: {e}"}}

    # ================== 日志查询相关 ==================

    def _handle_log_query(self, data: Dict[str, Any]) -> None:
        """
        处理 cmd="log.query"：
          - 若未传 path，则按 engine_id 与 date/start 推断默认路径
          - 读取文本日志，按时间范围/级别/关键字过滤
          - 回发一条 type="log" 的事件
        """
        path = (data.get("path") or "").strip()
        start_s = (data.get("start") or "").strip()
        end_s = (data.get("end") or "").strip()
        date_s = (data.get("date") or "").strip()  # 优先级高于 start 的日期
        limit = int(data.get("limit") or self._default_log_limit)

        # 关键字过滤：字符串或列表（任意一个命中即可）
        inc = data.get("include")
        include_terms: List[str] = []
        if isinstance(inc, str) and inc:
            include_terms = [inc]
        elif isinstance(inc, list):
            include_terms = [str(x) for x in inc if str(x)]

        # level 过滤：如 ["ERROR","WARNING"]
        lvl = data.get("level")
        level_set: Optional[set[str]] = None
        if isinstance(lvl, str) and lvl:
            level_set = {lvl.upper()}
        elif isinstance(lvl, list) and lvl:
            level_set = {str(x).upper() for x in lvl if str(x)}

        # 合法限制
        limit = max(1, min(limit, 10000))

        # 解析时间
        start_dt = self._try_parse_dt(start_s) if start_s else None
        end_dt = self._try_parse_dt(end_s) if end_s else None

        # 当未传 path 时，按 engine_id + 日期推断
        if not path:
            path = self._default_log_path(start_s=start_s, date_s=date_s)

        lines: List[str] = []
        if os.path.exists(path) and os.path.isfile(path):
            try:
                lines = self._read_log_range(
                    path=path,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    include_terms=include_terms,
                    level_set=level_set,
                    limit=limit,
                )
            except Exception as e:
                # 读取异常视为空
                log_data = LogData(msg=f"[EngineMesAdapter] {self.engine_id} log query read error: {e}",
                                                               level=LogLevel.ERROR)
                self.event_engine.put(Event(EVENT_LOG,log_data))
                lines = []
        else:
            # 文件不存在 -> 空
            lines = []

        # 回发一条 type="log" 的事件（走正常队列与 epoch/seq 逻辑）
        payload = {
            "type": "log",
            "engine": self.engine_id,
            "ts": _now_ts(),
            "data": {
                "path": path,
                "count": len(lines),
                "range": {
                    "start": start_dt.isoformat(sep=" ") if start_dt else None,
                    "end": end_dt.isoformat(sep=" ") if end_dt else None,
                },
                "lines": lines,  # 直接回原始文本行
            },
        }
        with self._epoch_lock:
            enq_epoch = self._epoch
        self._send_q.put((f"order:{self.engine_id}", payload, enq_epoch))

        # ===================== 订单查询：处理器 =====================

    def _handle_order_query(self, data: Dict[str, Any]) -> None:
        """
        处理 cmd="order.query":
          - 接收 start_date / end_date / limit（均可为空）
          - 规范化类型后直接调用 self.oms.filter_orders(...)
          - 将结果以 type="orders" 的事件回发
        """
        # limit 允许为 None 或 int
        limit_raw = data.get("limit", None)
        limit: Optional[int]
        try:
            if limit_raw is None or (isinstance(limit_raw, str) and not limit_raw.strip()):
                limit = None
            else:
                limit = int(limit_raw)
        except Exception:
            limit = None

        # start_date / end_date 可为 datetime / 时间戳 / 字符串
        def _norm_dt(val):
            if val is None:
                return None
            if isinstance(val, (int, float, datetime)):
                return val
            if isinstance(val, str):
                val = val.strip()
                if not val:
                    return None
                dt = self._try_parse_dt(val)
                # 解析成功则用 datetime，否则保持字符串（filter_orders 支持 str）
                return dt if dt else val
            # 其它类型都转成字符串（filter_orders 也接受 str）
            return str(val)

        start_param = _norm_dt(data.get("start_date"))
        end_param = _norm_dt(data.get("end_date"))

        # 查询
        orders_dicts: List[Dict[str, Any]] = []
        try:
            result = self.oms.filter_orders(limit=limit, start_date=start_param, end_date=end_param)
            log_data = LogData(msg=f"[EngineMesAdapter] limit{limit},{start_param},{end_param} result:{result}",
                               level=LogLevel.INFO)
            self.event_engine.put(Event(EVENT_LOG, log_data))
            if result:
                orders_dicts = [o.to_dict() for o in result]
        except Exception as e:
            log_data = LogData(msg=f"[EngineMesAdapter] {self.engine_id} order query error: {e}",
                               level=LogLevel.ERROR)
            self.event_engine.put(Event(EVENT_LOG, log_data))
            orders_dicts = []

        # 回发事件
        payload = {
            "type": "orders",
            "engine": self.engine_id,
            "ts": _now_ts(),
            "data": {
                "count": len(orders_dicts),
                "range": {
                    "start": start_param.isoformat(sep=" ") if isinstance(start_param, datetime) else (
                        start_param if isinstance(start_param, (str, int, float)) else None),
                    "end": end_param.isoformat(sep=" ") if isinstance(end_param, datetime) else (
                        end_param if isinstance(end_param, (str, int, float)) else None),
                },
                "limit": limit,
                "orders": orders_dicts,
            },
        }
        with self._epoch_lock:
            enq_epoch = self._epoch
        self._send_q.put((f"order:{self.engine_id}", payload, enq_epoch))

    def _default_log_path(self, start_s: str, date_s: str | None) -> str:
        """
        未传 path 时，按 engine_id 与日期推断：
          - 优先使用 date（YYYY-MM-DD）
          - 其次使用 start 的日期部分
          - 都没有则为“今天”
          今天：logs/{engine_id}.log
          历史：logs/{engine_id}.log.YYYY-MM-DD   （TimedRotatingFileHandler 的默认命名）
        """
        day = None
        if date_s:
            try:
                day = datetime.strptime(date_s.strip(), "%Y-%m-%d").date()
            except Exception:
                day = None

        if day is None and start_s:
            dt = self._try_parse_dt(start_s)
            if dt:
                day = dt.date()

        if day is None:
            day = datetime.now().date()

        today = datetime.now().date()
        base = f"{self.engine_id}.log"
        if day == today:
            return os.path.join("logs", base)
        else:
            return os.path.join("logs", f"{base}.{day.strftime('%Y-%m-%d')}")

    def _try_parse_dt(self, s: str) -> Optional[datetime]:
        """
        尝试解析多种常见时间格式：
          - YYYY-MM-DD HH:MM:SS
          - YYYY-MM-DDTHH:MM:SS
          - YYYY-MM-DD
        """
        s = s.strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt)
                return dt
            except Exception:
                continue
        # fromisoformat 兜底
        try:
            return datetime.fromisoformat(s)
        except Exception:
            return None

    def _read_log_range(
            self,
            path: str,
            start_dt: Optional[datetime],
            end_dt: Optional[datetime],
            include_terms: List[str],
            level_set: Optional[set[str]],
            limit: int,
    ) -> List[str]:
        """
        读取文本日志：
          - 若提供 start/end，则做时间范围过滤（闭区间）
          - 若提供 level_set，则按 [LEVEL] 过滤
          - 若提供 include_terms，则任一关键字命中即可
          - 最终只返回符合条件的最后 limit 行（tail 语义）
        """
        matched: List[str] = []

        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                # 先按关键字过滤（若传了）
                if include_terms:
                    # 任意一个命中即可
                    if not any(term in line for term in include_terms):
                        continue

                # 尝试解析时间与级别；解析失败则按“不过滤时间/级别”处理
                ts_dt = None
                lvl = None
                m = self._log_re.match(line)
                if m:
                    date_s, time_s, ms_s, lvl_s, _rest = m.groups()
                    # 解析时间
                    try:
                        ts_str = f"{date_s} {time_s}"
                        ts_dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        ts_dt = None
                    # 级别
                    if lvl_s:
                        lvl = lvl_s.upper()

                # 级别过滤
                if level_set is not None and lvl is not None:
                    if lvl not in level_set:
                        continue

                # 时间过滤（只有在能解析出 ts_dt 时才判断；否则不按时间过滤）
                if ts_dt:
                    if start_dt and ts_dt < start_dt:
                        continue
                    if end_dt and ts_dt > end_dt:
                        continue

                matched.append(line.rstrip("\n"))

        # 只返回最后 limit 行（tail）
        if len(matched) > limit:
            return matched[-limit:]
        return matched
