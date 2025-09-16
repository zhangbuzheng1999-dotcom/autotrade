# tests/sim_cta_engine.py

from websockets.legacy.exceptions import InvalidStatusCode

from engine.event_engine import EventEngine, Event, EVENT_COMMAND
from engine.oms_engine import OmsBase
from conn.engine_mes_adapter import EngineMesAdapter

# 你的真实数据类/枚举（按你的工程路径导入）

ee = EventEngine()
ee.start()
oms = OmsBase(event_engine=ee)
adapter = EngineMesAdapter(
    engine_id="mhi",
    event_engine=ee,
    oms=oms,
    pub_endpoint="tcp://127.0.0.1:7001",  # 上行到 ws_hub 的 SUB.bind
    sub_endpoint="tcp://127.0.0.1:7002",  # 下行从 ws_hub 的 PUB.bind
)


def on_cmd(evt: Event):
    payload = evt.data or {}
    cmd = payload.get("cmd")
    if cmd != "snapshot":  # snapshot 由适配器内部完成：切纪元+发快照
        print(f"[ENGINE] recv CMD -> {payload}")


ee.register(EVENT_COMMAND, on_cmd)

adapter.start()
print("[ENGINE] adapter started (engine_id='test')")


def emit_loop(date,orderid):
    now = time.time()

    # ORDER 1
    od = OrderData(
        symbol="code233",
        exchange=Exchange.HKFE,
        orderid=orderid,
        type=OrderType.MARKET,
        direction=Direction.LONG,
        volume=1,
        price=1.0,
        gateway_name="FUTU",
        status=OrderStatus.SUBMITTING,
        traded=0,
        avgFillPrice=0.0,
        broker_orderid="broker_1",
        reference="ref_1",datetime=date
    )
    ee.put(Event(EVENT_ORDER, od))
    time.sleep(0.2)

def emit_position_loop():
    now = time.time()

    # ORDER 1
    od = PositionData(
        symbol="test1",
        exchange=Exchange.HKFE,
        direction=Direction.LONG,
        volume=1,
        price=1.0,
        gateway_name="FUTU",

    )
    ee.put(Event(EVENT_POSITION, od))
    time.sleep(0.2)


# tests/sim_ws_client.py
from __future__ import annotations
import asyncio
import json
import threading
import time
from typing import Optional
from queue import Queue, Empty

import requests
import websockets
from websockets.exceptions import ConnectionClosed

# -------- 可按需覆盖的默认地址/账号 --------
WS_URL    = "ws://127.0.0.1:8000/ws"
LOGIN_URL = "http://127.0.0.1:8000/login"
USERNAME  = "testuser"
PASSWORD  = "testpassword"


class SimWsClient:
    """
    多线程包装：
      - 后台线程跑 asyncio 事件循环，维持 WS 连接（自动重连 + 重订阅）
      - 收到的任何消息（推送/响应）原样打印；自动回应 meta.ping -> meta.pong
      - 提供线程安全的发送接口；支持等待指定 id 的返回
      - 新增：query_logs(...) 发送 'engine.command' 的 'log.query'，并在收到日志事件时友好打印
    """
    def __init__(
        self,
        engine: str = "test",
        topics=None,
        ws_url: str = WS_URL,
        login_url: str = LOGIN_URL,
        username: str = USERNAME,
        password: str = PASSWORD,
    ):
        self.engine = engine
        self.topics = topics or [f"order:{engine}"]
        self.ws_url = ws_url
        self.login_url = login_url
        self.username = username
        self.password = password

        self._out_q: Queue[str] = Queue()
        self._stop_flag = threading.Event()
        self._thread: threading.Thread | None = None

        self._id = 0
        self._token: Optional[str] = None

        # 等待特定 id 的响应
        self._pending: dict[int, Queue[dict]] = {}
        self._pending_lock = threading.Lock()

    # ------------ 对外：控制 ------------
    def start(self):
        self._token = self._get_token()
        self._thread = threading.Thread(target=self._thread_main, name="SimClient-WS", daemon=True)
        self._thread.start()
        print("[CLIENT] started")

    def stop(self):
        self._stop_flag.set()
        self._out_q.put_nowait("__STOP__")
        if self._thread:
            self._thread.join(timeout=2.0)
        print("[CLIENT] stopped")

    # ------------ 对外：发送通用命令 ------------
    def send_command(self, engine: str, cmd: str, data: dict | None = None, wait: bool = False, timeout: float = 5.0):
        """给任意 engine 发命令；支持 engine='all' 广播"""
        rid = self._send_rpc("engine.command", {"engine": engine, "cmd": cmd, "data": data or {}})
        if wait:
            return self._await_response(rid, timeout=timeout)
        return None

    # ------------ 对外：便捷日志查询 ------------
    def query_logs(
        self,
        engine: str,
        *,
        path: str | None = None,
        date: str | None = None,         # "YYYY-MM-DD"
        start: str | None = None,        # "YYYY-MM-DD HH:MM:SS"
        end: str | None = None,          # "YYYY-MM-DD HH:MM:SS"
        limit: int = 500,
        include: str | list[str] | None = None,
        level: str | list[str] | None = None,
        wait_ack: bool = False,
        timeout: float = 5.0,
    ):
        """
        发送 'log.query' 到指定 engine。
        - 真正的日志数据以 event.emit + type="log" 异步推送回来，本客户端会自动打印。
        - wait_ack=True 时仅等待 hub 对 engine.command 的立即响应（不是日志返回）。
        """
        data = {
            "limit": limit,
        }
        if path:   data["path"]   = path
        if date:   data["date"]   = date
        if start:  data["start"]  = start
        if end:    data["end"]    = end
        if include is not None: data["include"] = include
        if level   is not None: data["level"]   = level

        rid = self._send_rpc("engine.command", {"engine": engine, "cmd": "log.query", "data": data})
        if wait_ack:
            return self._await_response(rid, timeout=timeout)
        return None

    # ------------ 内部：HTTP 拿 token ------------
    def _get_token(self) -> str:
        r = requests.post(self.login_url, json={"username": self.username, "password": self.password}, timeout=5)
        r.raise_for_status()
        token = r.json()["access_token"]
        print("[CLIENT] got token")
        return token

    # ------------ 内部：线程/事件循环 ------------
    def _thread_main(self):
        asyncio.run(self._run_ws())

    async def _run_ws(self):
        RECONNECT_BACKOFFS = [1, 2, 4, 8, 15]
        backoff_idx = 0
        while not self._stop_flag.is_set():
            try:
                # 关闭底层心跳，用应用层 JSON-RPC ping/pong
                async with websockets.connect(self.ws_url, ping_interval=None) as ws:
                    backoff_idx = 0
                    # 登录 + 订阅
                    await self._send_rpc_now(ws, "auth.login", {"access_token": self._token})
                    await self._send_rpc_now(ws, "sub.subscribe", {"topics": self.topics})

                    sender = asyncio.create_task(self._sender(ws))
                    receiver = asyncio.create_task(self._receiver(ws))

                    done, pending = await asyncio.wait({sender, receiver}, return_when=asyncio.FIRST_EXCEPTION)
                    for t in pending:
                        t.cancel()
                        try:
                            await t
                        except Exception:
                            pass
            except (ConnectionClosed, InvalidStatusCode, OSError) as e:
                delay = RECONNECT_BACKOFFS[min(backoff_idx, len(RECONNECT_BACKOFFS) - 1)]
                print(f"[CLIENT] ws disconnected, retry in {delay}s:", e)
                await asyncio.sleep(delay)
                backoff_idx += 1
            except Exception as e:
                delay = RECONNECT_BACKOFFS[min(backoff_idx, len(RECONNECT_BACKOFFS) - 1)]
                print(f"[CLIENT] ws error, retry in {delay}s:", e)
                await asyncio.sleep(delay)
                backoff_idx += 1

    # ------------ 内部：async 发送/接收 ------------
    async def _sender(self, ws):
        try:
            while True:
                try:
                    msg = self._out_q.get(timeout=0.1)
                except Empty:
                    await asyncio.sleep(0.01)
                    continue
                if msg == "__STOP__":
                    return
                await ws.send(msg)
        except asyncio.CancelledError:
            return

    async def _receiver(self, ws):
        try:
            while True:
                raw = await ws.recv()
                try:
                    msg = json.loads(raw)
                except Exception:
                    print("[CLIENT] recv(raw):", raw)
                    continue

                # 自动回应应用层 ping
                if isinstance(msg, dict) and msg.get("method") == "meta.ping":
                    try:
                        ts = (msg.get("params") or {}).get("ts")
                        pong = {"jsonrpc": "2.0", "method": "meta.pong", "params": {"ts": ts}, "id": self._next_id()}
                        await ws.send(json.dumps(pong))
                    except Exception as e:
                        print("[CLIENT] pong failed:", e)
                    print("[CLIENT] recv(ping):", msg)
                    continue

                # 事件推送（包括日志）
                if isinstance(msg, dict) and msg.get("method") == "event.emit":
                    params = msg.get("params") or {}
                    topic  = params.get("topic")
                    data   = params.get("data") or {}
                    # 日志事件：友好打印
                    if isinstance(data, dict) and data.get("type") == "log":
                        self._print_log_event(topic, data)
                        continue
                    # 其它事件：原样打印
                    print("[CLIENT] event:", params)
                    continue

                # RPC 响应
                rid = msg.get("id")
                if rid is not None:
                    q = None
                    with self._pending_lock:
                        q = self._pending.get(rid)
                    if q is not None:
                        q.put(msg)
                        print("[CLIENT] recv(resp):", msg)
                        continue

                print("[CLIENT] recv:", msg)
        except asyncio.CancelledError:
            return
        except Exception as e:
            print("[CLIENT] recv error:", e)

    # ------------ 内部：日志打印 ------------
    def _print_log_event(self, topic: str, payload: dict):
        """
        payload 结构：
          {
            "type": "log",
            "engine": "...",
            "ts":  ...,
            "epoch": E,
            "seq":  S,
            "data": {
              "path": "...",
              "count": N,
              "range": {"start": "...", "end": "..."},
              "lines": ["...", "...", ...]
            }
          }
        """
        data = payload.get("data") or {}
        path = data.get("path")
        cnt  = data.get("count")
        rng  = data.get("range") or {}
        lines = data.get("lines") or []

        hdr = f"[CLIENT] LOG topic={topic} path={path} count={cnt} range=({rng.get('start')} ~ {rng.get('end')})"
        print(hdr)
        if not lines:
            print("  (no lines)")
            return
        for ln in lines:
            print("  |", ln)

    # ------------ 内部：RPC 封装 ------------
    def _next_id(self) -> int:
        self._id += 1
        return self._id

    def _send_rpc(self, method: str, params: dict | None = None) -> int:
        rid = self._next_id()
        req = {"jsonrpc": "2.0", "method": method, "params": params or {}, "id": rid}
        with self._pending_lock:
            if rid not in self._pending:
                self._pending[rid] = Queue()
        self._out_q.put_nowait(json.dumps(req))
        return rid

    async def _send_rpc_now(self, ws, method: str, params: dict | None = None):
        rid = self._next_id()
        req = {"jsonrpc": "2.0", "method": method, "params": params or {}, "id": rid}
        with self._pending_lock:
            if rid not in self._pending:
                self._pending[rid] = Queue()
        await ws.send(json.dumps(req))

    def _await_response(self, rid: int, timeout: float = 5.0):
        q = None
        with self._pending_lock:
            q = self._pending.get(rid)
        if q is None:
            return None
        try:
            resp = q.get(timeout=timeout)
            return resp
        except Empty:
            return None
        finally:
            with self._pending_lock:
                self._pending.pop(rid, None)


cli = SimWsClient(engine="test")
cli.start()
cli.query_logs(engine="test", date="2025-08-08", level=["INFO","ERROR","WARNING"], limit=20)

time.sleep(1.0)

# cta_adapter发送订单数据
import pandas as pd
emit_loop(date=pd.to_datetime('2023-01-03'),orderid='a1')
emit_loop(date=pd.to_datetime('2023-02-03'),orderid='a2')
emit_loop(date=pd.to_datetime('2023-03-03'),orderid='a3')
emit_loop(date=pd.to_datetime('2023-04-03'),orderid='a4')
emit_loop(date=pd.to_datetime('2023-06-03'),orderid='a5')

emit_position_loop()
print('='*1000)
# 客户端发送命令
cli.send_command("test", "snapshot", {"x": 2})
cli.send_command("test", "xxx", {"log_path": '/:log.txt'})
cli.query_logs(engine="test", date="2025-08-08", level=["INFO","ERROR","WARNING"], limit=20)
oms.get_all_orders()

from engine.event_engine import EventEngine, Event, EVENT_ORDER, EVENT_POSITION
from engine.oms_engine import OmsBase

# 你的真实数据类/枚举（按你的工程路径导入）
from coreutils.object import OrderData, PositionData
from coreutils.constant import Exchange, Direction, OrderType, OrderStatus
import pandas as pd
ee = EventEngine()
ee.start()
oms = OmsBase(event_engine=ee)

od = OrderData(
    symbol="code233",
    exchange=Exchange.HKFE,
    orderid="order_id_13",
    type=OrderType.MARKET,
    direction=Direction.LONG,
    volume=1,
    price=1.0,
    gateway_name="FUTU",
    status=OrderStatus.ALLCANCELLED,
    traded=0,
    avgFillPrice=0.0,
    broker_orderid="broker_1",
    reference="ref_1",datetime=pd.to_datetime("2025-01-04 08:00:08")
)
ee.put(Event(EVENT_ORDER, od))
oms.filter_orders(start_date='2022-01-05',end_date='2023-08-08')

cmd_data = {}
def handle_cmd(event:Event):
    global cmd_data
    cmd_data = event.data
    print(cmd_data)
ee.register(EVENT_CTA_COMMAND, handle_cmd)