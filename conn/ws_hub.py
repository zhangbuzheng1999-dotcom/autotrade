# conn/ws_hub.py
from __future__ import annotations
import asyncio, time, jwt, contextlib, json
from datetime import datetime, timedelta, timezone
from typing import Dict, Set, Any

import zmq
import zmq.asyncio as azmq
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
import uvicorn

from coreutils.auth.user_manager import UserManager

# ===== 配置 =====
SECRET_KEY = "supersecretkey"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 10
REFRESH_TOKEN_EXPIRE_DAYS = 7

EVENTS_BIND = "tcp://*:7001"  # CtaBus PUB.connect -> SUB.bind
CMDS_BIND = "tcp://*:7002"  # CtaBus SUB.connect <- PUB.bind
EVENT_PREFIX = "order:"  # 上行事件主题：order:<engine>
CMD_PREFIX = "cmd:"  # 下行命令主题：cmd:<engine> / cmd:all

# ===== 健壮性参数 =====
WS_SEND_TIMEOUT = 1.0  # 单次向某客户端 send_json 的超时（秒）
WS_PING_INTERVAL = 15  # 向所有在线客户端发送应用层 ping 的间隔（秒）
WS_CLIENT_TIMEOUT = 45  # 客户端最长无活动时间（秒），超时将被清理

ZMQ_RCVHWM = 100_000  # Hub 收事件的高水位
ZMQ_SNDHWM = 100_000  # Hub 发命令的高水位

app = FastAPI()
user_manager = UserManager()

subscriptions: Dict[str, Set[WebSocket]] = {}
ws_identities: Dict[WebSocket, str] = {}
ws_last_seen: Dict[WebSocket, float] = {}

_zmq_ctx: azmq.Context | None = None
_zmq_sub = None  # type: ignore
_zmq_pub = None  # type: ignore
_zmq_rx_task: asyncio.Task | None = None


# ===== JWT =====
def _jwt_create(username: str, minutes: int) -> str:
    exp = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    return jwt.encode({"sub": username, "exp": int(exp.timestamp())}, SECRET_KEY, algorithm=ALGORITHM)


def create_access_token(username: str) -> str:
    return _jwt_create(username, ACCESS_TOKEN_EXPIRE_MINUTES)


def create_refresh_token(username: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    return jwt.encode({"sub": username, "exp": int(exp.timestamp()), "type": "refresh"}, SECRET_KEY,
                      algorithm=ALGORITHM)


def verify_token(token: str) -> str | None:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("sub")
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


# ===== JSON-RPC =====
def rpc_result(_id, result): return {"jsonrpc": "2.0", "result": result, "id": _id}


def rpc_error(_id, code, message, data=None):
    err = {"code": code, "message": message}
    if data is not None: err["data"] = data
    return {"jsonrpc": "2.0", "error": err, "id": _id}


async def rpc_notify(ws: WebSocket, method: str, params: dict):
    """发送带超时；慢连接直接清理，避免阻塞广播。"""
    try:
        await asyncio.wait_for(
            ws.send_json({"jsonrpc": "2.0", "method": method, "params": params}),
            timeout=WS_SEND_TIMEOUT,
        )
    except Exception:
        _cleanup_ws(ws)


# ===== ZMQ 上行事件 → WS 通知（原样透传 payload）=====
async def _zmq_event_pump():
    global _zmq_sub
    assert _zmq_sub is not None
    while True:
        try:
            topic_b, data_b = await _zmq_sub.recv_multipart()
            topic = topic_b.decode("utf-8")
            payload = json.loads(data_b.decode("utf-8"))  # {type,engine,ts,epoch,seq,data...}
            for ws in list(subscriptions.get(topic, set())):
                await rpc_notify(ws, "event.emit", {"topic": topic, "data": payload})
        except asyncio.CancelledError:
            break
        except Exception as e:
            print("[ws_hub] zmq pump error:", e)
            await asyncio.sleep(0.01)


async def _ws_global_pinger():
    """应用层 ping/pong 与闲置清理。"""
    while True:
        try:
            now = time.time()
            # 发 ping；慢/坏连接会在 rpc_notify 里被清理
            for ws in list(ws_identities.keys()):
                await rpc_notify(ws, "meta.ping", {"ts": int(now)})

            # 踢掉长时间无活动的连接
            for ws, seen in list(ws_last_seen.items()):
                if (now - seen) > WS_CLIENT_TIMEOUT:
                    _cleanup_ws(ws)
                    with contextlib.suppress(Exception):
                        await ws.close()
            await asyncio.sleep(WS_PING_INTERVAL)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print("[ws_hub] pinger error:", e)
            await asyncio.sleep(1.0)


# ===== 生命周期 =====
@app.on_event("startup")
async def startup():
    global _zmq_ctx, _zmq_sub, _zmq_pub, _zmq_rx_task
    _zmq_ctx = azmq.Context.instance()

    _zmq_sub = _zmq_ctx.socket(zmq.SUB)
    _zmq_sub.setsockopt(zmq.LINGER, 0)
    _zmq_sub.setsockopt(zmq.RCVHWM, ZMQ_RCVHWM)
    _zmq_sub.bind(EVENTS_BIND)
    _zmq_sub.setsockopt_string(zmq.SUBSCRIBE, EVENT_PREFIX)

    _zmq_pub = _zmq_ctx.socket(zmq.PUB)
    _zmq_pub.setsockopt(zmq.LINGER, 0)
    _zmq_pub.setsockopt(zmq.SNDHWM, ZMQ_SNDHWM)
    _zmq_pub.bind(CMDS_BIND)

    _zmq_rx_task = asyncio.create_task(_zmq_event_pump())
    app.state._pinger_task = asyncio.create_task(_ws_global_pinger())
    print(f"[ws_hub] up: SUB {EVENTS_BIND} ({EVENT_PREFIX}*), PUB {CMDS_BIND} ({CMD_PREFIX}*)")


@app.on_event("shutdown")
async def shutdown():
    global _zmq_ctx, _zmq_sub, _zmq_pub, _zmq_rx_task
    if _zmq_rx_task:
        _zmq_rx_task.cancel()
        with contextlib.suppress(Exception): await _zmq_rx_task
    t = getattr(app.state, "_pinger_task", None)
    if t:
        t.cancel()
        with contextlib.suppress(Exception): await t
    if _zmq_sub: _zmq_sub.close(0)
    if _zmq_pub: _zmq_pub.close(0)
    if _zmq_ctx: _zmq_ctx.term()


def _cleanup_ws(ws: WebSocket):
    for s in subscriptions.values(): s.discard(ws)
    ws_identities.pop(ws, None)
    ws_last_seen.pop(ws, None)


async def _send_zmq_command(engine: str, cmd: str, data: dict | None):
    assert _zmq_pub is not None
    topic = f"{CMD_PREFIX}{engine}"  # engine 可为具体引擎或 "all"
    payload = {"cmd": cmd, "data": (data or {}), "ts": int(time.time())}
    await _zmq_pub.send_multipart([topic.encode("utf-8"), json.dumps(payload, ensure_ascii=False).encode("utf-8")])


# ===== HTTP =====
@app.post("/login")
async def login(data: dict):
    username, password = data.get("username"), data.get("password")
    if user_manager.verify_user(username, password):
        return {"access_token": create_access_token(username), "refresh_token": create_refresh_token(username)}
    return JSONResponse({"error": "Invalid credentials"}, status_code=401)


@app.post("/refresh")
async def refresh(data: dict):
    token = data.get("refresh_token")
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("type") != "refresh": raise jwt.InvalidTokenError
        return {"access_token": create_access_token(payload.get("sub"))}
    except jwt.ExpiredSignatureError:
        return JSONResponse({"error": "Refresh token expired"}, status_code=401)
    except jwt.InvalidTokenError:
        return JSONResponse({"error": "Invalid refresh token"}, status_code=401)


# ===== WebSocket（JSON-RPC 2.0）=====
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    authed = False
    ws_last_seen[ws] = time.time()  # 初始活跃
    try:
        while True:
            req = await ws.receive_json()
            ws_last_seen[ws] = time.time()  # 任何消息都刷新活跃时间

            if not isinstance(req, dict) or req.get("jsonrpc") != "2.0" or "method" not in req:
                await ws.send_json(rpc_error(req.get("id"), -32600, "Invalid Request"));
                continue
            method, params, req_id = req["method"], (req.get("params") or {}), req.get("id")

            if method == "auth.login":
                user = verify_token(params.get("access_token"))
                if not user: await ws.send_json(rpc_error(req_id, -32001, "Unauthorized")); continue
                ws_identities[ws] = user;
                authed = True
                ws_last_seen[ws] = time.time()
                await ws.send_json(rpc_result(req_id, {"user": user, "status": "connected"}));
                continue

            if not authed:
                await ws.send_json(rpc_error(req_id, -32001, "Unauthorized"));
                continue

            # 心跳响应（客户端回 meta.pong）
            if method == "meta.pong":
                ws_last_seen[ws] = time.time()
                await ws.send_json(rpc_result(req_id, {"ok": True}));
                continue

            if method == "sub.subscribe":
                topics = params.get("topics") or []
                if not isinstance(topics, list):
                    await ws.send_json(rpc_error(req_id, -32602, "Invalid params: topics must be list"));
                    continue
                for t in topics: subscriptions.setdefault(t, set()).add(ws)
                await ws.send_json(rpc_result(req_id, {"status": "subscribed", "topics": topics}));
                continue

            if method == "sub.unsubscribe":
                topics = params.get("topics") or []
                if not isinstance(topics, list):
                    await ws.send_json(rpc_error(req_id, -32602, "Invalid params: topics must be list"));
                    continue
                for t in topics:
                    if t in subscriptions: subscriptions[t].discard(ws)
                await ws.send_json(rpc_result(req_id, {"status": "unsubscribed", "topics": topics}));
                continue

            if method == "engine.command":
                engine, cmd, data = params.get("engine"), params.get("cmd"), params.get("data") or {}
                if not engine or not cmd:
                    await ws.send_json(rpc_error(req_id, -32602, "Invalid params: require engine & cmd"));
                    continue
                await _send_zmq_command(engine=engine, cmd=cmd, data=data)
                await ws.send_json(rpc_result(req_id, {"status": "sent", "engine": engine, "cmd": cmd}));
                continue

            await ws.send_json(rpc_error(req_id, -32601, f"Method not found: {method}"))
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print("[ws_hub] error:", e)
    finally:
        _cleanup_ws(ws)
        with contextlib.suppress(Exception):
            await ws.close()


if __name__ == "__main__":
    # 便于手动跑：预置一个用户
    user_manager.add_user("testuser", "testpassword")
    host = "0.0.0.0"
    port = 8000
    print("[ws_hub] listening on %s:%d" % (host, port))
    uvicorn.run(app, host=host, port=port)

