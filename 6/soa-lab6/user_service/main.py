import os
import json
import time
import uuid
import socket
import sqlite3
import threading
from datetime import datetime, timezone
from contextlib import asynccontextmanager
import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

SERVICE_NAME = "user-service"
HOST_ID = socket.gethostname()
CONSUL_ID = f"{SERVICE_NAME}-{HOST_ID}"
CONSUL_URL = f"http://{os.getenv('CONSUL_HOST', 'consul')}:8500"
DB_PATH = os.getenv("DB_PATH", "users.db")
LOKI_URL = os.getenv("LOKI_URL", "").strip()

REQ_COUNT = Counter("http_requests_total", "Total HTTP requests", ["service", "method", "endpoint", "status"])
REQ_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["service", "method", "endpoint"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)


class UserPayload(BaseModel):
    name: str
    timezone: str = "Europe/Kyiv"
    channel: str = "telegram"
    contact: str


def _send_loki(level: str, line: str):
    if not LOKI_URL:
        return
    try:
        payload = {"streams": [{"stream": {"service": SERVICE_NAME, "level": level, "instance": HOST_ID}, "values": [[str(time.time_ns()), line]]}]}
        httpx.post(LOKI_URL, json=payload, timeout=1.0)
    except Exception:
        pass


def log_json(level: str, trace_id: str, event: str, **extra):
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "service": SERVICE_NAME,
        "instance": HOST_ID,
        "trace_id": trace_id,
        "event": event,
        **extra,
    }
    line = json.dumps(record, ensure_ascii=False)
    print(line, flush=True)
    if LOKI_URL:
        threading.Thread(target=_send_loki, args=(level, line), daemon=True).start()


def init_db():
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                timezone TEXT NOT NULL,
                channel TEXT NOT NULL,
                contact TEXT NOT NULL
            )
        """)
        conn.execute("""
            INSERT OR IGNORE INTO users (id, name, timezone, channel, contact)
            VALUES (1, 'Ярослав Обіход', 'Europe/Kyiv', 'telegram', '@yaroslav_kpi')
        """)


def consul_register():
    body = {
        "ID": CONSUL_ID,
        "Name": SERVICE_NAME,
        "Address": HOST_ID,
        "Port": 8000,
        "Check": {"HTTP": f"http://{HOST_ID}:8000/health", "Interval": "10s", "Timeout": "5s"},
    }
    for _ in range(10):
        try:
            res = httpx.put(f"{CONSUL_URL}/v1/agent/service/register", json=body, timeout=3.0)
            if res.status_code == 200:
                return
        except httpx.HTTPError:
            time.sleep(2)


def consul_deregister():
    try:
        httpx.put(f"{CONSUL_URL}/v1/agent/service/deregister/{CONSUL_ID}", timeout=2.0)
    except httpx.HTTPError:
        pass


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    consul_register()
    yield
    consul_deregister()


app = FastAPI(title="User Service (Observability)", lifespan=lifespan)


@app.middleware("http")
async def observability_middleware(request: Request, call_next):
    trace_id = request.headers.get("X-Trace-ID") or uuid.uuid4().hex[:16]
    start = time.perf_counter()
    response = await call_next(request)
    elapsed = time.perf_counter() - start
    path = request.url.path
    if path not in ("/metrics", "/health"):
        REQ_COUNT.labels(SERVICE_NAME, request.method, path, str(response.status_code)).inc()
        REQ_LATENCY.labels(SERVICE_NAME, request.method, path).observe(elapsed)
        log_json("INFO", trace_id, "http_request", method=request.method, path=path, status=response.status_code, duration_ms=round(elapsed * 1000, 2))
    response.headers["X-Trace-ID"] = trace_id
    return response


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME, "instance": HOST_ID}


@app.get("/users")
def list_users():
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        rows = conn.execute("SELECT id, name, timezone, channel, contact FROM users ORDER BY id").fetchall()
    return {"instance": HOST_ID, "users": [{"id": r[0], "name": r[1], "timezone": r[2], "channel": r[3], "contact": r[4]} for r in rows]}


@app.get("/users/{user_id}")
def get_user(user_id: int, request: Request):
    trace_id = request.headers.get("X-Trace-ID", "")
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        row = conn.execute("SELECT id, name, timezone, channel, contact FROM users WHERE id = ?", (user_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Користувача не знайдено")
    return {
        "user": {"id": row[0], "name": row[1], "timezone": row[2], "channel": row[3], "contact": row[4]},
        "trace_id": trace_id,
        "instance": f"{SERVICE_NAME}:{HOST_ID}",
    }


@app.post("/users", status_code=201)
def create_user(data: UserPayload):
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        cur = conn.execute("INSERT INTO users (name, timezone, channel, contact) VALUES (?, ?, ?, ?)", (data.name, data.timezone, data.channel, data.contact))
        user_id = cur.lastrowid
    return {"id": user_id, "name": data.name, "channel": data.channel, "instance": HOST_ID}
