import os
import re
import json
import time
import uuid
import socket
import sqlite3
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import httpx
from fastapi import FastAPI, Request, Response
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

SERVICE_NAME = "nlp-service"
HOST_ID = socket.gethostname()
CONSUL_ID = f"{SERVICE_NAME}-{HOST_ID}"
CONSUL_URL = f"http://{os.getenv('CONSUL_HOST', 'consul')}:8500"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
DB_PATH = os.getenv("DB_PATH", "nlp_history.db")
LOKI_URL = os.getenv("LOKI_URL", "").strip()

REQ_COUNT = Counter("http_requests_total", "Total HTTP requests", ["service", "method", "endpoint", "status"])
REQ_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["service", "method", "endpoint"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)


class ParseRequest(BaseModel):
    text: str
    timezone: str = "Europe/Kyiv"


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
            CREATE TABLE IF NOT EXISTS parse_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_text TEXT,
                parsed_title TEXT,
                scheduled_at TEXT,
                priority TEXT,
                parser_mode TEXT,
                created_at TEXT
            )
        """)


def register_in_consul():
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


def deregister_from_consul():
    try:
        httpx.put(f"{CONSUL_URL}/v1/agent/service/deregister/{CONSUL_ID}", timeout=2.0)
    except httpx.HTTPError:
        pass


def get_local_now(tz_name: str) -> datetime:
    try:
        return datetime.now(ZoneInfo(tz_name))
    except ZoneInfoNotFoundError:
        return datetime.now(ZoneInfo("Europe/Kyiv"))


def parse_locally(text: str, now_local: datetime) -> dict:
    lowered = text.lower()
    days_offset = 2 if "післязавтра" in lowered else (0 if "сьогодні" in lowered else 1)
    time_match = re.search(r"(\d{1,2}[:.]\d{2})", text)
    event_time = time_match.group(1).replace(".", ":") if time_match else "12:00"
    if len(event_time) == 4:
        event_time = "0" + event_time
    priority = "high" if any(k in lowered for k in ("терміново", "важливо", "дедлайн")) else "normal"
    target_date = (now_local + timedelta(days=days_offset)).strftime("%Y-%m-%d")
    title = re.sub(
        r"(післязавтра|завтра|сьогодні|(?:о|на)\s*\d{1,2}[:.]\d{2}|нагадай\s*мені\s*про|нагадай\s*про|нагадай|терміново|важливо)",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip(" ,.-!")
    return {"title": title or "Нова подія", "scheduled_at": f"{target_date} {event_time}", "priority": priority}


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    register_in_consul()
    yield
    deregister_from_consul()


app = FastAPI(title="NLP Service (Observability)", lifespan=lifespan)


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


@app.post("/nlp/parse")
def parse_text(req: ParseRequest, request: Request):
    trace_id = request.headers.get("X-Trace-ID", "")
    now_local = get_local_now(req.timezone)
    parsed = parse_locally(req.text, now_local)
    mode = "local_rules"

    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        conn.execute(
            "INSERT INTO parse_logs (raw_text, parsed_title, scheduled_at, priority, parser_mode, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (req.text, parsed["title"], parsed["scheduled_at"], parsed["priority"], mode, now_local.strftime("%Y-%m-%d %H:%M:%S")),
        )

    log_json("INFO", trace_id, "nlp_parsed", title=parsed["title"], scheduled_at=parsed["scheduled_at"], priority=parsed["priority"])
    return {
        "title": parsed["title"],
        "scheduled_at": parsed["scheduled_at"],
        "priority": parsed["priority"],
        "parser_mode": mode,
        "timezone": req.timezone,
        "trace_id": trace_id,
        "instance": f"{SERVICE_NAME}:{HOST_ID}",
    }


@app.get("/nlp/history")
def get_history():
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        rows = conn.execute("SELECT id, raw_text, parsed_title, scheduled_at, priority, parser_mode, created_at FROM parse_logs ORDER BY id DESC").fetchall()
    return {
        "instance": HOST_ID,
        "logs": [{"id": r[0], "raw_text": r[1], "title": r[2], "scheduled_at": r[3], "priority": r[4], "parser_mode": r[5], "created_at": r[6]} for r in rows],
    }
