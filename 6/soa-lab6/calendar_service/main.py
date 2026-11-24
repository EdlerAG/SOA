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
import pika
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

SERVICE_NAME = "calendar-service"
HOST_ID = socket.gethostname()
CONSUL_ID = f"{SERVICE_NAME}-{HOST_ID}"
CONSUL_URL = f"http://{os.getenv('CONSUL_HOST', 'consul')}:8500"
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = "calendar_events_queue"
DB_PATH = os.getenv("DB_PATH", "calendar.db")
LOKI_URL = os.getenv("LOKI_URL", "").strip()

_rr_lock = threading.Lock()
_rr_index: dict[str, int] = {}

REQ_COUNT = Counter("http_requests_total", "Total HTTP requests", ["service", "method", "endpoint", "status"])
REQ_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["service", "method", "endpoint"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)
EVENTS_CREATED = Counter("calendar_events_created_total", "Business metric: total calendar events created")


class CreateEventRequest(BaseModel):
    user_id: int = 1
    text: str
    source: str = "web"


class IncidentTicket(BaseModel):
    ticket_id: str
    status: str = "firing"
    severity: str = "warning"
    alertname: str
    service: str
    summary: str
    description: str = ""
    created_at: str = ""


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
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                title TEXT,
                scheduled_at TEXT,
                priority TEXT,
                source TEXT,
                status TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS incidents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id TEXT,
                status TEXT,
                severity TEXT,
                alertname TEXT,
                service TEXT,
                summary TEXT,
                description TEXT,
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


def pick_service_url(service_name: str) -> str:
    res = httpx.get(f"{CONSUL_URL}/v1/health/service/{service_name}?passing=true", timeout=4.0)
    res.raise_for_status()
    nodes = sorted(res.json(), key=lambda x: x["Service"]["ID"])
    if not nodes:
        raise HTTPException(status_code=503, detail=f"Немає активних інстансів {service_name}")
    with _rr_lock:
        idx = _rr_index.get(service_name, 0) % len(nodes)
        _rr_index[service_name] = idx + 1
    svc = nodes[idx]["Service"]
    return f"http://{svc['Address']}:{svc['Port']}"


def publish_event(payload: dict):
    conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, socket_timeout=3))
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE_NAME, durable=True)
    ch.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(payload, ensure_ascii=False),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    conn.close()


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    register_in_consul()
    yield
    deregister_from_consul()


app = FastAPI(title="Calendar Service (Observability & Incident Receiver)", lifespan=lifespan)


@app.middleware("http")
async def observability_middleware(request: Request, call_next):
    trace_id = request.headers.get("X-Trace-ID") or uuid.uuid4().hex[:16]
    request.state.trace_id = trace_id
    start = time.perf_counter()
    try:
        response = await call_next(request)
        status_code = response.status_code
    except Exception as exc:
        elapsed = time.perf_counter() - start
        path = request.url.path
        REQ_COUNT.labels(SERVICE_NAME, request.method, path, "500").inc()
        REQ_LATENCY.labels(SERVICE_NAME, request.method, path).observe(elapsed)
        log_json("ERROR", trace_id, "unhandled_exception", method=request.method, path=path, error=str(exc))
        raise
    elapsed = time.perf_counter() - start
    path = request.url.path
    if path not in ("/metrics", "/health"):
        REQ_COUNT.labels(SERVICE_NAME, request.method, path, str(status_code)).inc()
        REQ_LATENCY.labels(SERVICE_NAME, request.method, path).observe(elapsed)
        log_json("INFO", trace_id, "http_request", method=request.method, path=path, status=status_code, duration_ms=round(elapsed * 1000, 2))
    response.headers["X-Trace-ID"] = trace_id
    return response


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME, "instance": HOST_ID}


@app.get("/events")
def list_events():
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        rows = conn.execute("SELECT id, user_id, title, scheduled_at, priority, source, status FROM events ORDER BY id DESC").fetchall()
    return {
        "instance": HOST_ID,
        "events": [{"id": r[0], "user_id": r[1], "title": r[2], "scheduled_at": r[3], "priority": r[4], "source": r[5], "status": r[6]} for r in rows],
    }


@app.get("/events/simulate-slow")
def simulate_slow(request: Request):
    time.sleep(0.8)
    log_json("WARN", request.state.trace_id, "simulated_high_latency", delay_sec=0.8)
    return {"status": "slow_ok", "delay_sec": 0.8, "trace_id": request.state.trace_id}


@app.get("/events/simulate-error")
def simulate_error(request: Request):
    log_json("ERROR", request.state.trace_id, "simulated_500_error")
    raise HTTPException(status_code=500, detail="Симульована критична помилка для перевірки алертингу")


@app.post("/events", status_code=201)
def create_event(req: CreateEventRequest, request: Request):
    trace_id = request.state.trace_id
    fwd_headers = {"X-Trace-ID": trace_id}
    trace = [f"{SERVICE_NAME}:{HOST_ID}"]

    user_url = pick_service_url("user-service")
    user_resp = httpx.get(f"{user_url}/users/{req.user_id}", headers=fwd_headers, timeout=5.0)
    if user_resp.status_code != 200:
        raise HTTPException(status_code=404, detail="Користувача не знайдено")
    user_info = user_resp.json()
    trace.append(user_info["instance"])

    nlp_url = pick_service_url("nlp-service")
    nlp_resp = httpx.post(
        f"{nlp_url}/nlp/parse",
        json={"text": req.text, "timezone": user_info["user"]["timezone"]},
        headers=fwd_headers,
        timeout=5.0,
    )
    parsed = nlp_resp.json()
    trace.append(parsed["instance"])

    priority = parsed.get("priority", "normal")
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        cur = conn.execute(
            "INSERT INTO events (user_id, title, scheduled_at, priority, source, status) VALUES (?, ?, ?, ?, ?, ?)",
            (req.user_id, parsed["title"], parsed["scheduled_at"], priority, req.source, "АКТИВНА"),
        )
        event_id = cur.lastrowid

    EVENTS_CREATED.inc()
    publish_event({
        "trace_id": trace_id,
        "published_at": time.time(),
        "action": "СТВОРЕНО",
        "event_id": event_id,
        "title": parsed["title"],
        "scheduled_at": parsed["scheduled_at"],
        "priority": priority,
        "user": user_info["user"],
    })
    log_json("INFO", trace_id, "event_created_and_published", event_id=event_id, title=parsed["title"])

    return {
        "event_id": event_id,
        "trace_id": trace_id,
        "title": parsed["title"],
        "scheduled_at": parsed["scheduled_at"],
        "priority": priority,
        "status": "АКТИВНА",
        "trace": " -> ".join(trace),
    }


@app.post("/incidents", status_code=201)
def create_incident(ticket: IncidentTicket, request: Request):
    created_at = ticket.created_at or datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        conn.execute(
            """INSERT INTO incidents (ticket_id, status, severity, alertname, service, summary, description, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticket.ticket_id, ticket.status, ticket.severity, ticket.alertname, ticket.service, ticket.summary, ticket.description, created_at),
        )
    log_json("WARN", request.state.trace_id, "incident_ticket_created", ticket_id=ticket.ticket_id, alertname=ticket.alertname, service=ticket.service)
    return {"saved": True, "ticket_id": ticket.ticket_id}


@app.get("/incidents")
def list_incidents():
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        rows = conn.execute(
            "SELECT id, ticket_id, status, severity, alertname, service, summary, description, created_at FROM incidents ORDER BY id DESC"
        ).fetchall()
    return {
        "total": len(rows),
        "incidents": [
            {"id": r[0], "ticket_id": r[1], "status": r[2], "severity": r[3], "alertname": r[4], "service": r[5], "summary": r[6], "description": r[7], "created_at": r[8]}
            for r in rows
        ],
    }
