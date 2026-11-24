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
from fastapi import FastAPI, Request, Response
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

SERVICE_NAME = "notification-service"
HOST_ID = socket.gethostname()
CONSUL_ID = f"{SERVICE_NAME}-{HOST_ID}"
CONSUL_URL = f"http://{os.getenv('CONSUL_HOST', 'consul')}:8500"
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = "calendar_events_queue"
DB_PATH = os.getenv("DB_PATH", "notifications.db")
LOKI_URL = os.getenv("LOKI_URL", "").strip()

REQ_COUNT = Counter("http_requests_total", "Total HTTP requests", ["service", "method", "endpoint", "status"])
REQ_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["service", "method", "endpoint"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)
NOTIF_PROCESSED = Counter("notifications_processed_total", "Business metric: notifications processed from RabbitMQ")
QUEUE_LAG = Gauge("queue_lag_seconds", "Message queue processing lag in seconds", ["service"])


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
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER,
                trace_id TEXT,
                action TEXT,
                channel TEXT,
                recipient TEXT,
                priority TEXT,
                text TEXT,
                processed_by TEXT,
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


def consume_loop():
    while True:
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            ch = conn.channel()
            ch.queue_declare(queue=QUEUE_NAME, durable=True)

            def on_message(channel, method, properties, body):
                data = json.loads(body.decode("utf-8"))
                trace_id = data.get("trace_id") or uuid.uuid4().hex[:16]
                published_at = data.get("published_at")
                if published_at is not None:
                    QUEUE_LAG.labels(service=SERVICE_NAME).set(round(max(0.0, time.time() - float(published_at)), 4))
                user = data["user"]
                priority = data.get("priority", "normal")
                text = f"[{data['action']}] Подія '{data['title']}' на {data['scheduled_at']}"
                created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                with sqlite3.connect(DB_PATH, timeout=15.0) as db:
                    db.execute(
                        """INSERT INTO notifications (event_id, trace_id, action, channel, recipient, priority, text, processed_by, created_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (data["event_id"], trace_id, data["action"], user["channel"], user["contact"], priority, text, HOST_ID, created_at),
                    )
                NOTIF_PROCESSED.inc()
                log_json("INFO", trace_id, "notification_dispatched", event_id=data["event_id"], channel=user["channel"], recipient=user["contact"])
                channel.basic_ack(delivery_tag=method.delivery_tag)

            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)
            ch.start_consuming()
        except Exception:
            time.sleep(4)


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    register_in_consul()
    threading.Thread(target=consume_loop, daemon=True).start()
    yield
    deregister_from_consul()


app = FastAPI(title="Notification Service (Observability)", lifespan=lifespan)


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


@app.get("/notifications")
def get_notifications():
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        rows = conn.execute(
            "SELECT id, event_id, trace_id, action, channel, recipient, priority, text, processed_by, created_at FROM notifications ORDER BY id DESC"
        ).fetchall()
    return {
        "instance": HOST_ID,
        "total": len(rows),
        "notifications": [
            {"id": r[0], "event_id": r[1], "trace_id": r[2], "action": r[3], "channel": r[4], "recipient": r[5], "priority": r[6], "text": r[7], "processed_by": r[8], "created_at": r[9]}
            for r in rows
        ],
    }
