import os
import json
import time
import socket
import sqlite3
import threading
from contextlib import asynccontextmanager
import httpx
import pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

SERVICE_NAME = "calendar-service"
HOST_ID = socket.gethostname()
CONSUL_ID = f"{SERVICE_NAME}-{HOST_ID}"
CONSUL_URL = f"http://{os.getenv('CONSUL_HOST', 'consul')}:8500"
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = "calendar_events_queue"
DB_PATH = os.getenv("DB_PATH", "calendar.db")

_rr_lock = threading.Lock()
_rr_index: dict[str, int] = {}

# Простий Circuit Breaker: при 3 збоях поспіль відкривається на 15с
_cb_state: dict[str, dict] = {}
CB_FAILURE_THRESHOLD = 3
CB_OPEN_SECONDS = 15


class CreateEventRequest(BaseModel):
    user_id: int = 1
    text: str
    source: str = "web"


class UpdateEventRequest(BaseModel):
    title: str
    scheduled_at: str
    status: str = "ОНОВЛЕНО"


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
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


def register_in_consul():
    body = {
        "ID": CONSUL_ID,
        "Name": SERVICE_NAME,
        "Address": HOST_ID,
        "Port": 8000,
        "Check": {
            "HTTP": f"http://{HOST_ID}:8000/health",
            "Interval": "10s",
            "Timeout": "5s",
        },
    }
    for _ in range(10):
        try:
            res = httpx.put(f"{CONSUL_URL}/v1/agent/service/register", json=body, timeout=3.0)
            if res.status_code == 200:
                print(f"[Consul] {CONSUL_ID} registered", flush=True)
                return
        except httpx.HTTPError:
            time.sleep(2)


def deregister_from_consul():
    try:
        httpx.put(f"{CONSUL_URL}/v1/agent/service/deregister/{CONSUL_ID}", timeout=2.0)
    except httpx.HTTPError:
        pass


def check_circuit(service_name: str):
    state = _cb_state.get(service_name)
    if state and state["open_until"] > time.time():
        raise HTTPException(
            status_code=503,
            detail=f"Circuit Breaker відкритий для {service_name}, повторіть пізніше",
        )


def record_success(service_name: str):
    _cb_state[service_name] = {"failures": 0, "open_until": 0.0}


def record_failure(service_name: str):
    state = _cb_state.setdefault(service_name, {"failures": 0, "open_until": 0.0})
    state["failures"] += 1
    if state["failures"] >= CB_FAILURE_THRESHOLD:
        state["open_until"] = time.time() + CB_OPEN_SECONDS
        print(f"[CircuitBreaker] OPEN для {service_name} на {CB_OPEN_SECONDS}с", flush=True)


def pick_service_url(service_name: str) -> str:
    check_circuit(service_name)
    try:
        res = httpx.get(f"{CONSUL_URL}/v1/health/service/{service_name}?passing=true", timeout=4.0)
        res.raise_for_status()
        nodes = sorted(res.json(), key=lambda x: x["Service"]["ID"])
    except Exception as e:
        record_failure(service_name)
        raise HTTPException(status_code=503, detail=f"Помилка Consul Discovery: {e}")

    if not nodes:
        record_failure(service_name)
        raise HTTPException(status_code=503, detail=f"Немає активних інстансів {service_name}")

    with _rr_lock:
        idx = _rr_index.get(service_name, 0) % len(nodes)
        _rr_index[service_name] = idx + 1

    svc = nodes[idx]["Service"]
    print(f"[Discovery] {service_name}: {len(nodes)} реплік -> {svc['ID']} (RR {idx + 1}/{len(nodes)})", flush=True)
    return f"http://{svc['Address']}:{svc['Port']}"


def call_service(service_name: str, method: str, path: str, json_body: dict | None = None) -> httpx.Response:
    url = pick_service_url(service_name)
    try:
        resp = httpx.request(method, f"{url}{path}", json=json_body, timeout=5.0)
        if resp.status_code >= 500:
            record_failure(service_name)
            raise HTTPException(status_code=502, detail=f"Збій сервісу {service_name}: HTTP {resp.status_code}")
        record_success(service_name)
        return resp
    except httpx.RequestError as e:
        record_failure(service_name)
        raise HTTPException(status_code=503, detail=f"Сервіс {service_name} не відповідає: {e}")


def publish_event(payload: dict):
    try:
        params = pika.ConnectionParameters(host=RABBITMQ_HOST, socket_timeout=3, blocked_connection_timeout=3)
        conn = pika.BlockingConnection(params)
        ch = conn.channel()
        ch.queue_declare(queue=QUEUE_NAME, durable=True)
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(payload, ensure_ascii=False),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"RabbitMQ недоступний: {e}")


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    register_in_consul()
    yield
    deregister_from_consul()


app = FastAPI(title="Calendar Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME, "instance": HOST_ID}


@app.get("/events")
def list_events():
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT id, user_id, title, scheduled_at, priority, source, status FROM events ORDER BY id DESC"
        ).fetchall()
    return {
        "instance": HOST_ID,
        "events": [
            {
                "id": r[0],
                "user_id": r[1],
                "title": r[2],
                "scheduled_at": r[3],
                "priority": r[4],
                "source": r[5],
                "status": r[6],
            }
            for r in rows
        ],
    }


@app.get("/events/{event_id}")
def get_event(event_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT id, user_id, title, scheduled_at, priority, source, status FROM events WHERE id = ?",
            (event_id,),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Подію не знайдено")
    return {
        "id": row[0],
        "user_id": row[1],
        "title": row[2],
        "scheduled_at": row[3],
        "priority": row[4],
        "source": row[5],
        "status": row[6],
        "instance": HOST_ID,
    }


@app.post("/events", status_code=201)
def create_event(req: CreateEventRequest):
    trace = [f"{SERVICE_NAME}:{HOST_ID}"]

    user_resp = call_service("user-service", "GET", f"/users/{req.user_id}")
    if user_resp.status_code != 200:
        raise HTTPException(status_code=404, detail="Користувача не знайдено")
    user_info = user_resp.json()
    trace.append(user_info["instance"])

    nlp_resp = call_service(
        "nlp-service",
        "POST",
        "/nlp/parse",
        {"text": req.text, "timezone": user_info["user"]["timezone"]},
    )
    parsed = nlp_resp.json()
    trace.append(parsed["instance"])

    priority = parsed.get("priority", "normal")
    with sqlite3.connect(DB_PATH) as conn:
        conflict = conn.execute(
            "SELECT id, title FROM events WHERE user_id = ? AND scheduled_at = ? AND status != 'СКАСОВАНО'",
            (req.user_id, parsed["scheduled_at"]),
        ).fetchone()
        if conflict:
            raise HTTPException(
                status_code=409,
                detail=f"Конфлікт розкладу: на {parsed['scheduled_at']} вже є подія '{conflict[1]}' (id={conflict[0]})",
            )

        cur = conn.execute(
            "INSERT INTO events (user_id, title, scheduled_at, priority, source, status) VALUES (?, ?, ?, ?, ?, ?)",
            (req.user_id, parsed["title"], parsed["scheduled_at"], priority, req.source, "АКТИВНА"),
        )
        event_id = cur.lastrowid

    publish_event({
        "action": "СТВОРЕНО",
        "event_id": event_id,
        "title": parsed["title"],
        "scheduled_at": parsed["scheduled_at"],
        "priority": priority,
        "user": user_info["user"],
    })

    return {
        "event_id": event_id,
        "title": parsed["title"],
        "scheduled_at": parsed["scheduled_at"],
        "priority": priority,
        "status": "АКТИВНА",
        "trace": " -> ".join(trace),
    }


@app.put("/events/{event_id}")
def update_event(event_id: int, req: UpdateEventRequest):
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT user_id, priority FROM events WHERE id = ?", (event_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Подію не знайдено")
        user_id, priority = row[0], row[1]

        conn.execute(
            "UPDATE events SET title = ?, scheduled_at = ?, status = ? WHERE id = ?",
            (req.title, req.scheduled_at, req.status, event_id),
        )

    user_resp = call_service("user-service", "GET", f"/users/{user_id}")
    if user_resp.status_code != 200:
        raise HTTPException(status_code=404, detail="Користувача не знайдено")
    user_info = user_resp.json()

    publish_event({
        "action": "ЗМІНЕНО",
        "event_id": event_id,
        "title": req.title,
        "scheduled_at": req.scheduled_at,
        "priority": priority,
        "user": user_info["user"],
    })

    return {
        "event_id": event_id,
        "title": req.title,
        "scheduled_at": req.scheduled_at,
        "status": req.status,
        "instance": HOST_ID,
    }


@app.delete("/events/{event_id}")
def delete_event(event_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("DELETE FROM events WHERE id = ?", (event_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Подію не знайдено")
    return {"deleted_id": event_id, "instance": HOST_ID}
