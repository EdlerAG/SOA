import os
import json
import time
import socket
import sqlite3
import threading
import httpx
import pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

SERVICE_NAME = "calendar-service"
INSTANCE_ID = socket.gethostname()
SERVICE_ID = f"{SERVICE_NAME}-{INSTANCE_ID}"
CONSUL_URL = f"http://{os.getenv('CONSUL_HOST', 'consul')}:8500"
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = "calendar_events_queue"
DB_PATH = os.getenv("DB_PATH", "calendar.db")

app = FastAPI(title="Calendar Service")

# Лічильники для Round-Robin балансування по сервісах
_rr_lock = threading.Lock()
_rr_index: dict[str, int] = {}


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


def register_service():
    payload = {
        "ID": SERVICE_ID,
        "Name": SERVICE_NAME,
        "Address": INSTANCE_ID,
        "Port": 8000,
        "Check": {
            "HTTP": f"http://{INSTANCE_ID}:8000/health",
            "Interval": "10s",
            "Timeout": "5s"
        }
    }
    for _ in range(10):
        try:
            r = httpx.put(f"{CONSUL_URL}/v1/agent/service/register", json=payload, timeout=3.0)
            if r.status_code == 200:
                print(f"[Consul] Зареєстровано {SERVICE_ID}", flush=True)
                return
        except Exception:
            time.sleep(2)


def get_service_url(service_name: str) -> str:
    """Пошук живих інстансів через Consul + Round-Robin балансування"""
    try:
        r = httpx.get(f"{CONSUL_URL}/v1/health/service/{service_name}?passing=true", timeout=5.0)
        r.raise_for_status()
        nodes = sorted(r.json(), key=lambda x: x["Service"]["ID"])
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Помилка звернення до Consul: {e}")

    if not nodes:
        raise HTTPException(status_code=503, detail=f"Сервіс {service_name} недоступний у Consul")

    with _rr_lock:
        idx = _rr_index.get(service_name, 0) % len(nodes)
        _rr_index[service_name] = idx + 1

    service = nodes[idx]["Service"]
    print(
        f"[Discovery] {service_name}: знайдено {len(nodes)} реплік, обрано {service['ID']} (RR #{idx + 1}/{len(nodes)})",
        flush=True,
    )
    return f"http://{service['Address']}:{service['Port']}"


def send_to_queue(message: dict):
    """Відправка події в RabbitMQ"""
    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        ch = conn.channel()
        ch.queue_declare(queue=QUEUE_NAME, durable=True)
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(message, ensure_ascii=False),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"RabbitMQ тимчасово недоступний: {e}")


@app.on_event("startup")
def startup():
    init_db()
    register_service()


@app.on_event("shutdown")
def shutdown():
    try:
        httpx.put(f"{CONSUL_URL}/v1/agent/service/deregister/{SERVICE_ID}", timeout=2.0)
    except Exception:
        pass


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME, "instance": INSTANCE_ID}


@app.get("/events")
def list_events():
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT id, user_id, title, scheduled_at, priority, source, status FROM events ORDER BY id DESC"
        ).fetchall()
    return {
        "instance": INSTANCE_ID,
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
        ]
    }


@app.get("/events/{event_id}")
def get_event(event_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT id, user_id, title, scheduled_at, priority, source, status FROM events WHERE id = ?",
            (event_id,)
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
        "instance": INSTANCE_ID,
    }


@app.post("/events")
def create_event(req: CreateEventRequest):
    trace = [f"{SERVICE_NAME}:{INSTANCE_ID}"]

    user_url = get_service_url("user-service")
    user_resp = httpx.get(f"{user_url}/users/{req.user_id}", timeout=5.0)
    if user_resp.status_code != 200:
        raise HTTPException(status_code=404, detail="Користувача не знайдено")
    user_info = user_resp.json()
    trace.append(user_info["instance"])

    nlp_url = get_service_url("nlp-service")
    nlp_resp = httpx.post(
        f"{nlp_url}/nlp/parse",
        json={"text": req.text, "timezone": user_info["user"]["timezone"]},
        timeout=5.0
    )
    if nlp_resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Помилка обробки тексту в NLP-сервісі")
    parsed = nlp_resp.json()
    trace.append(parsed["instance"])

    priority = parsed.get("priority", "normal")
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            "INSERT INTO events (user_id, title, scheduled_at, priority, source, status) VALUES (?, ?, ?, ?, ?, ?)",
            (req.user_id, parsed["title"], parsed["scheduled_at"], priority, req.source, "АКТИВНА")
        )
        event_id = cur.lastrowid

    send_to_queue({
        "action": "СТВОРЕНО",
        "event_id": event_id,
        "title": parsed["title"],
        "scheduled_at": parsed["scheduled_at"],
        "priority": priority,
        "user": user_info["user"]
    })

    return {
        "event_id": event_id,
        "title": parsed["title"],
        "scheduled_at": parsed["scheduled_at"],
        "priority": priority,
        "status": "АКТИВНА",
        "trace": " -> ".join(trace)
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
            (req.title, req.scheduled_at, req.status, event_id)
        )

    user_url = get_service_url("user-service")
    user_resp = httpx.get(f"{user_url}/users/{user_id}", timeout=5.0)
    if user_resp.status_code != 200:
        raise HTTPException(status_code=404, detail="Користувача не знайдено")
    user_info = user_resp.json()

    send_to_queue({
        "action": "ЗМІНЕНО",
        "event_id": event_id,
        "title": req.title,
        "scheduled_at": req.scheduled_at,
        "priority": priority,
        "user": user_info["user"]
    })

    return {
        "event_id": event_id,
        "title": req.title,
        "scheduled_at": req.scheduled_at,
        "status": req.status,
        "instance": INSTANCE_ID
    }


@app.delete("/events/{event_id}")
def delete_event(event_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("DELETE FROM events WHERE id = ?", (event_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Подію не знайдено")
    return {"deleted_id": event_id, "instance": INSTANCE_ID}
