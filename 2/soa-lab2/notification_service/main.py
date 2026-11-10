import os
import json
import time
import socket
import sqlite3
import threading
from contextlib import asynccontextmanager
from datetime import datetime
import httpx
import pika
from fastapi import FastAPI

SERVICE_NAME = "notification-service"
HOST_ID = socket.gethostname()
CONSUL_ID = f"{SERVICE_NAME}-{HOST_ID}"
CONSUL_URL = f"http://{os.getenv('CONSUL_HOST', 'consul')}:8500"
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = "calendar_events_queue"
DB_PATH = os.getenv("DB_PATH", "notifications.db")


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER,
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


def consume_loop():
    while True:
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            ch = conn.channel()
            ch.queue_declare(queue=QUEUE_NAME, durable=True)

            def on_message(channel, method, properties, body):
                data = json.loads(body.decode("utf-8"))
                user = data["user"]
                priority = data.get("priority", "normal")
                text = f"[{data['action']}] Подія '{data['title']}' на {data['scheduled_at']}"
                created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                with sqlite3.connect(DB_PATH) as db:
                    db.execute(
                        """INSERT INTO notifications
                           (event_id, action, channel, recipient, priority, text, processed_by, created_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (data["event_id"], data["action"], user["channel"], user["contact"], priority, text, HOST_ID, created_at),
                    )

                print(f"[Notification -> {user['channel']} ({user['contact']})] {text}", flush=True)
                channel.basic_ack(delivery_tag=method.delivery_tag)

            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)
            print(f"[RabbitMQ] Слухаємо чергу {QUEUE_NAME}...", flush=True)
            ch.start_consuming()
        except Exception as e:
            print(f"[RabbitMQ] Очікування підключення ({e})", flush=True)
            time.sleep(4)


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    register_in_consul()
    threading.Thread(target=consume_loop, daemon=True).start()
    yield
    deregister_from_consul()


app = FastAPI(title="Notification Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME, "instance": HOST_ID}


@app.get("/notifications")
def get_notifications():
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """SELECT id, event_id, action, channel, recipient, priority, text, processed_by, created_at
               FROM notifications ORDER BY id DESC"""
        ).fetchall()
    return {
        "instance": HOST_ID,
        "total": len(rows),
        "notifications": [
            {
                "id": r[0],
                "event_id": r[1],
                "action": r[2],
                "channel": r[3],
                "recipient": r[4],
                "priority": r[5],
                "text": r[6],
                "processed_by": r[7],
                "created_at": r[8],
            }
            for r in rows
        ],
    }
