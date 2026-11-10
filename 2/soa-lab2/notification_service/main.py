import os
import json
import time
import socket
import sqlite3
import threading
from datetime import datetime
import httpx
import pika
from fastapi import FastAPI

SERVICE_NAME = "notification-service"
INSTANCE_ID = socket.gethostname()
SERVICE_ID = f"{SERVICE_NAME}-{INSTANCE_ID}"
CONSUL_URL = f"http://{os.getenv('CONSUL_HOST', 'consul')}:8500"
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = "calendar_events_queue"
DB_PATH = os.getenv("DB_PATH", "notifications.db")

app = FastAPI(title="Notification Service")


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


def consume_loop():
    while True:
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            ch = conn.channel()
            ch.queue_declare(queue=QUEUE_NAME, durable=True)

            def on_message(ch, method, properties, body):
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
                        (data["event_id"], data["action"], user["channel"], user["contact"], priority, text, INSTANCE_ID, created_at)
                    )

                print(f"[Notification -> {user['channel']} ({user['contact']})] {text}", flush=True)
                ch.basic_ack(delivery_tag=method.delivery_tag)

            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)
            print(f"[RabbitMQ] Очікування повідомлень у {QUEUE_NAME}...", flush=True)
            ch.start_consuming()
        except Exception as e:
            print(f"[RabbitMQ] Повторне підключення через 4с ({e})", flush=True)
            time.sleep(4)


@app.on_event("startup")
def startup():
    init_db()
    register_service()
    threading.Thread(target=consume_loop, daemon=True).start()


@app.on_event("shutdown")
def shutdown():
    try:
        httpx.put(f"{CONSUL_URL}/v1/agent/service/deregister/{SERVICE_ID}", timeout=2.0)
    except Exception:
        pass


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME, "instance": INSTANCE_ID}


@app.get("/notifications")
def get_notifications():
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """SELECT id, event_id, action, channel, recipient, priority, text, processed_by, created_at
               FROM notifications ORDER BY id DESC"""
        ).fetchall()
    return {
        "instance": INSTANCE_ID,
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
        ]
    }
