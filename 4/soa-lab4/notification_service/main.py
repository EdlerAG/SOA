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
DLQ_NAME = "calendar_events_dlq"
REPLY_QUEUE_NAME = "calendar_saga_reply_queue"
DB_PATH = os.getenv("DB_PATH", "notifications.db")


def init_db():
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER,
                saga_id TEXT,
                action TEXT,
                channel TEXT,
                recipient TEXT,
                priority TEXT,
                text TEXT,
                processed_by TEXT,
                created_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS inbox_messages (
                message_id TEXT PRIMARY KEY,
                saga_id TEXT,
                event_id INTEGER,
                status TEXT,
                error_detail TEXT,
                processed_at TEXT
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
            ch.queue_declare(queue=DLQ_NAME, durable=True)
            ch.queue_declare(
                queue=QUEUE_NAME,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": DLQ_NAME,
                },
            )
            ch.queue_declare(queue=REPLY_QUEUE_NAME, durable=True)

            def on_message(channel, method, properties, body):
                data = json.loads(body.decode("utf-8"))
                msg_id = data.get("message_id") or properties.message_id or f"msg-{data['event_id']}"
                saga_id = data.get("saga_id", "unknown")
                event_id = data["event_id"]
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                with sqlite3.connect(DB_PATH, timeout=15.0) as db:
                    existing = db.execute(
                        "SELECT status FROM inbox_messages WHERE message_id = ?",
                        (msg_id,),
                    ).fetchone()
                    if existing:
                        print(f"[Inbox] Дублікат повідомлення {msg_id} пропущено", flush=True)
                        channel.basic_ack(delivery_tag=method.delivery_tag)
                        return

                try:
                    if data.get("simulate_error") or "помилка" in data.get("title", "").lower():
                        raise RuntimeError("Помилка запису в сервісі-консьюмері (симуляція збою для SAGA)")

                    user = data["user"]
                    priority = data.get("priority", "normal")
                    text = f"[{data['action']}] Подія '{data['title']}' на {data['scheduled_at']}"

                    with sqlite3.connect(DB_PATH, timeout=15.0) as db:
                        db.execute(
                            """INSERT INTO inbox_messages (message_id, saga_id, event_id, status, error_detail, processed_at)
                               VALUES (?, ?, ?, 'PROCESSED', NULL, ?)""",
                            (msg_id, saga_id, event_id, now_str),
                        )
                        db.execute(
                            """INSERT INTO notifications
                               (event_id, saga_id, action, channel, recipient, priority, text, processed_by, created_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (event_id, saga_id, data["action"], user["channel"], user["contact"], priority, text, HOST_ID, now_str),
                        )

                    reply_payload = json.dumps({"saga_id": saga_id, "event_id": event_id, "status": "OK"})
                    channel.basic_publish(
                        exchange="",
                        routing_key=REPLY_QUEUE_NAME,
                        body=reply_payload,
                        properties=pika.BasicProperties(delivery_mode=2),
                    )
                    print(f"[Notification -> {user['channel']}] {text} (SAGA OK)", flush=True)
                    channel.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as err:
                    with sqlite3.connect(DB_PATH, timeout=15.0) as db:
                        db.execute(
                            """INSERT OR REPLACE INTO inbox_messages (message_id, saga_id, event_id, status, error_detail, processed_at)
                               VALUES (?, ?, ?, 'FAILED_TO_DLQ', ?, ?)""",
                            (msg_id, saga_id, event_id, str(err), now_str),
                        )
                    print(f"[Consumer ERROR -> DLQ] saga_id={saga_id} event_id={event_id}: {err}", flush=True)
                    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)
            print(f"[RabbitMQ] Слухаємо {QUEUE_NAME} (DLQ={DLQ_NAME})...", flush=True)
            ch.start_consuming()
        except Exception as e:
            print(f"[RabbitMQ] Перепідключення ({e})", flush=True)
            time.sleep(4)


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    register_in_consul()
    threading.Thread(target=consume_loop, daemon=True).start()
    yield
    deregister_from_consul()


app = FastAPI(title="Notification Service (Transactional Inbox & SAGA Consumer)", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME, "instance": HOST_ID}


@app.get("/notifications")
def get_notifications():
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        rows = conn.execute(
            """SELECT id, event_id, saga_id, action, channel, recipient, priority, text, processed_by, created_at
               FROM notifications ORDER BY id DESC"""
        ).fetchall()
    return {
        "instance": HOST_ID,
        "total": len(rows),
        "notifications": [
            {
                "id": r[0],
                "event_id": r[1],
                "saga_id": r[2],
                "action": r[3],
                "channel": r[4],
                "recipient": r[5],
                "priority": r[6],
                "text": r[7],
                "processed_by": r[8],
                "created_at": r[9],
            }
            for r in rows
        ],
    }


@app.get("/inbox")
def get_inbox():
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        rows = conn.execute(
            "SELECT message_id, saga_id, event_id, status, error_detail, processed_at FROM inbox_messages ORDER BY processed_at DESC"
        ).fetchall()
    return {
        "instance": HOST_ID,
        "inbox": [
            {
                "message_id": r[0],
                "saga_id": r[1],
                "event_id": r[2],
                "status": r[3],
                "error_detail": r[4],
                "processed_at": r[5],
            }
            for r in rows
        ],
    }
