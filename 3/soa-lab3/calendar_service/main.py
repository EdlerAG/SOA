import os
import json
import time
import uuid
import socket
import sqlite3
import threading
from contextlib import asynccontextmanager
from datetime import datetime
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
DLQ_NAME = "calendar_events_dlq"
REPLY_QUEUE_NAME = "calendar_saga_reply_queue"
DB_PATH = os.getenv("DB_PATH", "calendar.db")
SAGA_TIMEOUT_SEC = int(os.getenv("SAGA_TIMEOUT_SEC", "20"))

_rr_lock = threading.Lock()
_rr_index: dict[str, int] = {}

_cb_state: dict[str, dict] = {}
CB_FAILURE_THRESHOLD = 3
CB_OPEN_SECONDS = 15


class CreateEventRequest(BaseModel):
    user_id: int = 1
    text: str
    source: str = "web"
    simulate_error: bool = False


class UpdateEventRequest(BaseModel):
    title: str
    scheduled_at: str
    status: str = "ОНОВЛЕНО"
    simulate_error: bool = False


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
                status TEXT,
                previous_title TEXT,
                previous_scheduled_at TEXT,
                previous_status TEXT,
                updated_at REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS outbox_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT UNIQUE,
                saga_id TEXT,
                event_id INTEGER,
                action TEXT,
                payload TEXT,
                status TEXT,
                created_at TEXT,
                sent_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS saga_log (
                saga_id TEXT PRIMARY KEY,
                event_id INTEGER,
                action TEXT,
                state TEXT,
                reason TEXT,
                started_at REAL,
                updated_at TEXT
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


def declare_queues(ch):
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


def compensate_saga(saga_id: str, event_id: int, reason: str):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        saga_row = conn.execute(
            "SELECT action, state FROM saga_log WHERE saga_id = ?",
            (saga_id,),
        ).fetchone()
        if not saga_row or saga_row[1] in ("COMPENSATED", "COMPLETED"):
            return

        action = saga_row[0]
        if action == "СТВОРЕНО":
            conn.execute(
                "UPDATE events SET status = 'СКАСОВАНО (SAGA ROLLBACK)' WHERE id = ?",
                (event_id,),
            )
        elif action == "ЗМІНЕНО":
            prev = conn.execute(
                "SELECT previous_title, previous_scheduled_at, previous_status FROM events WHERE id = ?",
                (event_id,),
            ).fetchone()
            if prev and prev[0]:
                conn.execute(
                    "UPDATE events SET title = ?, scheduled_at = ?, status = ? WHERE id = ?",
                    (prev[0], prev[1], prev[2], event_id),
                )
            else:
                conn.execute(
                    "UPDATE events SET status = 'СКАСОВАНО (SAGA ROLLBACK)' WHERE id = ?",
                    (event_id,),
                )

        conn.execute(
            "UPDATE saga_log SET state = 'COMPENSATED', reason = ?, updated_at = ? WHERE saga_id = ?",
            (reason, now_str, saga_id),
        )
    print(f"[SAGA COMPENSATE] saga_id={saga_id} event_id={event_id} відкочено ({reason})", flush=True)


def complete_saga(saga_id: str, event_id: int):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        saga_row = conn.execute(
            "SELECT action, state FROM saga_log WHERE saga_id = ?",
            (saga_id,),
        ).fetchone()
        if not saga_row or saga_row[1] != "STARTED":
            return

        new_status = "АКТИВНА" if saga_row[0] == "СТВОРЕНО" else "ОНОВЛЕНО"
        conn.execute("UPDATE events SET status = ? WHERE id = ?", (new_status, event_id))
        conn.execute(
            "UPDATE saga_log SET state = 'COMPLETED', reason = 'Успішно підтверджено консьюмером', updated_at = ? WHERE saga_id = ?",
            (now_str, saga_id),
        )
    print(f"[SAGA COMPLETE] saga_id={saga_id} event_id={event_id} підтверджено", flush=True)


def outbox_relay_worker():
    while True:
        try:
            with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
                pending_msgs = conn.execute(
                    "SELECT id, message_id, payload FROM outbox_messages WHERE status = 'NEW' ORDER BY id ASC LIMIT 20"
                ).fetchall()

            if pending_msgs:
                params = pika.ConnectionParameters(host=RABBITMQ_HOST, socket_timeout=3)
                rmq_conn = pika.BlockingConnection(params)
                ch = rmq_conn.channel()
                declare_queues(ch)

                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
                    for row_id, msg_id, payload_str in pending_msgs:
                        ch.basic_publish(
                            exchange="",
                            routing_key=QUEUE_NAME,
                            body=payload_str,
                            properties=pika.BasicProperties(
                                delivery_mode=2,
                                message_id=msg_id,
                            ),
                        )
                        conn.execute(
                            "UPDATE outbox_messages SET status = 'SENT', sent_at = ? WHERE id = ?",
                            (now_str, row_id),
                        )
                        print(f"[OutboxRelay] Повідомлення {msg_id} відправлено в {QUEUE_NAME}", flush=True)
                rmq_conn.close()

            cutoff = time.time() - SAGA_TIMEOUT_SEC
            with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
                timed_out = conn.execute(
                    "SELECT saga_id, event_id FROM saga_log WHERE state = 'STARTED' AND started_at < ?",
                    (cutoff,),
                ).fetchall()
            for saga_id, event_id in timed_out:
                compensate_saga(saga_id, event_id, f"Таймаут очікування консьюмера ({SAGA_TIMEOUT_SEC}с)")

        except Exception as e:
            print(f"[OutboxRelay] Очікування брокера ({e})", flush=True)
        time.sleep(1.5)


def saga_dlq_and_reply_listener():
    while True:
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            ch = conn.channel()
            declare_queues(ch)

            def on_dlq_message(channel, method, properties, body):
                data = json.loads(body.decode("utf-8"))
                compensate_saga(
                    data["saga_id"],
                    data["event_id"],
                    f"Повідомлення потрапило в DLQ ({DLQ_NAME}): помилка в notification-service",
                )
                channel.basic_ack(delivery_tag=method.delivery_tag)

            def on_reply_message(channel, method, properties, body):
                data = json.loads(body.decode("utf-8"))
                if data.get("status") == "OK":
                    complete_saga(data["saga_id"], data["event_id"])
                else:
                    compensate_saga(data["saga_id"], data["event_id"], data.get("error", "Помилка консьюмера"))
                channel.basic_ack(delivery_tag=method.delivery_tag)

            ch.basic_consume(queue=DLQ_NAME, on_message_callback=on_dlq_message)
            ch.basic_consume(queue=REPLY_QUEUE_NAME, on_message_callback=on_reply_message)
            print(f"[SAGA Listener] Слухаємо {DLQ_NAME} та {REPLY_QUEUE_NAME}...", flush=True)
            ch.start_consuming()
        except Exception:
            time.sleep(3)


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    register_in_consul()
    threading.Thread(target=outbox_relay_worker, daemon=True).start()
    threading.Thread(target=saga_dlq_and_reply_listener, daemon=True).start()
    yield
    deregister_from_consul()


app = FastAPI(title="Calendar Service (Transactional Outbox & SAGA)", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME, "instance": HOST_ID}


@app.get("/events")
def list_events():
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
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
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
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


@app.get("/outbox")
def list_outbox():
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        rows = conn.execute(
            "SELECT id, message_id, saga_id, event_id, action, status, created_at, sent_at FROM outbox_messages ORDER BY id DESC"
        ).fetchall()
    return {
        "instance": HOST_ID,
        "outbox": [
            {
                "id": r[0],
                "message_id": r[1],
                "saga_id": r[2],
                "event_id": r[3],
                "action": r[4],
                "status": r[5],
                "created_at": r[6],
                "sent_at": r[7],
            }
            for r in rows
        ],
    }


@app.get("/saga")
def list_sagas():
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        rows = conn.execute(
            "SELECT saga_id, event_id, action, state, reason, updated_at FROM saga_log ORDER BY started_at DESC"
        ).fetchall()
    return {
        "instance": HOST_ID,
        "sagas": [
            {
                "saga_id": r[0],
                "event_id": r[1],
                "action": r[2],
                "state": r[3],
                "reason": r[4],
                "updated_at": r[5],
            }
            for r in rows
        ],
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
    message_id = str(uuid.uuid4())
    saga_id = str(uuid.uuid4())
    now_ts = time.time()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    should_fail = req.simulate_error or any(w in req.text.lower() for w in ("помилка", "fail", "error"))

    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        conflict = conn.execute(
            "SELECT id, title FROM events WHERE user_id = ? AND scheduled_at = ? AND status NOT LIKE 'СКАСОВАНО%'",
            (req.user_id, parsed["scheduled_at"]),
        ).fetchone()
        if conflict:
            raise HTTPException(
                status_code=409,
                detail=f"Конфлікт розкладу: на {parsed['scheduled_at']} вже є подія '{conflict[1]}' (id={conflict[0]})",
            )

        cur = conn.execute(
            """INSERT INTO events (user_id, title, scheduled_at, priority, source, status, updated_at)
               VALUES (?, ?, ?, ?, ?, 'ОЧІКУЄ ПІДТВЕРДЖЕННЯ (SAGA)', ?)""",
            (req.user_id, parsed["title"], parsed["scheduled_at"], priority, req.source, now_ts),
        )
        event_id = cur.lastrowid

        outbox_payload = json.dumps(
            {
                "message_id": message_id,
                "saga_id": saga_id,
                "action": "СТВОРЕНО",
                "event_id": event_id,
                "title": parsed["title"],
                "scheduled_at": parsed["scheduled_at"],
                "priority": priority,
                "user": user_info["user"],
                "simulate_error": should_fail,
            },
            ensure_ascii=False,
        )

        conn.execute(
            """INSERT INTO outbox_messages (message_id, saga_id, event_id, action, payload, status, created_at)
               VALUES (?, ?, ?, 'СТВОРЕНО', ?, 'NEW', ?)""",
            (message_id, saga_id, event_id, outbox_payload, now_str),
        )
        conn.execute(
            """INSERT INTO saga_log (saga_id, event_id, action, state, reason, started_at, updated_at)
               VALUES (?, ?, 'СТВОРЕНО', 'STARTED', 'Записано в Transactional Outbox', ?, ?)""",
            (saga_id, event_id, now_ts, now_str),
        )

    return {
        "event_id": event_id,
        "saga_id": saga_id,
        "outbox_message_id": message_id,
        "title": parsed["title"],
        "scheduled_at": parsed["scheduled_at"],
        "priority": priority,
        "status": "ОЧІКУЄ ПІДТВЕРДЖЕННЯ (SAGA)",
        "simulate_error": should_fail,
        "trace": " -> ".join(trace),
    }


@app.put("/events/{event_id}")
def update_event(event_id: int, req: UpdateEventRequest):
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        row = conn.execute(
            "SELECT user_id, title, scheduled_at, priority, status FROM events WHERE id = ?",
            (event_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Подію не знайдено")
        user_id, prev_title, prev_sched, priority, prev_status = row

    user_resp = call_service("user-service", "GET", f"/users/{user_id}")
    if user_resp.status_code != 200:
        raise HTTPException(status_code=404, detail="Користувача не знайдено")
    user_info = user_resp.json()

    message_id = str(uuid.uuid4())
    saga_id = str(uuid.uuid4())
    now_ts = time.time()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    should_fail = req.simulate_error or any(w in req.title.lower() for w in ("помилка", "fail", "error"))

    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        conn.execute(
            """UPDATE events
               SET title = ?, scheduled_at = ?, status = 'ОЧІКУЄ ОНОВЛЕННЯ (SAGA)',
                   previous_title = ?, previous_scheduled_at = ?, previous_status = ?, updated_at = ?
               WHERE id = ?""",
            (req.title, req.scheduled_at, prev_title, prev_sched, prev_status, now_ts, event_id),
        )

        outbox_payload = json.dumps(
            {
                "message_id": message_id,
                "saga_id": saga_id,
                "action": "ЗМІНЕНО",
                "event_id": event_id,
                "title": req.title,
                "scheduled_at": req.scheduled_at,
                "priority": priority,
                "user": user_info["user"],
                "simulate_error": should_fail,
            },
            ensure_ascii=False,
        )

        conn.execute(
            """INSERT INTO outbox_messages (message_id, saga_id, event_id, action, payload, status, created_at)
               VALUES (?, ?, ?, 'ЗМІНЕНО', ?, 'NEW', ?)""",
            (message_id, saga_id, event_id, outbox_payload, now_str),
        )
        conn.execute(
            """INSERT INTO saga_log (saga_id, event_id, action, state, reason, started_at, updated_at)
               VALUES (?, ?, 'ЗМІНЕНО', 'STARTED', 'Оновлення записано в Transactional Outbox', ?, ?)""",
            (saga_id, event_id, now_ts, now_str),
        )

    return {
        "event_id": event_id,
        "saga_id": saga_id,
        "outbox_message_id": message_id,
        "title": req.title,
        "scheduled_at": req.scheduled_at,
        "status": "ОЧІКУЄ ОНОВЛЕННЯ (SAGA)",
        "simulate_error": should_fail,
        "instance": HOST_ID,
    }


@app.delete("/events/{event_id}")
def delete_event(event_id: int):
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        cur = conn.execute("DELETE FROM events WHERE id = ?", (event_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Подію не знайдено")
    return {"deleted_id": event_id, "instance": HOST_ID}
