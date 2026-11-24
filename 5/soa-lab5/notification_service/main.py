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
import jwt
from jwt import PyJWKClient
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

SERVICE_NAME = "notification-service"
HOST_ID = socket.gethostname()
CONSUL_ID = f"{SERVICE_NAME}-{HOST_ID}"
CONSUL_URL = f"http://{os.getenv('CONSUL_HOST', 'consul')}:8500"
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = "calendar_events_queue"
JWKS_URL = os.getenv("KEYCLOAK_JWKS_URL", "http://keycloak:8080/realms/soa-calendar/protocol/openid-connect/certs")
ALLOWED_ISSUERS = {
    os.getenv("KEYCLOAK_ISSUER", "http://localhost:8180/realms/soa-calendar"),
    "http://keycloak:8080/realms/soa-calendar",
}
DB_PATH = os.getenv("DB_PATH", "notifications.db")

bearer_scheme = HTTPBearer(auto_error=False)
jwks_client = PyJWKClient(JWKS_URL)


def require_scope(required_scope: str):
    def dependency(creds: HTTPAuthorizationCredentials | None = Depends(bearer_scheme)) -> dict:
        if not creds or not creds.credentials:
            raise HTTPException(status_code=401, detail="Відсутній Bearer JWT токен")
        try:
            signing_key = jwks_client.get_signing_key_from_jwt(creds.credentials)
            payload = jwt.decode(
                creds.credentials,
                signing_key.key,
                algorithms=["RS256"],
                options={"verify_aud": False},
            )
            if payload.get("iss") not in ALLOWED_ISSUERS:
                raise HTTPException(status_code=401, detail="Недійсний issuer токена")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=401, detail=f"Недійсний JWT токен: {e}")

        scopes = set(payload.get("scope", "").split())
        if required_scope not in scopes:
            raise HTTPException(
                status_code=403,
                detail=f"Недостатньо прав: потрібен OAuth2 scope '{required_scope}'",
            )
        return payload
    return dependency


def init_db():
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
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
                user = data["user"]
                priority = data.get("priority", "normal")
                text = f"[{data['action']}] Подія '{data['title']}' на {data['scheduled_at']}"
                created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                with sqlite3.connect(DB_PATH, timeout=15.0) as db:
                    db.execute(
                        """INSERT INTO notifications (event_id, action, channel, recipient, priority, text, processed_by, created_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (data["event_id"], data["action"], user["channel"], user["contact"], priority, text, HOST_ID, created_at),
                    )
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


app = FastAPI(title="Notification Service (JWT + Scopes + CORS)", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://cad.kpi.ua", "http://cad.kpi.ua", "http://localhost:8080"],
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME, "instance": HOST_ID}


@app.get("/notifications")
def get_notifications(token: dict = Depends(require_scope("notifications:read"))):
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        rows = conn.execute(
            "SELECT id, event_id, action, channel, recipient, priority, text, processed_by, created_at FROM notifications ORDER BY id DESC"
        ).fetchall()
    return {
        "instance": HOST_ID,
        "requested_by": token.get("preferred_username"),
        "total": len(rows),
        "notifications": [
            {"id": r[0], "event_id": r[1], "action": r[2], "channel": r[3], "recipient": r[4], "priority": r[5], "text": r[6], "processed_by": r[7], "created_at": r[8]}
            for r in rows
        ],
    }
