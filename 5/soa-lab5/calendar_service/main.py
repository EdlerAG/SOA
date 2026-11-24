import os
import json
import time
import socket
import sqlite3
import threading
from contextlib import asynccontextmanager
import httpx
import pika
import jwt
from jwt import PyJWKClient
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

SERVICE_NAME = "calendar-service"
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
DB_PATH = os.getenv("DB_PATH", "calendar.db")

_rr_lock = threading.Lock()
_rr_index: dict[str, int] = {}

bearer_scheme = HTTPBearer(auto_error=False)
jwks_client = PyJWKClient(JWKS_URL)


class CreateEventRequest(BaseModel):
    user_id: int = 1
    text: str
    source: str = "web"


class UpdateEventRequest(BaseModel):
    title: str
    scheduled_at: str
    status: str = "ОНОВЛЕНО"


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
        return {"claims": payload, "raw_token": creds.credentials}
    return dependency


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


app = FastAPI(title="Calendar Service (JWT + Scopes + CORS)", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://cad.kpi.ua", "http://cad.kpi.ua", "http://localhost:8080"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME, "instance": HOST_ID}


@app.get("/events")
def list_events(auth: dict = Depends(require_scope("calendar:read"))):
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        rows = conn.execute(
            "SELECT id, user_id, title, scheduled_at, priority, source, status FROM events ORDER BY id DESC"
        ).fetchall()
    return {
        "instance": HOST_ID,
        "authenticated_user": auth["claims"].get("preferred_username"),
        "scopes": auth["claims"].get("scope"),
        "events": [
            {"id": r[0], "user_id": r[1], "title": r[2], "scheduled_at": r[3], "priority": r[4], "source": r[5], "status": r[6]}
            for r in rows
        ],
    }


@app.get("/events/{event_id}")
def get_event(event_id: int, _: dict = Depends(require_scope("calendar:read"))):
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        row = conn.execute(
            "SELECT id, user_id, title, scheduled_at, priority, source, status FROM events WHERE id = ?",
            (event_id,),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Подію не знайдено")
    return {"id": row[0], "user_id": row[1], "title": row[2], "scheduled_at": row[3], "priority": row[4], "source": row[5], "status": row[6]}


@app.post("/events", status_code=201)
def create_event(req: CreateEventRequest, auth: dict = Depends(require_scope("calendar:write"))):
    raw_token = auth["raw_token"]
    fwd_headers = {"Authorization": f"Bearer {raw_token}"}
    trace = [f"{SERVICE_NAME}:{HOST_ID}"]

    user_url = pick_service_url("user-service")
    user_resp = httpx.get(f"{user_url}/users/{req.user_id}", headers=fwd_headers, timeout=5.0)
    if user_resp.status_code in (401, 403):
        raise HTTPException(status_code=user_resp.status_code, detail=user_resp.json().get("detail"))
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
    if nlp_resp.status_code in (401, 403):
        raise HTTPException(status_code=nlp_resp.status_code, detail=nlp_resp.json().get("detail"))
    parsed = nlp_resp.json()
    trace.append(parsed["instance"])

    priority = parsed.get("priority", "normal")
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
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
        "authenticated_user": auth["claims"].get("preferred_username"),
        "trace": " -> ".join(trace),
    }


@app.put("/events/{event_id}")
def update_event(event_id: int, req: UpdateEventRequest, auth: dict = Depends(require_scope("calendar:write"))):
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        cur = conn.execute(
            "UPDATE events SET title = ?, scheduled_at = ?, status = ? WHERE id = ?",
            (req.title, req.scheduled_at, req.status, event_id),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Подію не знайдено")
    return {"event_id": event_id, "title": req.title, "status": req.status, "updated_by": auth["claims"].get("preferred_username")}


@app.delete("/events/{event_id}")
def delete_event(event_id: int, _: dict = Depends(require_scope("calendar:write"))):
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        cur = conn.execute("DELETE FROM events WHERE id = ?", (event_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Подію не знайдено")
    return {"deleted_id": event_id}
