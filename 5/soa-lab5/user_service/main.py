import os
import time
import socket
import sqlite3
from contextlib import asynccontextmanager
import httpx
import jwt
from jwt import PyJWKClient
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

SERVICE_NAME = "user-service"
HOST_ID = socket.gethostname()
CONSUL_ID = f"{SERVICE_NAME}-{HOST_ID}"
CONSUL_URL = f"http://{os.getenv('CONSUL_HOST', 'consul')}:8500"
JWKS_URL = os.getenv("KEYCLOAK_JWKS_URL", "http://keycloak:8080/realms/soa-calendar/protocol/openid-connect/certs")
ALLOWED_ISSUERS = {
    os.getenv("KEYCLOAK_ISSUER", "http://localhost:8180/realms/soa-calendar"),
    "http://keycloak:8080/realms/soa-calendar",
}
DB_PATH = os.getenv("DB_PATH", "users.db")

bearer_scheme = HTTPBearer(auto_error=False)
jwks_client = PyJWKClient(JWKS_URL)


class UserPayload(BaseModel):
    name: str
    timezone: str = "Europe/Kyiv"
    channel: str = "telegram"
    contact: str


def verify_jwt(creds: HTTPAuthorizationCredentials | None) -> dict:
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
        return payload
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Недійсний JWT токен: {e}")


def require_scope(required_scope: str):
    def dependency(creds: HTTPAuthorizationCredentials | None = Depends(bearer_scheme)) -> dict:
        payload = verify_jwt(creds)
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
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                timezone TEXT NOT NULL,
                channel TEXT NOT NULL,
                contact TEXT NOT NULL
            )
        """)
        conn.execute("""
            INSERT OR IGNORE INTO users (id, name, timezone, channel, contact)
            VALUES (1, 'Ярослав Обіход', 'Europe/Kyiv', 'telegram', '@yaroslav_kpi')
        """)


def consul_register():
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


def consul_deregister():
    try:
        httpx.put(f"{CONSUL_URL}/v1/agent/service/deregister/{CONSUL_ID}", timeout=2.0)
    except httpx.HTTPError:
        pass


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    consul_register()
    yield
    consul_deregister()


app = FastAPI(title="User Service (JWT + Scopes + CORS)", lifespan=lifespan)

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


@app.get("/users")
def list_users(token: dict = Depends(require_scope("users:read"))):
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        rows = conn.execute("SELECT id, name, timezone, channel, contact FROM users ORDER BY id").fetchall()
    return {
        "instance": HOST_ID,
        "requested_by": token.get("preferred_username"),
        "users": [
            {"id": r[0], "name": r[1], "timezone": r[2], "channel": r[3], "contact": r[4]}
            for r in rows
        ],
    }


@app.get("/users/{user_id}")
def get_user(user_id: int, token: dict = Depends(require_scope("users:read"))):
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        row = conn.execute(
            "SELECT id, name, timezone, channel, contact FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Користувача не знайдено")
    return {
        "user": {
            "id": row[0],
            "name": row[1],
            "timezone": row[2],
            "channel": row[3],
            "contact": row[4],
        },
        "requested_by": token.get("preferred_username"),
        "instance": f"{SERVICE_NAME}:{HOST_ID}",
    }


@app.post("/users", status_code=201)
def create_user(data: UserPayload, _: dict = Depends(require_scope("users:write"))):
    with sqlite3.connect(DB_PATH, timeout=15.0) as conn:
        cur = conn.execute(
            "INSERT INTO users (name, timezone, channel, contact) VALUES (?, ?, ?, ?)",
            (data.name, data.timezone, data.channel, data.contact),
        )
        user_id = cur.lastrowid
    return {"id": user_id, "name": data.name, "channel": data.channel, "instance": HOST_ID}
