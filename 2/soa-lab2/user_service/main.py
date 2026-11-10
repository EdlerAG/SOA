import os
import time
import socket
import sqlite3
from contextlib import asynccontextmanager
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

SERVICE_NAME = "user-service"
HOST_ID = socket.gethostname()
CONSUL_ID = f"{SERVICE_NAME}-{HOST_ID}"
CONSUL_URL = f"http://{os.getenv('CONSUL_HOST', 'consul')}:8500"
DB_PATH = os.getenv("DB_PATH", "users.db")


class UserPayload(BaseModel):
    name: str
    timezone: str = "Europe/Kyiv"
    channel: str = "telegram"
    contact: str


def db_conn():
    return sqlite3.connect(DB_PATH)


def init_db():
    with db_conn() as conn:
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


app = FastAPI(title="User Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME, "instance": HOST_ID}


@app.get("/users")
def list_users():
    with db_conn() as conn:
        rows = conn.execute("SELECT id, name, timezone, channel, contact FROM users ORDER BY id").fetchall()
    return {
        "instance": HOST_ID,
        "users": [
            {"id": r[0], "name": r[1], "timezone": r[2], "channel": r[3], "contact": r[4]}
            for r in rows
        ],
    }


@app.get("/users/{user_id}")
def get_user(user_id: int):
    with db_conn() as conn:
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
        "instance": f"{SERVICE_NAME}:{HOST_ID}",
    }


@app.post("/users", status_code=201)
def create_user(data: UserPayload):
    with db_conn() as conn:
        cur = conn.execute(
            "INSERT INTO users (name, timezone, channel, contact) VALUES (?, ?, ?, ?)",
            (data.name, data.timezone, data.channel, data.contact),
        )
        user_id = cur.lastrowid
    return {"id": user_id, "name": data.name, "channel": data.channel, "instance": HOST_ID}


@app.put("/users/{user_id}")
def update_user(user_id: int, data: UserPayload):
    with db_conn() as conn:
        cur = conn.execute(
            "UPDATE users SET name = ?, timezone = ?, channel = ?, contact = ? WHERE id = ?",
            (data.name, data.timezone, data.channel, data.contact, user_id),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Користувача не знайдено")
    return {"id": user_id, "name": data.name, "timezone": data.timezone, "channel": data.channel, "instance": HOST_ID}
