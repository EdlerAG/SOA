import os
import time
import socket
import sqlite3
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

SERVICE_NAME = "user-service"
INSTANCE_ID = socket.gethostname()
SERVICE_ID = f"{SERVICE_NAME}-{INSTANCE_ID}"
CONSUL_URL = f"http://{os.getenv('CONSUL_HOST', 'consul')}:8500"
DB_PATH = os.getenv("DB_PATH", "users.db")

app = FastAPI(title="User Service")


class UserCreate(BaseModel):
    name: str
    timezone: str = "Europe/Kyiv"
    channel: str = "telegram"
    contact: str


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
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


@app.get("/users")
def get_all_users():
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("SELECT id, name, timezone, channel, contact FROM users ORDER BY id").fetchall()
    return {
        "instance": INSTANCE_ID,
        "users": [
            {"id": r[0], "name": r[1], "timezone": r[2], "channel": r[3], "contact": r[4]}
            for r in rows
        ]
    }


@app.get("/users/{user_id}")
def get_user(user_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT id, name, timezone, channel, contact FROM users WHERE id = ?",
            (user_id,)
        ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Користувача не знайдено")

    print(f"[UserService:{INSTANCE_ID}] Профіль id={user_id} ({row[1]})", flush=True)
    return {
        "user": {
            "id": row[0],
            "name": row[1],
            "timezone": row[2],
            "channel": row[3],
            "contact": row[4]
        },
        "instance": f"{SERVICE_NAME}:{INSTANCE_ID}"
    }


@app.post("/users")
def create_user(data: UserCreate):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            "INSERT INTO users (name, timezone, channel, contact) VALUES (?, ?, ?, ?)",
            (data.name, data.timezone, data.channel, data.contact)
        )
        user_id = cur.lastrowid
    return {"id": user_id, "name": data.name, "channel": data.channel, "instance": INSTANCE_ID}
