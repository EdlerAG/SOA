import os
import re
import time
import socket
import sqlite3
from datetime import datetime, timedelta
import httpx
from fastapi import FastAPI
from pydantic import BaseModel

SERVICE_NAME = "nlp-service"
INSTANCE_ID = socket.gethostname()
SERVICE_ID = f"{SERVICE_NAME}-{INSTANCE_ID}"
CONSUL_URL = f"http://{os.getenv('CONSUL_HOST', 'consul')}:8500"
DB_PATH = os.getenv("DB_PATH", "nlp_history.db")

app = FastAPI(title="NLP Service")


class ParseRequest(BaseModel):
    text: str
    timezone: str = "Europe/Kyiv"


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS parse_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_text TEXT,
                parsed_title TEXT,
                scheduled_at TEXT,
                priority TEXT,
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


@app.post("/nlp/parse")
def parse_text(req: ParseRequest):
    text_lower = req.text.lower()

    if "післязавтра" in text_lower:
        days_offset = 2
    elif "сьогодні" in text_lower:
        days_offset = 0
    else:
        days_offset = 1

    # Підтримуємо формат як 14:30, так і 14.30
    time_match = re.search(r"(\d{1,2}[:.]\d{2})", req.text)
    event_time = time_match.group(1).replace(".", ":") if time_match else "12:00"
    if len(event_time) == 4:
        event_time = "0" + event_time

    priority = "high" if any(w in text_lower for w in ("терміново", "важливо", "дедлайн")) else "normal"

    target_date = (datetime.now() + timedelta(days=days_offset)).strftime("%Y-%m-%d")
    scheduled_at = f"{target_date} {event_time}"

    title = re.sub(
        r"(післязавтра|завтра|сьогодні|(?:о|на)\s*\d{1,2}[:.]\d{2}|нагадай\s*мені\s*про|нагадай\s*про|нагадай|терміново|важливо)",
        "",
        req.text,
        flags=re.IGNORECASE
    ).strip(" ,.-!")

    if not title:
        title = "Нова подія"

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO parse_logs (raw_text, parsed_title, scheduled_at, priority, created_at) VALUES (?, ?, ?, ?, ?)",
            (req.text, title, scheduled_at, priority, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )

    print(f"[NLP:{INSTANCE_ID}] '{req.text}' -> '{title}' ({scheduled_at}, priority={priority})", flush=True)
    return {
        "title": title,
        "scheduled_at": scheduled_at,
        "priority": priority,
        "timezone": req.timezone,
        "instance": f"{SERVICE_NAME}:{INSTANCE_ID}"
    }


@app.get("/nlp/history")
def get_history():
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT id, raw_text, parsed_title, scheduled_at, priority, created_at FROM parse_logs ORDER BY id DESC"
        ).fetchall()
    return {
        "instance": INSTANCE_ID,
        "logs": [
            {
                "id": r[0],
                "raw_text": r[1],
                "title": r[2],
                "scheduled_at": r[3],
                "priority": r[4],
                "created_at": r[5],
            }
            for r in rows
        ]
    }
