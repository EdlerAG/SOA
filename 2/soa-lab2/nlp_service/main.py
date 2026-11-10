import os
import re
import json
import time
import socket
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import httpx
from fastapi import FastAPI
from pydantic import BaseModel

SERVICE_NAME = "nlp-service"
HOST_ID = socket.gethostname()
CONSUL_ID = f"{SERVICE_NAME}-{HOST_ID}"
CONSUL_URL = f"http://{os.getenv('CONSUL_HOST', 'consul')}:8500"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
DB_PATH = os.getenv("DB_PATH", "nlp_history.db")


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
                parser_mode TEXT,
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


def get_local_now(tz_name: str) -> datetime:
    try:
        return datetime.now(ZoneInfo(tz_name))
    except ZoneInfoNotFoundError:
        return datetime.now(ZoneInfo("Europe/Kyiv"))


def parse_with_gemini(text: str, now_local: datetime) -> dict | None:
    if not GEMINI_API_KEY:
        return None
    prompt = (
        f"Поточна дата і час користувача: {now_local.strftime('%Y-%m-%d %H:%M')}. "
        f"Витягни з тексту назву події, дату/час у форматі YYYY-MM-DD HH:MM і priority (high або normal). "
        f"Поверни тільки JSON з ключами title, scheduled_at, priority. Текст: {text}"
    )
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"
    try:
        r = httpx.post(
            url,
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"responseMimeType": "application/json"},
            },
            timeout=6.0,
        )
        r.raise_for_status()
        raw_json = r.json()["candidates"][0]["content"]["parts"][0]["text"]
        data = json.loads(raw_json)
        if {"title", "scheduled_at"} <= data.keys():
            return {
                "title": str(data["title"]).strip() or "Нова подія",
                "scheduled_at": str(data["scheduled_at"]).strip(),
                "priority": "high" if data.get("priority") == "high" else "normal",
            }
    except Exception as e:
        print(f"[NLP:{HOST_ID}] Gemini fallback to local rules: {e}", flush=True)
    return None


def parse_locally(text: str, now_local: datetime) -> dict:
    lowered = text.lower()
    if "післязавтра" in lowered:
        days_offset = 2
    elif "сьогодні" in lowered:
        days_offset = 0
    else:
        days_offset = 1

    time_match = re.search(r"(\d{1,2}[:.]\d{2})", text)
    event_time = time_match.group(1).replace(".", ":") if time_match else "12:00"
    if len(event_time) == 4:
        event_time = "0" + event_time

    priority = "high" if any(k in lowered for k in ("терміново", "важливо", "дедлайн")) else "normal"
    target_date = (now_local + timedelta(days=days_offset)).strftime("%Y-%m-%d")

    title = re.sub(
        r"(післязавтра|завтра|сьогодні|(?:о|на)\s*\d{1,2}[:.]\d{2}|нагадай\s*мені\s*про|нагадай\s*про|нагадай|терміново|важливо)",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip(" ,.-!")

    return {
        "title": title or "Нова подія",
        "scheduled_at": f"{target_date} {event_time}",
        "priority": priority,
    }


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    register_in_consul()
    yield
    deregister_from_consul()


app = FastAPI(title="NLP Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME, "instance": HOST_ID}


@app.post("/nlp/parse")
def parse_text(req: ParseRequest):
    now_local = get_local_now(req.timezone)
    llm_result = parse_with_gemini(req.text, now_local)
    if llm_result:
        parsed, mode = llm_result, "gemini"
    else:
        parsed, mode = parse_locally(req.text, now_local), "local_rules"

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """INSERT INTO parse_logs
               (raw_text, parsed_title, scheduled_at, priority, parser_mode, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                req.text,
                parsed["title"],
                parsed["scheduled_at"],
                parsed["priority"],
                mode,
                now_local.strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )

    print(f"[NLP:{HOST_ID}] ({mode}) '{req.text}' -> '{parsed['title']}' [{parsed['scheduled_at']}]", flush=True)
    return {
        "title": parsed["title"],
        "scheduled_at": parsed["scheduled_at"],
        "priority": parsed["priority"],
        "parser_mode": mode,
        "timezone": req.timezone,
        "instance": f"{SERVICE_NAME}:{HOST_ID}",
    }


@app.get("/nlp/history")
def get_history():
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT id, raw_text, parsed_title, scheduled_at, priority, parser_mode, created_at FROM parse_logs ORDER BY id DESC"
        ).fetchall()
    return {
        "instance": HOST_ID,
        "logs": [
            {
                "id": r[0],
                "raw_text": r[1],
                "title": r[2],
                "scheduled_at": r[3],
                "priority": r[4],
                "parser_mode": r[5],
                "created_at": r[6],
            }
            for r in rows
        ],
    }
