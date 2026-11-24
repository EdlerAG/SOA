import os
import re
import uuid
from datetime import datetime, timezone
import httpx
from fastapi import FastAPI
from pydantic import BaseModel

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://api-gateway").rstrip("/")
AGENT_URL = os.getenv("AGENT_URL", "http://a2a-producer:8091").rstrip("/")

app = FastAPI(title="A2A Calendar Producer Agent")
TASKS_STORE: dict[str, dict] = {}

AGENT_CARD = {
    "name": "Calendar Producer AI Agent",
    "description": "AI-агент продюсера з ЛР 3: планує події через NLP/Outbox та емітує події у RabbitMQ.",
    "url": f"{AGENT_URL}/a2a",
    "version": "1.0.0",
    "capabilities": {"streaming": False, "pushNotifications": False, "stateTransitionHistory": True},
    "defaultInputModes": ["text/plain", "application/json"],
    "defaultOutputModes": ["application/json", "text/plain"],
    "skills": [
        {
            "id": "produce-calendar-event",
            "name": "Produce Calendar Event & Outbox Task",
            "description": "Приймає текстовий запит, знаходить користувача за контактом і створює подію в Transactional Outbox.",
            "tags": ["calendar", "outbox", "rabbitmq", "producer"],
            "examples": ["Створи подію: A2A стендап завтра о 15:00 для artem@pm.me"],
        },
        {
            "id": "query-saga-status",
            "name": "Query SAGA & Outbox State",
            "description": "Повертає актуальний стан Outbox/SAGA логів.",
            "tags": ["saga", "outbox"],
        },
    ],
}


class A2ARpcRequest(BaseModel):
    jsonrpc: str = "2.0"
    id: str | int = "1"
    method: str
    params: dict = {}


def _extract_text(params: dict) -> str:
    msg = params.get("message", {})
    parts = msg.get("parts", [])
    texts = [p.get("text", "") for p in parts if isinstance(p, dict) and (p.get("type") or p.get("kind")) == "text"]
    return " ".join(t for t in texts if t).strip()


@app.get("/health")
def health():
    return {"status": "ok", "agent": "a2a-producer"}


@app.get("/.well-known/agent.json")
@app.get("/.well-known/agent-card.json")
def get_agent_card():
    return AGENT_CARD


@app.post("/a2a")
def handle_a2a_rpc(req: A2ARpcRequest):
    if req.method == "tasks/get":
        task_id = req.params.get("id", "")
        task = TASKS_STORE.get(task_id)
        if not task:
            return {"jsonrpc": "2.0", "id": req.id, "error": {"code": -32001, "message": "Task not found"}}
        return {"jsonrpc": "2.0", "id": req.id, "result": task}

    if req.method not in {"tasks/send", "message/send"}:
        return {"jsonrpc": "2.0", "id": req.id, "error": {"code": -32601, "message": f"Method {req.method} not found"}}

    task_id = req.params.get("id") or f"task-{uuid.uuid4().hex[:10]}"
    session_id = req.params.get("sessionId") or f"sess-{uuid.uuid4().hex[:8]}"
    prompt_text = _extract_text(req.params)

    if re.search(r"ignore\s+.*instructions|;\s*drop\s+table", prompt_text, re.I):
        failed_task = {
            "id": task_id,
            "sessionId": session_id,
            "status": {"state": "failed", "timestamp": datetime.now(timezone.utc).isoformat()},
            "artifacts": [{"name": "guardrail_error", "parts": [{"type": "text", "text": "A2A Guardrail blocked unsafe payload"}]}],
        }
        TASKS_STORE[task_id] = failed_task
        return {"jsonrpc": "2.0", "id": req.id, "result": failed_task}

    if ("saga" in prompt_text.lower() or "outbox" in prompt_text.lower()) and "створ" not in prompt_text.lower():
        saga_res = httpx.get(f"{GATEWAY_URL}/api/saga", timeout=8.0).json()
        task = {
            "id": task_id,
            "sessionId": session_id,
            "status": {"state": "completed", "timestamp": datetime.now(timezone.utc).isoformat()},
            "artifacts": [{"name": "saga_status", "parts": [{"type": "data", "data": saga_res}]}],
        }
        TASKS_STORE[task_id] = task
        return {"jsonrpc": "2.0", "id": req.id, "result": task}

    user_id = 1
    email_match = re.search(r"[\w.+-]+@[\w-]+\.[\w.-]+", prompt_text)
    if email_match:
        ures = httpx.get(f"{GATEWAY_URL}/api/users", params={"contact": email_match.group(0)}, timeout=6.0)
        if ures.status_code == 200 and ures.json().get("users"):
            user_id = ures.json()["users"][0]["id"]

    ev_res = httpx.post(
        f"{GATEWAY_URL}/api/events",
        json={"user_id": user_id, "text": prompt_text or "A2A Подія завтра о 15:00", "source": "a2a-producer"},
        timeout=10.0,
    )
    ev_res.raise_for_status()
    created = ev_res.json()

    task = {
        "id": task_id,
        "sessionId": session_id,
        "status": {
            "state": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message": {
                "role": "agent",
                "parts": [{"type": "text", "text": f"Продюсер створив подію #{created['event_id']} та помістив у Outbox (saga={created['saga_id']})."}],
            },
        },
        "artifacts": [
            {
                "name": "produced_calendar_event",
                "parts": [{"type": "data", "data": created}],
            }
        ],
    }
    TASKS_STORE[task_id] = task
    return {"jsonrpc": "2.0", "id": req.id, "result": task}
