import os
import time
import uuid
from datetime import datetime, timezone
import httpx
from fastapi import FastAPI
from pydantic import BaseModel

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://api-gateway").rstrip("/")
PRODUCER_AGENT_URL = os.getenv("PRODUCER_AGENT_URL", "http://a2a-producer:8091").rstrip("/")
AGENT_URL = os.getenv("AGENT_URL", "http://a2a-consumer:8092").rstrip("/")

app = FastAPI(title="A2A Notification Consumer Agent")
TASKS_STORE: dict[str, dict] = {}

AGENT_CARD = {
    "name": "Notification Consumer AI Agent",
    "description": "AI-агент консюмера з ЛР 3: взаємодіє з Producer AI Agent через A2A та верифікує доставку сповіщень.",
    "url": f"{AGENT_URL}/a2a",
    "version": "1.0.0",
    "capabilities": {"streaming": False, "pushNotifications": False, "stateTransitionHistory": True},
    "defaultInputModes": ["text/plain", "application/json"],
    "defaultOutputModes": ["application/json", "text/plain"],
    "skills": [
        {
            "id": "orchestrate-and-consume",
            "name": "Discover Producer & Orchestrate Event Notification",
            "description": "Читає Agent Card продюсера, делегує створення події через A2A tasks/send та перевіряє доставку сповіщення консюмером.",
            "tags": ["a2a", "consumer", "notifications", "orchestration"],
            "examples": ["Заплануй зустріч 'A2A Колаборація завтра о 15:00' для artem@pm.me і підтвердь сповіщення"],
        }
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
    return {"status": "ok", "agent": "a2a-consumer"}


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

    task_id = req.params.get("id") or f"ctask-{uuid.uuid4().hex[:10]}"
    session_id = req.params.get("sessionId") or f"csess-{uuid.uuid4().hex[:8]}"
    goal_text = _extract_text(req.params) or "A2A Демо-зустріч завтра о 15:00 для artem@pm.me"

    card_res = httpx.get(f"{PRODUCER_AGENT_URL}/.well-known/agent.json", timeout=6.0)
    card_res.raise_for_status()
    producer_card = card_res.json()
    producer_rpc_url = producer_card["url"]
    producer_skills = [s["id"] for s in producer_card.get("skills", [])]
    if "produce-calendar-event" not in producer_skills:
        raise RuntimeError("Producer Agent Card does not expose produce-calendar-event skill")

    delegated_id = f"ptask-{uuid.uuid4().hex[:8]}"
    rpc_payload = {
        "jsonrpc": "2.0",
        "id": "a2a-subcall-1",
        "method": "tasks/send",
        "params": {
            "id": delegated_id,
            "sessionId": session_id,
            "message": {"role": "user", "parts": [{"type": "text", "text": goal_text}]},
        },
    }
    prod_res = httpx.post(producer_rpc_url, json=rpc_payload, timeout=12.0)
    prod_res.raise_for_status()
    prod_result = prod_res.json().get("result", {})
    event_data = prod_result.get("artifacts", [{}])[0].get("parts", [{}])[0].get("data", {})
    event_id = event_data.get("event_id")

    matched_notif = None
    for _ in range(12):
        nres = httpx.get(f"{GATEWAY_URL}/api/notifications", timeout=6.0).json()
        notifs = nres.get("notifications", [])
        matched_notif = next((n for n in notifs if n.get("event_id") == event_id), None)
        if matched_notif:
            break
        time.sleep(1.0)

    ai_digest = (
        f"Споживач отримав підтвердження для події #{event_id} ('{event_data.get('title')}'): "
        f"сповіщення доставлено в канал {matched_notif['channel']} ({matched_notif['recipient']})."
        if matched_notif
        else f"Подію #{event_id} створено продюсером; асинхронне сповіщення ще обробляється."
    )

    task = {
        "id": task_id,
        "sessionId": session_id,
        "status": {
            "state": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message": {"role": "agent", "parts": [{"type": "text", "text": ai_digest}]},
        },
        "artifacts": [
            {
                "name": "a2a_collaboration_report",
                "parts": [
                    {
                        "type": "data",
                        "data": {
                            "discovered_producer": producer_card["name"],
                            "verified_skills": producer_skills,
                            "delegated_task_id": delegated_id,
                            "producer_event": event_data,
                            "consumer_notification": matched_notif,
                            "ai_summary": ai_digest,
                        },
                    }
                ],
            }
        ],
    }
    TASKS_STORE[task_id] = task
    return {"jsonrpc": "2.0", "id": req.id, "result": task}
