import argparse
import os
import re
import time
from collections import deque
import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://api-gateway").rstrip("/")
MCP_PORT = int(os.getenv("MCP_PORT", "8090"))

INJECTION_REGEX = re.compile(
    r"(ignore\s+(all\s+)?(previous|prior)\s+instructions?|reveal\s+system\s+prompt|"
    r"ігноруй\s+попередні\s+інструкції|;\s*(drop|delete)\s+table|169\.254\.169\.254|\.\./)",
    re.I,
)

_call_times: deque[float] = deque()

mcp = FastMCP(
    name="SmartCalendarMCP",
    host="0.0.0.0",
    port=MCP_PORT,
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)


def guard_text(value: str, field: str, max_len: int = 400) -> str:
    clean = value.strip()
    if not clean or len(clean) > max_len:
        raise ValueError(f"Некоректна довжина поля '{field}'.")
    if INJECTION_REGEX.search(clean):
        raise ValueError(f"Guardrail заблокував підозрілий вміст у полі '{field}'.")
    return clean


def check_rate_limit() -> None:
    now = time.time()
    while _call_times and now - _call_times[0] > 60:
        _call_times.popleft()
    if len(_call_times) >= 120:
        raise ValueError("Перевищено ліміт викликів за хвилину.")
    _call_times.append(now)


@mcp.tool()
def list_users() -> dict:
    check_rate_limit()
    res = httpx.get(f"{GATEWAY_URL}/api/users", timeout=8.0)
    res.raise_for_status()
    return res.json()


@mcp.tool()
def find_user_by_contact(contact: str) -> dict:
    check_rate_limit()
    safe_contact = guard_text(contact, "contact", 120)
    res = httpx.get(f"{GATEWAY_URL}/api/users", params={"contact": safe_contact}, timeout=8.0)
    res.raise_for_status()
    return res.json()


@mcp.tool()
def list_events(user_id: int | None = None) -> dict:
    check_rate_limit()
    params = {}
    if user_id is not None:
        if user_id < 1 or user_id > 100000:
            raise ValueError("user_id має бути в межах 1..100000.")
        params["user_id"] = user_id
    res = httpx.get(f"{GATEWAY_URL}/api/events", params=params, timeout=8.0)
    res.raise_for_status()
    return res.json()


@mcp.tool()
def create_event(user_id: int, text: str) -> dict:
    check_rate_limit()
    if user_id < 1 or user_id > 100000:
        raise ValueError("user_id має бути в межах 1..100000.")
    safe_text = guard_text(text, "text", 400)
    res = httpx.post(
        f"{GATEWAY_URL}/api/events",
        json={"user_id": user_id, "text": safe_text, "source": "mcp"},
        timeout=10.0,
    )
    res.raise_for_status()
    return res.json()


@mcp.tool()
def update_event(event_id: int, title: str, scheduled_at: str, status: str = "ОНОВЛЕНО") -> dict:
    check_rate_limit()
    if event_id < 1 or event_id > 1000000:
        raise ValueError("Некоректний event_id.")
    res = httpx.put(
        f"{GATEWAY_URL}/api/events/{event_id}",
        json={
            "title": guard_text(title, "title", 200),
            "scheduled_at": guard_text(scheduled_at, "scheduled_at", 32),
            "status": guard_text(status, "status", 40),
        },
        timeout=10.0,
    )
    res.raise_for_status()
    return res.json()


@mcp.tool()
def list_notifications() -> dict:
    check_rate_limit()
    res = httpx.get(f"{GATEWAY_URL}/api/notifications", timeout=8.0)
    res.raise_for_status()
    return res.json()


@mcp.resource("calendar://events")
def resource_all_events() -> str:
    res = httpx.get(f"{GATEWAY_URL}/api/events", timeout=8.0)
    res.raise_for_status()
    return res.text


@mcp.resource("calendar://notifications")
def resource_all_notifications() -> str:
    res = httpx.get(f"{GATEWAY_URL}/api/notifications", timeout=8.0)
    res.raise_for_status()
    return res.text


@mcp.resource("calendar://users/{user_id}")
def resource_user_profile(user_id: int) -> str:
    if user_id < 1 or user_id > 100000:
        raise ValueError("Некоректний user_id у URI.")
    res = httpx.get(f"{GATEWAY_URL}/api/users/{user_id}", timeout=8.0)
    res.raise_for_status()
    return res.text


@mcp.prompt()
def schedule_meeting_prompt(contact: str, topic: str) -> str:
    safe_contact = guard_text(contact, "contact", 120)
    safe_topic = guard_text(topic, "topic", 200)
    return (
        f"Знайди користувача '{safe_contact}' через find_user_by_contact, "
        f"перевір його події (list_events) та створи або онови зустріч на тему '{safe_topic}'."
    )


@mcp.prompt()
def daily_digest_prompt(user_id: int) -> str:
    if user_id < 1 or user_id > 100000:
        raise ValueError("Некоректний user_id.")
    return f"Прочитай calendar://users/{user_id} та підсумуй події й сповіщення користувача."


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--transport", choices=["stdio", "streamable-http"], default=os.getenv("MCP_TRANSPORT", "streamable-http"))
    args = parser.parse_args()
    mcp.run(transport=args.transport)
