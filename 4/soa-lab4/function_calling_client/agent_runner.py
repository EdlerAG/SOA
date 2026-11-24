import os
import json
import re
import httpx
from openai import APIError, OpenAI, RateLimitError
from pydantic import ValidationError

from guardrails import (
    CreateEventArgs,
    FindUserArgs,
    GuardrailError,
    ListEventsArgs,
    TokenBudget,
    UpdateEventArgs,
    check_http_scope,
    check_text_safety,
    sanitize_output,
)

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://api-gateway").rstrip("/")
LLM_API_KEY = (os.getenv("LLM_API_KEY") or os.getenv("GEMINI_API_KEY") or "").strip()
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://generativelanguage.googleapis.com/v1beta/openai/").strip()
LLM_MODEL = os.getenv("LLM_MODEL", "gemini-2.5-flash").strip()

NON_REASONING_PROMPT = (
    "Ти помічник календаря Smart Calendar. Виконуй кроки строго по порядку: "
    "1) find_user_by_contact, 2) list_user_events, "
    "3) якщо подія вже є — update_event, якщо немає — create_event. Відповідай стисло українською."
)

REASONING_PROMPT = (
    "Ти агент керування календарем Smart Calendar. "
    "Виконай запит користувача з мінімальною кількістю викликів інструментів і змінюй дані лише за прямою умовою."
)

TOOLS_SCHEMA = [
    {
        "type": "function",
        "function": {
            "name": "find_user_by_contact",
            "description": "Пошук користувача за email або контактом.",
            "parameters": {
                "type": "object",
                "properties": {"contact": {"type": "string"}},
                "required": ["contact"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_user_events",
            "description": "Отримати події користувача за user_id.",
            "parameters": {
                "type": "object",
                "properties": {"user_id": {"type": "integer"}},
                "required": ["user_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_event",
            "description": "Створити нову подію календаря.",
            "parameters": {
                "type": "object",
                "properties": {
                    "user_id": {"type": "integer"},
                    "text": {"type": "string"},
                    "source": {"type": "string"},
                },
                "required": ["user_id", "text"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_event",
            "description": "Оновити існуючу подію за event_id.",
            "parameters": {
                "type": "object",
                "properties": {
                    "event_id": {"type": "integer"},
                    "title": {"type": "string"},
                    "scheduled_at": {"type": "string"},
                    "status": {"type": "string"},
                },
                "required": ["event_id", "title", "scheduled_at"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_notifications",
            "description": "Отримати журнал сповіщень.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
]


def call_tool(name: str, arguments: dict) -> dict:
    try:
        if name == "find_user_by_contact":
            args = FindUserArgs(**arguments)
            check_http_scope(GATEWAY_URL, "GET", "/api/users")
            return httpx.get(f"{GATEWAY_URL}/api/users", params={"contact": args.contact}, timeout=8.0).json()

        if name == "list_user_events":
            args = ListEventsArgs(**arguments)
            check_http_scope(GATEWAY_URL, "GET", "/api/events")
            return httpx.get(f"{GATEWAY_URL}/api/events", params={"user_id": args.user_id}, timeout=8.0).json()

        if name == "create_event":
            args = CreateEventArgs(**arguments)
            check_http_scope(GATEWAY_URL, "POST", "/api/events")
            res = httpx.post(f"{GATEWAY_URL}/api/events", json=args.model_dump(), timeout=10.0)
            res.raise_for_status()
            return res.json()

        if name == "update_event":
            args = UpdateEventArgs(**arguments)
            check_http_scope(GATEWAY_URL, "PUT", f"/api/events/{args.event_id}")
            res = httpx.put(
                f"{GATEWAY_URL}/api/events/{args.event_id}",
                json={"title": args.title, "scheduled_at": args.scheduled_at, "status": args.status},
                timeout=10.0,
            )
            res.raise_for_status()
            return res.json()

        if name == "list_notifications":
            check_http_scope(GATEWAY_URL, "GET", "/api/notifications")
            return httpx.get(f"{GATEWAY_URL}/api/notifications", timeout=8.0).json()
    except ValidationError as exc:
        raise GuardrailError("Level-3", "INVALID_TOOL_ARGS", f"Некоректні аргументи {name}: {exc}") from exc

    raise GuardrailError("Level-3", "UNKNOWN_TOOL", f"Невідомий інструмент: {name}")


def next_local_step(prompt: str, trace: list[dict], mode: str):
    email_match = re.search(r"[\w.+-]+@[\w-]+\.[\w.-]+", prompt)
    quoted = re.findall(r"['«\"]([^'»\"]+)['»\"]", prompt)
    target = quoted[-1] if quoted else "send your CA-2"

    if email_match:
        contact = email_match.group(0)
        if len(trace) == 0:
            return "find_user_by_contact", {"contact": contact}, None
        if len(trace) == 1:
            users = trace[0]["result"].get("users", [])
            if not users:
                return None, None, f"Користувача {contact} не знайдено."
            return "list_user_events", {"user_id": users[0]["id"]}, None
        if len(trace) == 2:
            user_id = trace[0]["result"]["users"][0]["id"]
            events = trace[1]["result"].get("events", [])
            found = next((e for e in events if "CA-2" in e.get("title", "") or target in e.get("title", "")), None)
            if found:
                return "update_event", {
                    "event_id": found["id"],
                    "title": target,
                    "scheduled_at": found["scheduled_at"],
                    "status": "ОНОВЛЕНО",
                }, None
            return "create_event", {"user_id": user_id, "text": f"{target} завтра о 14:00", "source": "ai-agent"}, None
        last = trace[-1]
        verb = "оновлено" if last["tool"] == "update_event" else "створено"
        return None, None, f"[{mode}] Користувача {contact} знайдено, подію {verb} (event_id={last['result'].get('event_id')})."

    if "сповіщ" in prompt.lower() or "notification" in prompt.lower():
        if not trace:
            return "list_notifications", {}, None
        return None, None, f"[{mode}] Отримано сповіщень: {len(trace[-1]['result'].get('notifications', []))}."

    if not trace:
        return "create_event", {"user_id": 1, "text": prompt, "source": "ai-agent"}, None
    return None, None, f"[{mode}] Подію створено (event_id={trace[-1]['result'].get('event_id')})."


def run_agent_prompt(prompt: str, model_mode: str = "non-reasoning", max_total_tokens: int = 8000, max_tool_calls: int = 6) -> dict:
    check_text_safety(prompt, "prompt")
    budget = TokenBudget(max_prompt=3000, max_total=max_total_tokens, max_calls=max_tool_calls)
    system_prompt = REASONING_PROMPT if model_mode == "reasoning" else NON_REASONING_PROMPT
    budget.add_prompt(system_prompt + "\n" + prompt + json.dumps(TOOLS_SCHEMA, ensure_ascii=False))

    messages = [{"role": "system", "content": system_prompt}, {"role": "user", "content": prompt}]
    trace: list[dict] = []

    backend_name = "local-planner"
    if LLM_API_KEY:
        client = OpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL, max_retries=0)
        try:
            for _ in range(max_tool_calls + 2):
                resp = client.chat.completions.create(model=LLM_MODEL, messages=messages, tools=TOOLS_SCHEMA, tool_choice="auto")
                msg = resp.choices[0].message
                if resp.usage:
                    budget.prompt_tokens = resp.usage.prompt_tokens
                    budget.completion_tokens = resp.usage.completion_tokens
                    budget.check()
                if not msg.tool_calls:
                    return {"backend": LLM_MODEL, "model_mode": model_mode, "answer": msg.content, "tool_trace": trace, "token_usage": budget.to_dict()}
                messages.append(msg.model_dump(exclude_none=True))
                for tc in msg.tool_calls:
                    raw_args = json.loads(tc.function.arguments or "{}")
                    res = call_tool(tc.function.name, raw_args)
                    safe = budget.add_tool_result(sanitize_output(json.dumps(res, ensure_ascii=False)))
                    trace.append({"tool": tc.function.name, "args": raw_args, "result": res})
                    messages.append({"role": "tool", "tool_call_id": tc.id, "content": safe})
            raise GuardrailError("Level-1", "LOOP_LIMIT", "Перевищено ліміт ітерацій.")
        except (RateLimitError, APIError):
            backend_name = f"{LLM_MODEL}-fallback-local"
            done = [t for t in trace if t["tool"] in ("create_event", "update_event")]
            if done:
                return {"backend": backend_name, "model_mode": model_mode, "answer": f"[{model_mode}] Операцію вже виконано (event_id={done[-1]['result'].get('event_id')}).", "tool_trace": trace, "token_usage": budget.to_dict()}
            trace = []

    for _ in range(max_tool_calls + 2):
        tool_name, tool_args, answer = next_local_step(prompt, trace, model_mode)
        if answer is not None:
            budget.add_completion(answer)
            return {"backend": backend_name, "model_mode": model_mode, "answer": answer, "tool_trace": trace, "token_usage": budget.to_dict()}
        res = call_tool(tool_name, tool_args or {})
        budget.add_tool_result(sanitize_output(json.dumps(res, ensure_ascii=False)))
        budget.add_completion(json.dumps({"tool": tool_name, "args": tool_args}, ensure_ascii=False))
        trace.append({"tool": tool_name, "args": tool_args, "result": res})

    raise GuardrailError("Level-1", "LOOP_LIMIT", "Перевищено ліміт ітерацій.")
