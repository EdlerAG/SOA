import re
from typing import Literal
from urllib.parse import urlparse
from pydantic import BaseModel, Field, field_validator


class GuardrailError(Exception):
    def __init__(self, level: str, code: str, message: str):
        self.level = level
        self.code = code
        self.message = message
        super().__init__(message)


INJECTION_RULES = [
    (re.compile(r"ignore\s+(all\s+)?(previous|prior|above)\s+instructions?", re.I), "Спроба перехоплення інструкцій"),
    (re.compile(r"(ігноруй|забудь)\s+(всі\s+)?попередні\s+інструкції", re.I), "Спроба перехоплення інструкцій"),
    (re.compile(r"(reveal|print|show|dump)\s+(your\s+)?(system\s+prompt|secrets?|env|api[_\s]?key)", re.I), "Спроба витоку системного промпту або ключів"),
    (re.compile(r"(покажи|виведи)\s+системний\s+промпт", re.I), "Спроба витоку системного промпту"),
    (re.compile(r"<\s*\|?im_start\|?\s*>|<\/?system>", re.I), "Службові теги в запиті"),
    (re.compile(r";\s*(drop|delete|truncate|alter)\s+table", re.I), "SQL-ін'єкція"),
    (re.compile(r"https?://(169\.254\.169\.254|127\.0\.0\.1|0\.0\.0\.0|metadata\.google)", re.I), "Підозрілий внутрішній URL (SSRF)"),
    (re.compile(r"\.\./\.\./"), "Вихід за межі шляху"),
]

ALLOWED_METHODS = {"GET", "POST", "PUT"}
ALLOWED_PATHS = ("/api/users", "/api/events", "/api/notifications", "/api/nlp/")


def estimate_tokens(text: str) -> int:
    return max(1, int(len(text) / 3.5))


class TokenBudget:
    def __init__(self, max_prompt: int = 2000, max_total: int = 3500, max_calls: int = 6):
        self.max_prompt = max_prompt
        self.max_total = max_total
        self.max_calls = max_calls
        self.prompt_tokens = 0
        self.completion_tokens = 0
        self.tool_calls = 0

    def add_prompt(self, text: str) -> None:
        t = estimate_tokens(text)
        if t > self.max_prompt:
            raise GuardrailError("Level-1", "PROMPT_TOKEN_LIMIT", f"Промпт перевищує ліміт ({t} > {self.max_prompt}).")
        self.prompt_tokens += t
        self.check()

    def add_completion(self, text: str) -> None:
        self.completion_tokens += estimate_tokens(text)
        self.check()

    def add_tool_result(self, raw: str) -> str:
        self.tool_calls += 1
        if self.tool_calls > self.max_calls:
            raise GuardrailError("Level-1", "MAX_TOOL_CALLS_EXCEEDED", f"Перевищено ліміт викликів ({self.tool_calls} > {self.max_calls}).")
        short = raw[:1200]
        self.prompt_tokens += estimate_tokens(short)
        self.check()
        return short

    def check(self) -> None:
        total = self.prompt_tokens + self.completion_tokens
        if total > self.max_total:
            raise GuardrailError("Level-1", "TOTAL_TOKEN_BUDGET_EXCEEDED", f"Вичерпано бюджет токенів ({total} > {self.max_total}).")

    def to_dict(self) -> dict:
        return {
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.prompt_tokens + self.completion_tokens,
            "max_total_tokens": self.max_total,
            "tool_calls": self.tool_calls,
            "max_tool_calls": self.max_calls,
        }


def check_text_safety(text: str, field: str = "prompt") -> str:
    if len(text) > 2500:
        raise GuardrailError("Level-3", "PROMPT_TOO_LONG", f"Поле {field} занадто довге.")
    for pattern, reason in INJECTION_RULES:
        if pattern.search(text):
            code = "PROMPT_INJECTION_BLOCKED" if field == "prompt" else "UNSAFE_TOOL_ARGUMENT"
            raise GuardrailError("Level-3", code, f"Блокування ({field}): {reason}")
    return text.strip()


def sanitize_output(text: str) -> str:
    cleaned = text
    for pattern, _ in INJECTION_RULES:
        cleaned = pattern.sub("[BLOCKED]", cleaned)
    return cleaned


def check_http_scope(base_url: str, method: str, path: str) -> None:
    if method.upper() not in ALLOWED_METHODS:
        raise GuardrailError("Level-3", "FORBIDDEN_HTTP_METHOD", f"Метод {method} заборонено.")
    if not any(path.startswith(p) for p in ALLOWED_PATHS):
        raise GuardrailError("Level-3", "OUT_OF_SCOPE_ENDPOINT", f"Шлях {path} не входить у білий список.")
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or "169.254" in (parsed.hostname or ""):
        raise GuardrailError("Level-3", "INVALID_BASE_URL", "Недопустимий базовий URL.")


class FindUserArgs(BaseModel):
    contact: str = Field(min_length=3, max_length=120)

    @field_validator("contact")
    @classmethod
    def check_contact(cls, v: str) -> str:
        return check_text_safety(v, "contact")


class ListEventsArgs(BaseModel):
    user_id: int = Field(ge=1, le=100000)


class CreateEventArgs(BaseModel):
    user_id: int = Field(ge=1, le=100000)
    text: str = Field(min_length=3, max_length=500)
    source: Literal["ai-agent", "web", "telegram", "mcp"] = "ai-agent"

    @field_validator("text")
    @classmethod
    def check_event_text(cls, v: str) -> str:
        return check_text_safety(v, "text")


class UpdateEventArgs(BaseModel):
    event_id: int = Field(ge=1, le=1000000)
    title: str = Field(min_length=2, max_length=200)
    scheduled_at: str = Field(min_length=10, max_length=32)
    status: str = Field(default="ОНОВЛЕНО", min_length=2, max_length=40)

    @field_validator("title", "status")
    @classmethod
    def check_fields(cls, v: str) -> str:
        return check_text_safety(v, "title/status")
