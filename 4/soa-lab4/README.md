# Лабораторна робота №4 — Інтеграція сервісних API з AI-агентами

Розширення мікросервісної архітектури Smart Calendar (ЛР 1–3) підтримкою **Function Calling**, **Model Context Protocol (MCP)** та **Agent-to-Agent (A2A)**.

## Архітектура та порти
- **API Gateway (Nginx):** `http://localhost:8080`
- **Function Calling Client API:** `http://localhost:8093` (Рівні 1–3 guardrails, порівняння `non-reasoning` / `reasoning`, ланцюговий виклик інструментів)
- **MCP Server (`FastMCP`, `stdio` + `streamable-http`):** `http://localhost:8090/mcp` (та через Gateway `http://localhost:8080/mcp`)
- **MCP Inspector UI (`@modelcontextprotocol/inspector`):** `http://localhost:6274` (підключення до `http://mcp-server:8090/mcp` через `Streamable HTTP`)
- **A2A Producer Agent (`a2a-producer`):** `http://localhost:8091/.well-known/agent.json` та JSON-RPC `POST /a2a`
- **A2A Consumer Agent (`a2a-consumer`):** `http://localhost:8092/.well-known/agent.json` та JSON-RPC `POST /a2a`

## Запуск і тестування
```bash
docker compose up --build -d
```
