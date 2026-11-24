# Інтелектуальні сервіс-орієнтовані системи (ІСОС / SOA)

Репозиторій лабораторних робіт з дисципліни «Інтелектуальні сервіс-орієнтовані системи».

## Структура
- `1/` — Лабораторна робота №1 (DDD, Event Storming, Context Mapping, діаграми `.excalidraw` та звіт `Lab1_Obikhod_SOA.docx`)
- `2/` — Лабораторна робота №2 (`soa-lab2`: 4 мікросервіси на FastAPI, Nginx API Gateway, Consul, RabbitMQ, Docker Compose та звіт `Lab2_Obikhod_SOA.docx`)
- `3/` — Лабораторна робота №3 (`soa-lab3`: Transactional Outbox, Transactional Inbox, SAGA з DLQ і таймаутом та звіт `Lab3_Obikhod_SOA.docx`)
- `4/` — Лабораторна робота №4 (`soa-lab4`: Function Calling із Guardrails рівнів 1–3, офіційний MCP-сервер `stdio`/`streamable-http`, A2A Producer/Consumer агенти та звіт `Lab4_Obikhod_SOA.docx`)
- `5/` — Лабораторна робота №5 (`soa-lab5`: CORS для `cad.kpi.ua`, Keycloak OAuth2 Authorization Code + PKCE, JWT і OAuth2 Scopes та звіт `Lab5_Obikhod_SOA.docx`)
- `6/` — Лабораторна робота №6 (`soa-lab6`: Prometheus через Consul SD, структуровані JSON-логи з `trace_id`, Loki, Grafana, Alertmanager, n8n та звіт `Lab6_Obikhod_SOA.docx`)

## Швидкий запуск на захисті
```powershell
.\run.ps1 4
.\run.ps1 stop
```
