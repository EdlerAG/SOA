# Лабораторна робота №6 — Спостережуваність і AIOps у SOA (`soa-lab6`)

## Реалізовано згідно з методичкою `ЛР 6.docx`
1. **Prometheus Metrics + Consul Service Discovery**:
   - Усі 4 сервіси експортують `/metrics`: `http_requests_total`, `http_request_duration_seconds` (`p50`, `p95`, `p99`), `calendar_events_created_total`, `notifications_processed_total`.
   - `prometheus/prometheus.yml` автоматично знаходить усі репліки через `consul_sd_configs`.
2. **Структуровані JSON-логи з наскрізним `trace_id`**:
   - Nginx генерує `X-Trace-ID` (`$request_id`) і логує JSON у `stdout`.
   - `calendar-service` прокидає `X-Trace-ID` по HTTP у `user-service` та `nlp-service`, а також у повідомлення RabbitMQ для `notification-service`.
3. **Grafana Dashboard (`http://localhost:3000`, `admin`/`admin`)**:
   - Автоматичний provisioning datasource Prometheus та дашборду `SOA Calendar — Observability` (Health, Throughput, Error Rate, Latency p50/p95/p99, Replica Distribution, Business Metrics).
4. **Alerting (`prometheus/alerts.yml` + `alertmanager/alertmanager.yml`)**:
   - Правила `ServiceDown`, `HighLatencyP99` (>0.5с), `HighErrorRate` (>5%).
   - Alertmanager пересилає спрацьовані алерти на webhook **n8n** (`http://n8n:5678/webhook/prometheus-alert`).
5. **n8n Automated Incident Response (`http://localhost:5678`)**:
   - Готовий workflow `n8n/incident_workflow.json` приймає алерт від Alertmanager, форматує тікет (`INC-XXXX`) та автоматично зберігає його в `POST http://calendar-service:8000/incidents` (або у Slack/Jira webhook).

## Запуск та тестування (PowerShell)

```powershell
cd "D:\Потрібне\5 курс\ІСОС\Лаби\6\soa-lab6"
docker compose up --build -d --scale nlp-service=3
```

1. **Генерація трафіку та перевірка наскрізного `trace_id`:**
```powershell
1..5 | ForEach-Object {
  $body = @{ user_id = 1; text = "Моніторинг ЛР6 #$_ завтра о 12:00"; source = "web" } | ConvertTo-Json
  Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/events" -ContentType "application/json; charset=utf-8" -Body $body
}
docker compose logs --tail=15 calendar-service nlp-service notification-service
```
2. **Симуляція високої затримки (`HighLatencyP99`) та 5xx помилок (`HighErrorRate`):**
```powershell
1..15 | ForEach-Object { Invoke-RestMethod "http://localhost:8080/api/events/simulate-slow" }
1..5 | ForEach-Object { try { Invoke-RestMethod "http://localhost:8080/api/events/simulate-error" } catch {} }
```
3. **Інциденти, створені через n8n:**
   - Імпортувати `n8n/incident_workflow.json` у UI n8n (`http://localhost:5678`) та активувати workflow (`Active`).
   - Переглянути створені інцидент-тікети: `Invoke-RestMethod "http://localhost:8080/api/incidents"` (або `http://calendar-service:8000/incidents`).
