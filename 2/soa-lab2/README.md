# Лабораторна робота №2 — Мікросервісна система календаря (`soa-lab2`)

## Архітектура
- `api-gateway` (Nginx, `:8080`) — єдина точка входу, динамічний резолвінг сервісів через Docker DNS (`127.0.0.11`).
- `consul` (`:8500`) — Service Discovery та health-checks кожні 10с.
- `rabbitmq` (`:5672`, UI `:15672`, `guest`/`guest`) — брокер повідомлень (`calendar_events_queue`).
- `user-service` — профілі користувачів, часові пояси, канали сповіщень (`users.db`).
- `nlp-service` — розпізнавання дати/часу/пріоритету з тексту та логування (`nlp_history.db`).
- `calendar-service` — оркестратор, Round-Robin балансування через Consul, CRUD подій (`calendar.db`), публікація подій у RabbitMQ.
- `notification-service` — асинхронний консьюмер RabbitMQ, збереження доставлених сповіщень (`notifications.db`).

## Запуск у терміналі VS Code (PowerShell)

Перейти в папку проєкту та запустити з масштабуванням `nlp-service` до 3 реплік:

```powershell
cd "D:\Потрібне\5 курс\ІСОС\Лаби\2\soa-lab2"
docker compose up --build -d --scale nlp-service=3
docker compose ps
```

## Команди для перевірки та демонстрації

### 1. Перевірка Service Discovery (Consul)
```powershell
Invoke-RestMethod "http://localhost:8500/v1/health/service/nlp-service?passing=true" | ForEach-Object { $_.Service.ID }
```

### 2. Створення подій (синхронний ланцюжок + Round-Robin балансування + RabbitMQ)
```powershell
1..3 | ForEach-Object {
  $body = @{ user_id = 1; text = "Терміново захист ЛР #$_ завтра о 14:30"; source = "telegram" } | ConvertTo-Json
  Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/events" -ContentType "application/json; charset=utf-8" -Body $body
}
```

### 3. Перегляд подій, модифікація (`PUT`) та видалення (`DELETE`)
```powershell
Invoke-RestMethod "http://localhost:8080/api/events"

$upd = @{ title = "Захист ЛР (перенесено)"; scheduled_at = "2026-11-12 16:00"; status = "ПЕРЕНЕСЕНО" } | ConvertTo-Json
Invoke-RestMethod -Method Put -Uri "http://localhost:8080/api/events/1" -ContentType "application/json; charset=utf-8" -Body $upd

Invoke-RestMethod -Method Delete -Uri "http://localhost:8080/api/events/3"
```

### 4. Перевірка сповіщень з RabbitMQ та історії NLP
```powershell
Invoke-RestMethod "http://localhost:8080/api/notifications"
Invoke-RestMethod "http://localhost:8080/api/nlp/history"
docker compose logs --tail=20 calendar-service notification-service
```

### 5. Користувачі (`GET` / `POST`)
```powershell
Invoke-RestMethod "http://localhost:8080/api/users"
$u = @{ name = "Олена Коваль"; timezone = "Europe/Kyiv"; channel = "email"; contact = "olena@kpi.ua" } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/users" -ContentType "application/json; charset=utf-8" -Body $u
```

### Зупинка
```powershell
docker compose down
```
