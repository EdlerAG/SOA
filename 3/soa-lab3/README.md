# Лабораторна робота №3 — Transactional Outbox, Inbox та SAGA (`soa-lab3`)

## Що реалізовано на базі ЛР2
1. **Transactional Outbox (`calendar-service`)**:
   - При `POST /api/events` та `PUT /api/events/{id}` запис у `events`, `outbox_messages` (`status='NEW'`) та `saga_log` (`state='STARTED'`) виконується в **одній локальній транзакції SQLite**.
   - Фоновий `outbox_relay_worker` вичитує нові повідомлення з `outbox_messages`, публікує їх у RabbitMQ (`calendar_events_queue`) і змінює їхній статус на `SENT`.
2. **Transactional Inbox (`notification-service`)**:
   - Таблиця `inbox_messages` зберігає `message_id` кожного вхідного повідомлення та забезпечує ідемпотентність (захист від повторної обробки дублікатів при доставці *At-least-once*).
3. **SAGA з відкатом через Dead Letter Queue (DLQ) та за таймаутом**:
   - Черга `calendar_events_queue` налаштована з `x-dead-letter-routing-key: calendar_events_dlq`.
   - Якщо передати `"simulate_error": true` (або слово `"помилка"` в тексті події), консьюмер `notification-service` кидає виняток і викликає `basic_nack(..., requeue=False)`.
   - Повідомлення потрапляє в `calendar_events_dlq`, звідки його вичитує `calendar-service` і виконує **компенсаційну транзакцію**: переводить подію у статус `СКАСОВАНО (SAGA ROLLBACK)` (або повертає попередні значення при `PUT`) і фіксує причину в `saga_log` (`state='COMPENSATED'`).
   - Також `outbox_relay_worker` перевіряє `SAGA_TIMEOUT_SEC` і автоматично відкочує завислі транзакції по таймауту.

## Запуск та перевірка в PowerShell

```powershell
cd "D:\Потрібне\5 курс\ІСОС\Лаби\3\soa-lab3"
docker compose up --build -d
```

### 1. Успішна SAGA (Outbox -> RabbitMQ -> Inbox -> Підтвердження)
```powershell
$ok = @{ user_id = 1; text = "Зустріч по диплому завтра о 14:00"; source = "telegram"; simulate_error = $false } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/events" -ContentType "application/json; charset=utf-8" -Body $ok
Start-Sleep -Seconds 3
Invoke-RestMethod "http://localhost:8080/api/events"
Invoke-RestMethod "http://localhost:8080/api/outbox"
Invoke-RestMethod "http://localhost:8080/api/saga"
```

### 2. Збій у консьюмері -> DLQ -> Компенсаційний відкат SAGA
```powershell
$fail = @{ user_id = 1; text = "Помилка синхронізації післязавтра о 16:00"; source = "telegram"; simulate_error = $true } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/events" -ContentType "application/json; charset=utf-8" -Body $fail
Start-Sleep -Seconds 3
Invoke-RestMethod "http://localhost:8080/api/events"
Invoke-RestMethod "http://localhost:8080/api/saga"
Invoke-RestMethod "http://localhost:8080/api/inbox"
```
