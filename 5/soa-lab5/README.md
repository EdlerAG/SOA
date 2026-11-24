# Лабораторна робота №5 — Безпека сервісних API (`soa-lab5`)

## Реалізовано згідно з методичкою `ЛР 5 СОА.docx`
1. **CORS для сайту кафедри `cad.kpi.ua`**:
   - У кожному сервісі FastAPI підключено `CORSMiddleware` з `allow_origins=["https://cad.kpi.ua", "http://cad.kpi.ua", "http://localhost:8080"]`.
   - У `gateway/nginx.conf` заголовки `Access-Control-Allow-*` передаються прозоро без перезапису.
2. **OAuth2 Authorization Code Grant + PKCE (Keycloak)**:
   - Автоматичний імпорт Realm `soa-calendar` (`keycloak/realm-soa.json`), порт `8180`, користувач `yaroslav` / `kpi2026`.
   - Веб-сторінка для демонстрації Auth Code Flow доступна на `http://localhost:8080/`.
3. **Захист JWT-токенами (RS256 через JWKS Keycloak)**:
   - Без токена всі бізнес-ендпоінти повертають `401 Unauthorized`.
   - `calendar-service` прокидає `Authorization: Bearer <JWT>` до `user-service` та `nlp-service` (Zero Trust).
4. **Окремі OAuth2 Scopes для різних сервісів**:
   - `user-service`: `users:read`, `users:write`
   - `nlp-service`: `nlp:parse`, `nlp:read`
   - `calendar-service`: `calendar:read`, `calendar:write`
   - `notification-service`: `notifications:read`

## Команди для перевірки (PowerShell)

```powershell
cd "D:\Потрібне\5 курс\ІСОС\Лаби\5\soa-lab5"
docker compose up --build -d
```

1. **Перевірка CORS для `https://cad.kpi.ua` (preflight OPTIONS):**
```powershell
curl.exe -i -X OPTIONS http://localhost:8080/api/events `
  -H "Origin: https://cad.kpi.ua" `
  -H "Access-Control-Request-Method: GET" `
  -H "Access-Control-Request-Headers: Authorization"
```
2. **Перевірка без токена -> `401 Unauthorized`:**
```powershell
curl.exe -i http://localhost:8080/api/events
```
3. **OAuth2 Auth Code Grant у браузері:** відкрити `http://localhost:8080/` → натиснути «Увійти через Keycloak» → увійти як `yaroslav` / `kpi2026`.
4. **Перевірка окремих Scopes (`403 Forbidden` при нестачі прав):** зняти галочку `calendar:write` на сторінці `http://localhost:8080/`, увійти заново й натиснути `POST /api/events` → сервіс поверне `HTTP 403`.
