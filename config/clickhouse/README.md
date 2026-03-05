# Конфигурация ClickHouse

Конфигурация ClickHouse для локальной разработки и тестирования DataFlow operator.

## Запуск

ClickHouse запускается через `docker-compose` из корня проекта `dataflow`:

```bash
cd dataflow
docker-compose up -d clickhouse
```

## Параметры подключения

| Параметр | Значение |
|----------|----------|
| **Host** | localhost |
| **HTTP порт** | 8123 |
| **Native порт** | 9000 |
| **База данных** | dataflow |
| **Пользователь** | dataflow |
| **Пароль** | dataflow |

**Connection string (Native):**
```
clickhouse://dataflow:dataflow@localhost:9000/dataflow?dial_timeout=10s
```

**JDBC URL (HTTP):**
```
jdbc:ch:http://localhost:8123/dataflow
```

## Структура конфигурации

- `users.d/default-user.xml` — пользователь `dataflow` с доступом из любой сети
- `users.d/session-settings.xml` — лимиты сессий и конкурентных запросов (см. SESSION_IS_LOCKED)

Конфиг монтируется в контейнер: `./config/clickhouse/users.d` → `/etc/clickhouse-server/users.d`

---

## Ошибка SESSION_IS_LOCKED (Code 373)

Ошибка возникает, когда **один и тот же session_id** используется несколькими запросами одновременно. ClickHouse блокирует сессию на время выполнения запроса.

### Решение 1: Настройка SQL-клиента (рекомендуется)

**DBeaver:**
1. Правый клик по подключению ClickHouse → **Edit Connection**
2. Вкладка **Connection settings** → **SQL Editor**
3. **Open separate connection for each editor** = **Always**
4. Вкладка **Metadata** → **Open separate connection for metadata read** = **Always**

**DataGrip:** Аналогично — настройте отдельное соединение для каждой вкладки редактора.

**Альтернатива — HTTP вместо Native:**
- Подключение через HTTP (порт 8123) без `session_id` — каждый запрос выполняется в отдельном контексте
- JDBC URL: `jdbc:ch:http://localhost:8123/dataflow` (драйвер ClickHouse)

### Решение 2: Серверная конфигурация

В `users.d/session-settings.xml` заданы:
- `max_sessions_for_user: 50` — больше одновременных сессий на пользователя
- `max_concurrent_queries_for_user: 50` — больше конкурентных запросов

Это помогает, когда несколько **разных** сессий работают параллельно. Не устраняет проблему переиспользования одной сессии в клиенте.

### Решение 3: Поведение при работе

- Не запускайте несколько запросов одновременно в одной вкладке
- Дождитесь завершения текущего запроса перед запуском следующего
- Используйте отдельные вкладки с отдельными подключениями

---

## Примеры DataFlow

Примеры конфигураций ClickHouse-to-ClickHouse: `config/samples/clickhouse-to-clickhouse.yaml`, `config/samples/clickhouse-to-clickhouse2.yaml`
