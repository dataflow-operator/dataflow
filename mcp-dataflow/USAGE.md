# Использование DataFlow MCP Server

Это руководство описывает, как использовать MCP сервер для управления DataFlow Operator через AI-ассистента в Cursor.

## Настройка в Cursor

После установки MCP сервера (см. [INSTALLATION.md](INSTALLATION.md)), добавьте его в конфигурацию Cursor.

### Конфигурация для Docker

**macOS/Linux:**
```json
{
  "mcpServers": {
    "dataflow": {
      "command": "docker",
      "args": [
        "run",
        "--rm",
        "-i",
        "-v", "${HOME}/.kube/config:/root/.kube/config:ro",
        "mcp-dataflow:latest",
        "--kubeconfig", "/root/.kube/config"
      ]
    }
  }
}
```

**Windows:**
```json
{
  "mcpServers": {
    "dataflow": {
      "command": "docker",
      "args": [
        "run",
        "--rm",
        "-i",
        "-v", "${USERPROFILE}\\.kube\\config:/root/.kube/config:ro",
        "mcp-dataflow:latest",
        "--kubeconfig", "/root/.kube/config"
      ]
    }
  }
}
```

### Конфигурация для бинарника

**macOS/Linux:**
```json
{
  "mcpServers": {
    "dataflow": {
      "command": "/usr/local/bin/mcp-dataflow",
      "args": ["--kubeconfig", "${HOME}/.kube/config"]
    }
  }
}
```

**Windows:**
```json
{
  "mcpServers": {
    "dataflow": {
      "command": "C:\\tools\\mcp-dataflow.exe",
      "args": ["--kubeconfig", "${USERPROFILE}\\.kube\\config"]
    }
  }
}
```

## Примеры использования

### 1. Создание простого потока данных

**Запрос к AI:**
> "Создай DataFlow из Kafka топика 'events' в PostgreSQL таблицу 'processed_events'"

**Что происходит:**
- AI использует инструмент `generate_dataflow_config` для создания конфигурации
- Затем использует `create_dataflow` для применения в Kubernetes

### 2. Просмотр существующих потоков

**Запрос к AI:**
> "Покажи все DataFlow ресурсы в namespace 'production'"

**Что происходит:**
- AI использует инструмент `list_dataflows` с параметром namespace

### 3. Проверка статуса потока

**Запрос к AI:**
> "Какой статус у DataFlow 'kafka-to-postgres'?"

**Что происходит:**
- AI использует инструмент `get_dataflow_status` для получения детального статуса

### 4. Создание потока с трансформациями

**Запрос к AI:**
> "Создай поток из Kafka в PostgreSQL с фильтрацией ошибок и маскированием паролей"

**Что происходит:**
- AI генерирует конфигурацию с трансформациями:
  - `filter` - для фильтрации ошибок
  - `mask` - для маскирования паролей

### 5. Генерация конфигурации с маршрутизацией

**Запрос к AI:**
> "Создай router поток, который направляет ошибки в отдельный Kafka топик, а предупреждения в PostgreSQL"

**Что происходит:**
- AI создает конфигурацию с трансформацией `router` и несколькими приемниками

## Доступные инструменты

### Управление ресурсами

- **create_dataflow** - создание нового DataFlow ресурса
- **get_dataflow** - получение информации о ресурсе
- **list_dataflows** - список всех ресурсов
- **delete_dataflow** - удаление ресурса
- **get_dataflow_status** - детальный статус

### Генерация и валидация

- **generate_dataflow_config** - генерация YAML конфигурации
- **validate_dataflow_config** - валидация конфигурации

### Справочная информация

- **list_transformations** - список доступных трансформаций
- **list_connectors** - список поддерживаемых коннекторов

### Мониторинг

- **get_dataflow_metrics** - получение метрик (требует настройки Prometheus)

## Поддерживаемые источники данных

### Kafka
- Чтение из топиков
- Поддержка TLS и SASL аутентификации
- Поддержка Avro схем

### PostgreSQL
- Чтение из таблиц
- Поддержка кастомных SQL запросов
- Polling режим

### Trino
- Чтение из таблиц
- Поддержка Keycloak OAuth2
- Поддержка кастомных SQL запросов

## Поддерживаемые приемники данных

### Kafka
- Запись в топики
- Поддержка TLS и SASL аутентификации

### PostgreSQL
- Запись в таблицы
- Автоматическое создание таблиц
- Поддержка UPSERT режима
- Batch вставки

### Trino
- Запись в таблицы
- Поддержка Keycloak OAuth2
- Автоматическое создание таблиц
- Batch вставки

## Доступные трансформации

1. **timestamp** - добавление временной метки
2. **flatten** - разворачивание массивов
3. **filter** - фильтрация по условию JSONPath
4. **mask** - маскирование чувствительных данных
5. **router** - маршрутизация в разные приемники
6. **select** - выбор определенных полей
7. **remove** - удаление полей
8. **snakeCase** - конвертация имен полей в snake_case
9. **camelCase** - конвертация имен полей в CamelCase

## Безопасность

### Использование Kubernetes Secrets

Все конфигурации поддерживают использование `SecretRef` для безопасного хранения учетных данных:

```yaml
spec:
  source:
    type: kafka
    kafka:
      brokersSecretRef:
        name: kafka-secrets
        key: brokers
      topicSecretRef:
        name: kafka-secrets
        key: topic
```

AI может автоматически генерировать конфигурации с использованием Secrets при запросе.

## Устранение неполадок

### MCP сервер не отвечает

1. Проверьте, что сервер запущен:
   - Для Docker: `docker ps | grep mcp-dataflow`
   - Для бинарника: проверьте процесс

2. Проверьте логи Cursor на наличие ошибок

3. Убедитесь, что путь к kubeconfig правильный

### Ошибки при создании DataFlow

1. Проверьте права доступа к Kubernetes кластеру
2. Убедитесь, что CRD DataFlow установлен
3. Проверьте валидность конфигурации через `validate_dataflow_config`

### Проблемы с подключением к Kubernetes

1. Проверьте доступность кластера: `kubectl cluster-info`
2. Проверьте права доступа: `kubectl auth can-i create dataflows`
3. Убедитесь, что kubeconfig актуален

## Дополнительные ресурсы

- [Документация DataFlow Operator](../docs/en/index.md)
- [Примеры конфигураций](../config/samples/)
- [Инструкции по установке](INSTALLATION.md)
