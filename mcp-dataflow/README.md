# DataFlow MCP Server

MCP (Model Context Protocol) сервер для управления DataFlow Operator через AI-ассистента в Cursor.

## Быстрый старт

1. **Быстрая установка:** [QUICKSTART.md](QUICKSTART.md)
2. **Подробная установка:** [INSTALLATION.md](INSTALLATION.md)
3. **Использование:** [USAGE.md](USAGE.md)
4. **Разработка:** Используйте `task help` для просмотра доступных команд

## Возможности

MCP сервер предоставляет следующие инструменты для работы с DataFlow Operator:

### Управление DataFlow ресурсами

1. **create_dataflow** - создание нового DataFlow ресурса
   - Генерация YAML манифеста на основе описания
   - Валидация конфигурации
   - Применение через Kubernetes API

2. **get_dataflow** - получение информации о DataFlow ресурсе
   - Статус потока данных
   - Метрики обработки
   - Информация о подах процессора

3. **list_dataflows** - список всех DataFlow ресурсов
   - Фильтрация по namespace
   - Статусы всех потоков

4. **update_dataflow** - обновление существующего DataFlow ресурса
   - Изменение конфигурации источника/приемника
   - Добавление/удаление трансформаций

5. **delete_dataflow** - удаление DataFlow ресурса

### Генерация конфигураций

6. **generate_dataflow_config** - генерация конфигурации DataFlow
   - Создание манифеста на основе описания сценария
   - Поддержка всех типов источников (Kafka, PostgreSQL, Trino)
   - Поддержка всех типов приемников
   - Генерация трансформаций

7. **validate_dataflow_config** - валидация конфигурации
   - Проверка синтаксиса YAML
   - Валидация схемы CRD
   - Проверка обязательных полей

### Мониторинг и метрики

8. **get_dataflow_metrics** - получение метрик DataFlow
   - Количество обработанных сообщений
   - Время обработки
   - Количество ошибок
   - Статус подключения коннекторов

9. **get_dataflow_status** - детальный статус DataFlow
   - Фаза выполнения (Running, Error, Pending)
   - Последнее время обработки
   - Количество обработанных сообщений
   - Количество ошибок

### Работа с трансформациями

10. **list_transformations** - список доступных трансформаций
    - Описание каждой трансформации
    - Примеры использования

11. **generate_transformation** - генерация конфигурации трансформации
    - Создание конфигурации на основе описания
    - Валидация параметров

### Работа с коннекторами

12. **list_connectors** - список поддерживаемых коннекторов
    - Источники (Kafka, PostgreSQL, Trino)
    - Приемники (Kafka, PostgreSQL, Trino)
    - Параметры конфигурации

13. **test_connector_connection** - тестирование подключения
    - Проверка доступности источника/приемника
    - Валидация учетных данных

## Архитектура

MCP сервер реализован на Go и использует:
- Kubernetes client-go для работы с Kubernetes API
- Prometheus client для получения метрик
- YAML парсер для работы с конфигурациями

## Установка

Доступны два варианта установки:

### Вариант 1: Docker (Рекомендуется)

```bash
# Сборка образа
task docker-build

# Или используйте готовый образ
docker pull ghcr.io/ilyario/mcp-dataflow:latest
```

Подробные инструкции по настройке в Cursor см. в [INSTALLATION.md](INSTALLATION.md).

### Вариант 2: Бинарник

```bash
# Сборка для текущей платформы
task build

# Или сборка для всех платформ
task build-all
```

Бинарники будут в директории `dist/`. Подробные инструкции см. в [INSTALLATION.md](INSTALLATION.md).

### Предварительные требования

- [Task](https://taskfile.dev/) для сборки (или используйте команды напрямую)
- Go 1.21+ (только для сборки из исходников)
- Docker (для варианта с Docker)

## Конфигурация

MCP сервер требует:
- Доступ к Kubernetes кластеру (kubeconfig)
- Доступ к Prometheus endpoint (опционально, для метрик)

## Использование

MCP сервер подключается к Cursor через конфигурацию MCP серверов.

**Подробное руководство по использованию:** [USAGE.md](USAGE.md)

**Пример конфигурации для Docker:**
```json
{
  "mcpServers": {
    "dataflow": {
      "command": "docker",
      "args": [
        "run", "--rm", "-i",
        "-v", "${HOME}/.kube/config:/root/.kube/config:ro",
        "mcp-dataflow:latest"
      ]
    }
  }
}
```

**Пример конфигурации для бинарника:**
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

Подробные инструкции по настройке см. в [INSTALLATION.md](INSTALLATION.md).

## Примеры использования

### Создание простого потока Kafka -> PostgreSQL

```python
# Через MCP инструмент
create_dataflow(
    name="kafka-to-postgres",
    namespace="default",
    source_type="kafka",
    source_config={
        "brokers": ["localhost:9092"],
        "topic": "input-topic"
    },
    sink_type="postgresql",
    sink_config={
        "connectionString": "postgres://...",
        "table": "output_table"
    }
)
```

### Генерация конфигурации с трансформациями

```python
generate_dataflow_config(
    description="Поток из Kafka в PostgreSQL с фильтрацией ошибок и маскированием чувствительных данных",
    source="kafka",
    sink="postgresql",
    transformations=[
        {"type": "filter", "condition": "$.level != 'error'"},
        {"type": "mask", "fields": ["password", "token"]}
    ]
)
```

## Разработка

### Структура проекта

```
mcp-dataflow/
├── cmd/
│   └── server/
│       └── main.go          # Точка входа MCP сервера
├── internal/
│   ├── handlers/            # Обработчики MCP инструментов
│   │   ├── dataflow.go      # CRUD операции с DataFlow
│   │   ├── config.go        # Генерация конфигураций
│   │   └── metrics.go       # Работа с метриками
│   └── kubernetes/          # Kubernetes клиент
├── Dockerfile               # Docker образ
├── Taskfile.yml             # Task runner конфигурация
├── build.sh                 # Скрипт сборки для всех платформ
├── go.mod
├── go.sum
├── README.md
└── INSTALLATION.md          # Инструкции по установке
```

### Полезные команды

Используйте [Task](https://taskfile.dev/) для управления проектом:

```bash
# Показать все доступные команды
task help

# Сборка бинарника для текущей платформы
task build

# Сборка для всех платформ
task build-all

# Сборка Docker образа
task docker-build

# Запуск Docker контейнера для тестирования
task docker-run

# Запуск тестов
task test

# Запуск тестов с покрытием
task test-coverage

# Форматирование и проверка кода
task fmt
task vet
task lint

# Очистка артефактов
task clean
```

Все команды также можно выполнить напрямую через Go/Docker, см. [INSTALLATION.md](INSTALLATION.md).

### Добавление нового инструмента

1. Создать обработчик в `internal/handlers/`
2. Зарегистрировать в `cmd/server/main.go`
3. Добавить тесты
4. Обновить документацию

## Лицензия

Apache License 2.0
