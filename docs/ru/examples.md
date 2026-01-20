# Examples

Практические примеры использования DataFlow Operator для различных сценариев обработки данных.

## Простой Kafka → PostgreSQL поток

Базовый пример передачи данных из Kafka топика в PostgreSQL таблицу.

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-to-postgres
spec:
  source:
    type: kafka
    kafka:
      brokers:
        - localhost:9092
      topic: input-topic
      consumerGroup: dataflow-group
  sink:
    type: postgresql
    postgresql:
      connectionString: "postgres://dataflow:dataflow@postgres:5432/dataflow?sslmode=disable"
      table: output_table
      autoCreateTable: true
```

**Применение:**
```bash
kubectl apply -f config/samples/kafka-to-postgres.yaml
```

## С трансформациями (Flatten + Timestamp)

Пример обработки сообщений с массивом товаров, развертывание в отдельные сообщения и добавление временной метки.

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: stock-flatten
spec:
  source:
    type: kafka
    kafka:
      brokers:
        - localhost:9092
      topic: stock-topic
      consumerGroup: dataflow-group
  sink:
    type: postgresql
    postgresql:
      connectionString: "postgres://dataflow:dataflow@postgres:5432/dataflow?sslmode=disable"
      table: stock_items
      autoCreateTable: true
      batchSize: 50
  transformations:
    # Развернуть массив rowsStock в отдельные сообщения
    - type: flatten
      flatten:
        field: rowsStock
    # Добавить временную метку
    - type: timestamp
      timestamp:
        fieldName: created_at
```

**Входное сообщение:**
```json
{
  "type": "stock",
  "version": 32476984,
  "rowsStock": [
    {"sku": 400125868, "section": "A015"},
    {"sku": 400125868, "section": "A001"}
  ]
}
```

**Выходные сообщения:**
```json
{
  "type": "stock",
  "version": 32476984,
  "sku": 400125868,
  "section": "A015",
  "created_at": "2024-01-15T10:30:00Z"
}
```

```json
{
  "type": "stock",
  "version": 32476984,
  "sku": 400125868,
  "section": "A001",
  "created_at": "2024-01-15T10:30:00Z"
}
```

## С роутером для множественных приемников

Пример маршрутизации сообщений в разные приемники на основе условий.

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: router-example
spec:
  source:
    type: kafka
    kafka:
      brokers:
        - localhost:9092
      topic: events
      consumerGroup: dataflow-group
  # Основной приемник для сообщений, не соответствующих условиям
  sink:
    type: kafka
    kafka:
      brokers:
        - localhost:9092
      topic: default-events
  transformations:
    - type: router
      router:
        routes:
          # Ошибки → отдельный топик
          - condition: "$.level"
            sink:
              type: kafka
              kafka:
                brokers:
                  - localhost:9092
                topic: error-events
          # Предупреждения → PostgreSQL
          - condition: "$.priority"
            sink:
              type: postgresql
              postgresql:
                connectionString: "postgres://dataflow:dataflow@postgres:5432/dataflow?sslmode=disable"
                table: warnings
                autoCreateTable: true
```

**Входные сообщения:**
```json
{"level": "error", "message": "Critical error"}     // → error-events топик
{"priority": "high", "message": "Warning"}          // → warnings таблица
{"message": "Info"}                                 // → default-events топик
```

## С фильтрацией и маскированием

Пример обработки пользовательских данных с фильтрацией активных пользователей, маскированием чувствительных данных и удалением внутренних полей.

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: secure-pipeline
spec:
  source:
    type: postgresql
    postgresql:
      connectionString: "postgres://dataflow:dataflow@postgres:5432/dataflow?sslmode=disable"
      table: users
      query: "SELECT * FROM users WHERE updated_at > NOW() - INTERVAL '1 hour'"
      pollInterval: 300
  sink:
    type: kafka
    kafka:
      brokers:
        - localhost:9092
      topic: public-users
  transformations:
    # Фильтровать только активных пользователей
    - type: filter
      filter:
        condition: "$.active"

    # Маскировать чувствительные данные
    - type: mask
      mask:
        fields:
          - password
          - email
        keepLength: true

    # Удалить внутренние поля
    - type: remove
      remove:
        fields:
          - internal_id
          - secret_token
          - debug_info

    # Добавить временную метку экспорта
    - type: timestamp
      timestamp:
        fieldName: exported_at
```

**Входное сообщение:**
```json
{
  "id": 1,
  "username": "john",
  "email": "john@example.com",
  "password": "secret123",
  "active": true,
  "internal_id": 999,
  "secret_token": "abc123"
}
```

**Выходное сообщение:**
```json
{
  "id": 1,
  "username": "john",
  "email": "***********",
  "password": "*********",
  "active": true,
  "exported_at": "2024-01-15T10:30:00Z"
}
```

## PostgreSQL → Kafka с выбором полей

Пример чтения из PostgreSQL, выборки определенных полей и отправки в Kafka.

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: postgres-to-kafka-select
spec:
  source:
    type: postgresql
    postgresql:
      connectionString: "postgres://dataflow:dataflow@postgres:5432/dataflow?sslmode=disable"
      table: orders
      query: "SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '1 day'"
      pollInterval: 60
  sink:
    type: kafka
    kafka:
      brokers:
        - localhost:9092
      topic: order-events
  transformations:
    - type: select
      select:
        fields:
          - order_id
          - customer_id
          - total
          - status
          - created_at
    - type: timestamp
      timestamp:
        fieldName: processed_at
```

## PostgreSQL → PostgreSQL (репликация / ETL)

Пример чтения данных из одной PostgreSQL базы и записи преобразованных данных в другую PostgreSQL базу.

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: postgres-to-postgres
spec:
  source:
    type: postgresql
    postgresql:
      connectionString: "postgres://dataflow:dataflow@source-postgres:5432/source_db?sslmode=disable"
      table: source_orders
      query: "SELECT * FROM source_orders WHERE updated_at > NOW() - INTERVAL '5 minutes'"
      pollInterval: 60
  sink:
    type: postgresql
    postgresql:
      connectionString: "postgres://dataflow:dataflow@target-postgres:5432/target_db?sslmode=disable"
      table: target_orders
      autoCreateTable: true
      batchSize: 100
  transformations:
    # Оставляем только нужные поля
    - type: select
      select:
        fields:
          - id
          - customer_id
          - total
          - status
          - updated_at
    # Добавляем время синхронизации
    - type: timestamp
      timestamp:
        fieldName: synced_at
```

**Варианты использования:**

- **Онлайн-репликация**: периодическое копирование обновленных записей из операционной БД в аналитическую
- **ETL-пайплайн**: подготовка и очистка данных при переносе между схемами/кластерами PostgreSQL


## Комплексный пример: ETL пайплайн

Полноценный ETL пайплайн с множественными трансформациями.

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: etl-pipeline
spec:
  source:
    type: kafka
    kafka:
      brokers:
        - kafka1:9092
        - kafka2:9092
      topic: raw-events
      consumerGroup: etl-group
  sink:
    type: postgresql
    postgresql:
      connectionString: "postgres://dataflow:dataflow@postgres:5432/analytics?sslmode=disable"
      table: processed_events
      autoCreateTable: true
      batchSize: 100
  transformations:
    # 1. Развернуть вложенные массивы
    - type: flatten
      flatten:
        field: items

    # 2. Добавить временную метку обработки
    - type: timestamp
      timestamp:
        fieldName: processed_at
        format: RFC3339

    # 3. Фильтровать только валидные события
    - type: filter
      filter:
        condition: "$.valid"

    # 4. Маскировать PII данные
    - type: mask
      mask:
        fields:
          - user.email
          - user.phone
        keepLength: true

    # 5. Удалить отладочную информацию
    - type: remove
      remove:
        fields:
          - debug
          - internal_metadata
          - test_flag

    # 6. Выбрать только нужные поля для финального результата
    - type: select
      select:
        fields:
          - event_id
          - user.id
          - item.sku
          - item.quantity
          - processed_at
```

## Kafka → Kafka с роутингом по типам

Пример чтения из одного Kafka топика и маршрутизации в разные топики на основе типа события.

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-router
spec:
  source:
    type: kafka
    kafka:
      brokers:
        - localhost:9092
      topic: all-events
      consumerGroup: router-group
  sink:
    type: kafka
    kafka:
      brokers:
        - localhost:9092
      topic: default-events
  transformations:
    - type: router
      router:
        routes:
          - condition: "$.type"
            sink:
              type: kafka
              kafka:
                brokers:
                  - localhost:9092
                topic: user-events
          - condition: "$.category"
            sink:
              type: kafka
              kafka:
                brokers:
                  - localhost:9092
                topic: product-events
```

      autoCreateNamespace: true
      autoCreateTable: true
  transformations:
    - type: timestamp
      timestamp:
        fieldName: ingested_at
    - type: remove
      remove:
        fields:
          - internal_id
          - audit_trail
    - type: mask
      mask:
        fields:
          - credit_card
        keepLength: true
```

## Использование Secrets для credentials
      url: "amqp://guest:guest@localhost:5672/"
      queue: events-queue
      exchange: events-exchange
      routingKey: events.*
  sink:
    type: postgresql
    postgresql:
      connectionString: "postgres://dataflow:dataflow@postgres:5432/dataflow?sslmode=disable"
      table: events
      autoCreateTable: true
      batchSize: 50
  transformations:
    - type: filter
      filter:
        condition: "$.status"
    - type: timestamp
      timestamp:
        fieldName: received_at
```

## Использование Secrets для credentials

DataFlow Operator поддерживает конфигурацию коннекторов из Kubernetes Secrets через поля `*SecretRef`. Это позволяет безопасно хранить чувствительные данные без их явного указания в спецификации DataFlow.

### Пример: Kafka → PostgreSQL с Secrets

#### Шаг 1: Создание Secrets

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-credentials
  namespace: default
type: Opaque
stringData:
  brokers: "kafka1:9092,kafka2:9092"
  topic: "input-topic"
  consumerGroup: "dataflow-group"
  username: "kafka-user"
  password: "kafka-password"
---
apiVersion: v1
kind: Secret
metadata:
  name: postgres-credentials
  namespace: default
type: Opaque
stringData:
  connectionString: "postgres://user:password@postgres:5432/dbname?sslmode=disable"
  table: "output_table"
```

#### Шаг 2: DataFlow с SecretRef

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: secure-dataflow
spec:
  source:
    type: kafka
    kafka:
      brokersSecretRef:
        name: kafka-credentials
        key: brokers
      topicSecretRef:
        name: kafka-credentials
        key: topic
      consumerGroupSecretRef:
        name: kafka-credentials
        key: consumerGroup
      sasl:
        mechanism: scram-sha-256
        usernameSecretRef:
          name: kafka-credentials
          key: username
        passwordSecretRef:
          name: kafka-credentials
          key: password
  sink:
    type: postgresql
    postgresql:
      connectionStringSecretRef:
        name: postgres-credentials
        key: connectionString
      tableSecretRef:
        name: postgres-credentials
        key: table
      autoCreateTable: true
```

**Применение:**
```bash
kubectl apply -f config/samples/kafka-to-postgres-secrets.yaml
```

### Пример: TLS сертификаты из Secrets

Для TLS конфигурации оператор автоматически определяет, является ли значение из secret путем к файлу или содержимым сертификата.

**Как это работает:**
- Если значение начинается с `-----BEGIN` (например, `-----BEGIN CERTIFICATE-----`), оператор распознает его как содержимое сертификата и создает временный файл
- Если значение не начинается с `-----BEGIN` и существует как файл, оно используется как путь к файлу
- Сертификаты могут храниться в секретах как в текстовом формате (PEM), так и в base64-кодированном виде (в поле `data` секрета)

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-tls-certs
type: Opaque
stringData:
  ca.crt: |
    -----BEGIN CERTIFICATE-----
    MIIDXTCCAkWgAwIBAgIJAK...
    -----END CERTIFICATE-----
  client.crt: |
    -----BEGIN CERTIFICATE-----
    MIIDXTCCAkWgAwIBAgIJAK...
    -----END CERTIFICATE-----
  client.key: |
    -----BEGIN PRIVATE KEY-----
    MIIEvQIBADANBgkqhkiG9w0...
    -----END PRIVATE KEY-----
---
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-tls-secure
spec:
  source:
    type: kafka
    kafka:
      brokers:
        - secure-kafka:9093
      topic: secure-topic
      tls:
        caSecretRef:
          name: kafka-tls-certs
          key: ca.crt
        certSecretRef:
          name: kafka-tls-certs
          key: client.crt
        keySecretRef:
          name: kafka-tls-certs
          key: client.key
```

### Пример: Secrets в разных namespace

Вы можете использовать secrets из других namespace:

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: cross-namespace-secrets
  namespace: dataflow
spec:
  source:
    type: postgresql
    postgresql:
      connectionStringSecretRef:
        name: postgres-credentials
        namespace: shared-secrets  # Другой namespace
        key: connectionString
```

### Преимущества использования SecretRef

- **Безопасность**: Credentials не хранятся в спецификации DataFlow
- **Управление**: Централизованное управление secrets через Kubernetes
- **Ротация**: Обновление secrets без изменения DataFlow ресурсов
- **RBAC**: Контроль доступа через Kubernetes RBAC

Подробнее см. раздел [Использование Secrets в Kubernetes](connectors.md#использование-secrets-в-kubernetes) в документации по коннекторам.

## Мониторинг и отладка

### Проверка статуса DataFlow

```bash
# Получить список всех DataFlow
kubectl get dataflow

# Детальная информация
kubectl describe dataflow <name>

# Статус в формате YAML
kubectl get dataflow <name> -o yaml
```

### Просмотр логов

```bash
# Логи оператора
kubectl logs -l app.kubernetes.io/name=dataflow-operator -f

# События Kubernetes
kubectl get events --sort-by='.lastTimestamp' | grep dataflow
```

### Проверка обработанных сообщений

Статус DataFlow содержит метрики:

```yaml
status:
  phase: Running
  processedCount: 1500
  errorCount: 2
  lastProcessedTime: "2024-01-15T10:30:00Z"
  message: "Processing messages successfully"
```

## Рекомендации

### Производительность

- Используйте `batchSize` для PostgreSQL приемников
- Настройте правильный `pollInterval` для PostgreSQL источников
- Используйте несколько инстансов оператора для масштабирования

### Безопасность

- Используйте Kubernetes Secrets для credentials
- Включайте TLS для Kafka соединений
- Маскируйте чувствительные данные перед отправкой

### Надежность

- Настройте правильные consumer groups для Kafka
- Мониторьте статус DataFlow ресурсов

## Дополнительные примеры

Больше примеров можно найти в директории `config/samples/`:

- `kafka-to-postgres.yaml` - базовый Kafka → PostgreSQL
- `flatten-example.yaml` - пример с Flatten трансформацией
- `router-example.yaml` - пример с Router трансформацией
