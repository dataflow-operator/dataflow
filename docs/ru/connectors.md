# Connectors

DataFlow Operator поддерживает различные коннекторы для источников и приемников данных. Каждый коннектор реализует стандартный интерфейс и может использоваться как источник (source) или приемник (sink) данных.

## Обзор коннекторов

| Коннектор | Источник | Приемник | Особенности |
|-----------|----------|----------|-------------|
| Kafka | ✅ | ✅ | Consumer groups, TLS, SASL |
| PostgreSQL | ✅ | ✅ | SQL запросы, батч-вставки, автосоздание таблиц |
| RabbitMQ | ✅ | ✅ | Exchanges, routing keys, очереди |
| Iceberg | ✅ | ✅ | REST API, автосоздание namespace/таблиц |

## Kafka

Kafka коннектор поддерживает чтение и запись сообщений из/в топики Apache Kafka. Поддерживает consumer groups для масштабирования, TLS и SASL аутентификацию.

### Источник (Source)

Конфигурация Kafka источника:

```yaml
source:
  type: kafka
  kafka:
    # Список брокеров Kafka (обязательно)
    brokers:
      - kafka1:9092
      - kafka2:9092
      - kafka3:9092

    # Топик для чтения (обязательно)
    topic: input-topic

    # Consumer group (опционально, по умолчанию: dataflow-operator)
    consumerGroup: my-group

    # TLS конфигурация (опционально)
    tls:
      # Пропустить проверку сертификата (не рекомендуется для production)
      insecureSkipVerify: false
      # Путь к CA сертификату
      caFile: /path/to/ca.crt
      # Путь к клиентскому сертификату
      certFile: /path/to/client.crt
      # Путь к приватному ключу
      keyFile: /path/to/client.key

    # SASL аутентификация (опционально)
    sasl:
      # Механизм: plain, scram-sha-256, scram-sha-512
      mechanism: scram-sha-256
      username: kafka-user
      password: kafka-password
```

#### Особенности Kafka источника

- **Consumer Groups**: Используйте разные consumer groups для масштабирования обработки
- **Начальная позиция**: По умолчанию читает с самого старого сообщения (`OffsetOldest`)
- **Метаданные**: Каждое сообщение содержит метаданные:
  - `topic` - название топика
  - `partition` - номер партиции
  - `offset` - смещение сообщения
  - `key` - ключ сообщения (если есть)

#### Пример с TLS и SASL

```yaml
source:
  type: kafka
  kafka:
    brokers:
      - secure-kafka:9093
    topic: secure-topic
    consumerGroup: secure-group
    tls:
      caFile: /etc/kafka/ca.crt
      certFile: /etc/kafka/client.crt
      keyFile: /etc/kafka/client.key
    sasl:
      mechanism: scram-sha-512
      username: kafka-user
      password: ${KAFKA_PASSWORD}  # Используйте Secrets в Kubernetes
```

### Приемник (Sink)

Конфигурация Kafka приемника:

```yaml
sink:
  type: kafka
  kafka:
    # Список брокеров Kafka (обязательно)
    brokers:
      - kafka1:9092

    # Топик для записи (обязательно)
    topic: output-topic

    # TLS конфигурация (опционально, аналогично источнику)
    tls:
      caFile: /path/to/ca.crt
      certFile: /path/to/client.crt
      keyFile: /path/to/client.key

    # SASL конфигурация (опционально, аналогично источнику)
    sasl:
      mechanism: scram-sha-256
      username: kafka-user
      password: kafka-password
```

#### Особенности Kafka приемника

- **Синхронная запись**: Использует `SyncProducer` для гарантированной доставки
- **Ключи сообщений**: Сохраняет ключи из метаданных сообщения
- **Метаданные**: После записи обновляет метаданные с `partition` и `offset`

## PostgreSQL

PostgreSQL коннектор поддерживает чтение из таблиц и запись в таблицы PostgreSQL. Поддерживает кастомные SQL запросы, периодический опрос и батч-вставки.

### Источник (Source)

Конфигурация PostgreSQL источника:

```yaml
source:
  type: postgresql
  postgresql:
    # Connection string (обязательно)
    # Формат: postgres://user:password@host:port/dbname?sslmode=mode
    connectionString: "postgres://user:password@localhost:5432/dbname?sslmode=disable"

    # Таблица для чтения (обязательно, если не указан query)
    table: source_table

    # Кастомный SQL запрос (опционально)
    # Если указан, используется вместо чтения всей таблицы
    query: "SELECT * FROM source_table WHERE updated_at > NOW() - INTERVAL '1 hour'"

    # Интервал опроса в секундах (опционально, по умолчанию: 5)
    # Используется для периодического чтения новых данных
    pollInterval: 60
```

#### Особенности PostgreSQL источника

- **Периодический опрос**: Регулярно опрашивает таблицу на наличие новых данных
- **Кастомные запросы**: Поддержка сложных SQL запросов с JOIN, WHERE, и т.д.
- **Метаданные**: Каждое сообщение содержит метаданные:
  - `table` - название таблицы
- **Формат данных**: Данные преобразуются в JSON формат

#### Пример с кастомным запросом

```yaml
source:
  type: postgresql
  postgresql:
    connectionString: "postgres://user:password@localhost:5432/analytics"
    query: |
      SELECT
        u.id,
        u.email,
        o.order_id,
        o.total
      FROM users u
      JOIN orders o ON u.id = o.user_id
      WHERE o.created_at > NOW() - INTERVAL '1 day'
    pollInterval: 300  # Опрос каждые 5 минут
```

### Приемник (Sink)

Конфигурация PostgreSQL приемника:

```yaml
sink:
  type: postgresql
  postgresql:
    # Connection string (обязательно)
    connectionString: "postgres://user:password@localhost:5432/dbname?sslmode=disable"

    # Таблица для записи (обязательно)
    table: target_table

    # Размер батча для вставки (опционально, по умолчанию: 1)
    # Увеличьте для повышения производительности
    batchSize: 100

    # Автоматическое создание таблицы (опционально, по умолчанию: false)
    # Если true, создает таблицу с JSONB полем для гибкой схемы
    autoCreateTable: true
```

#### Особенности PostgreSQL приемника

- **Батч-вставки**: Группирует сообщения для эффективной записи
- **Автосоздание таблиц**: Автоматически создает таблицы с JSONB полем
- **Гибкая схема**: Поддерживает как JSONB (для автосозданных таблиц), так и колоночный формат
- **Индексы**: Автоматически создает GIN индекс на JSONB поле для быстрого поиска

#### Пример с автосозданием таблицы

```yaml
sink:
  type: postgresql
  postgresql:
    connectionString: "postgres://user:password@localhost:5432/analytics"
    table: events
    autoCreateTable: true  # Создаст таблицу с JSONB полем
    batchSize: 50
```

Автоматически созданная таблица будет иметь структуру:

```sql
CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_events_data ON events USING GIN (data);
```

## RabbitMQ

RabbitMQ коннектор поддерживает чтение из очередей и публикацию в очереди RabbitMQ. Поддерживает exchanges, routing keys и прямую публикацию в очереди.

### Источник (Source)

Конфигурация RabbitMQ источника:

```yaml
source:
  type: rabbitmq
  rabbitmq:
    # URL подключения (обязательно)
    # Формат: amqp://user:password@host:port/vhost
    url: "amqp://guest:guest@localhost:5672/"

    # Очередь для чтения (обязательно)
    queue: input-queue

    # Exchange для привязки очереди (опционально)
    exchange: input-exchange

    # Routing key для привязки (опционально, требуется если указан exchange)
    routingKey: input.key
```

#### Особенности RabbitMQ источника

- **Автоматическое объявление**: Автоматически объявляет очередь, если она не существует
- **Привязка к exchange**: Автоматически привязывает очередь к exchange, если указан
- **Подтверждение доставки**: Использует ручное подтверждение (manual ack) для надежности
- **Метаданные**: Каждое сообщение содержит:
  - `exchange` - название exchange
  - `routingKey` - routing key сообщения
  - `deliveryTag` - тег доставки

#### Пример с exchange и routing key

```yaml
source:
  type: rabbitmq
  rabbitmq:
    url: "amqp://user:password@rabbitmq:5672/vhost"
    queue: events-queue
    exchange: events-exchange
    routingKey: events.*  # Поддержка wildcards
```

### Приемник (Sink)

Конфигурация RabbitMQ приемника:

```yaml
sink:
  type: rabbitmq
  rabbitmq:
    # URL подключения (обязательно)
    url: "amqp://guest:guest@localhost:5672/"

    # Exchange для публикации (обязательно)
    exchange: output-exchange

    # Routing key (обязательно)
    routingKey: output.key

    # Прямая публикация в очередь (опционально)
    # Если указан, сообщения публикуются напрямую в очередь, минуя exchange
    queue: output-queue
```

#### Особенности RabbitMQ приемника

- **Автоматическое объявление exchange**: Автоматически объявляет exchange типа `topic`
- **Динамический routing key**: Может использовать routing key из метаданных сообщения
- **Надежная доставка**: Использует persistent messages для надежности

#### Пример с динамическим routing key

Если сообщение содержит метаданные `routingKey`, оно будет использовано вместо указанного в конфигурации:

```yaml
sink:
  type: rabbitmq
  rabbitmq:
    url: "amqp://user:password@rabbitmq:5672/"
    exchange: events-exchange
    routingKey: default.key  # Используется, если нет в метаданных
```

## Iceberg

Iceberg коннектор работает с Apache Iceberg через REST API. Оптимизирован для работы с большими данными и поддерживает автоматическое создание namespace и таблиц.

### Источник (Source)

Конфигурация Iceberg источника:

```yaml
source:
  type: iceberg
  iceberg:
    # URL REST Catalog (обязательно)
    restCatalogUrl: "http://iceberg-rest:8181"

    # Namespace в каталоге (обязательно)
    namespace: analytics

    # Название таблицы (обязательно)
    table: source_table

    # Токен аутентификации (опционально)
    token: "your-auth-token"
```

#### Особенности Iceberg источника

- **Стриминг чтение**: Использует Arrow RecordBatches для эффективного чтения
- **Поддержка типов**: Автоматически обрабатывает различные типы данных Iceberg
- **Метаданные**: Каждое сообщение содержит:
  - `namespace` - namespace таблицы
  - `table` - название таблицы

#### Требования для работы

Для работы Iceberg коннектора необходимо настроить переменные окружения для доступа к объектному хранилищу (S3/MinIO):

```bash
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password
export AWS_ENDPOINT_URL_S3=http://localhost:9000  # Для MinIO
```

### Приемник (Sink)

Конфигурация Iceberg приемника:

```yaml
sink:
  type: iceberg
  iceberg:
    # URL REST Catalog (обязательно)
    restCatalogUrl: "http://iceberg-rest:8181"

    # Namespace в каталоге (обязательно)
    namespace: analytics

    # Название таблицы (обязательно)
    table: target_table

    # Токен аутентификации (опционально)
    token: "your-auth-token"

    # Автоматическое создание namespace (опционально, по умолчанию: true)
    autoCreateNamespace: true

    # Автоматическое создание таблицы (опционально, по умолчанию: false)
    # Если true, создает таблицу с полем "data" типа String
    autoCreateTable: true
```

#### Особенности Iceberg приемника

- **Батч-запись**: Группирует сообщения в батчи для эффективной записи
- **Автосоздание**: Автоматически создает namespace и таблицы при необходимости
- **Обработка конфликтов**: Автоматически обрабатывает конфликты при параллельных записях
- **Arrow формат**: Использует Apache Arrow для эффективной сериализации данных

#### Пример с автосозданием

```yaml
sink:
  type: iceberg
  iceberg:
    restCatalogUrl: "http://iceberg-rest:8181"
    namespace: analytics
    table: events
    token: "${ICEBERG_TOKEN}"
    autoCreateNamespace: true
    autoCreateTable: true
```

Автоматически созданная таблица будет иметь схему с полем `data` типа `String`, в котором хранится JSON представление сообщения.

## Использование Secrets в Kubernetes

DataFlow Operator поддерживает конфигурацию коннекторов из Kubernetes Secrets. Это позволяет безопасно хранить чувствительные данные (пароли, токены, connection strings) без их явного указания в спецификации DataFlow.

### Обзор

Вместо указания значений напрямую в конфигурации, вы можете использовать ссылки на Kubernetes Secrets через поля `*SecretRef`. Оператор автоматически читает значения из secrets и подставляет их в конфигурацию коннекторов.

### Структура SecretRef

Каждая ссылка на secret имеет следующую структуру:

```yaml
secretRef:
  name: my-secret          # Имя secret (обязательно)
  namespace: my-namespace  # Namespace secret (опционально, по умолчанию - namespace DataFlow)
  key: my-key              # Ключ в secret (обязательно)
```

### Поддерживаемые поля

Все коннекторы поддерживают ссылки на secrets для следующих полей:

#### Kafka
- `brokersSecretRef` - список брокеров (через запятую)
- `topicSecretRef` - название топика
- `consumerGroupSecretRef` - consumer group
- `sasl.usernameSecretRef` - имя пользователя SASL
- `sasl.passwordSecretRef` - пароль SASL
- `tls.certSecretRef` - сертификат клиента
- `tls.keySecretRef` - приватный ключ
- `tls.caSecretRef` - CA сертификат

#### PostgreSQL
- `connectionStringSecretRef` - строка подключения
- `tableSecretRef` - название таблицы

#### Iceberg
- `restCatalogUrlSecretRef` - URL REST Catalog
- `namespaceSecretRef` - namespace в каталоге
- `tableSecretRef` - название таблицы
- `tokenSecretRef` - токен аутентификации

#### RabbitMQ
- `urlSecretRef` - URL подключения
- `queueSecretRef` - название очереди
- `exchangeSecretRef` - название exchange
- `routingKeySecretRef` - routing key

### Примеры использования

#### Пример 1: Kafka с SASL аутентификацией

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-credentials
  namespace: default
type: Opaque
stringData:
  brokers: "kafka1:9092,kafka2:9092,kafka3:9092"
  topic: "input-topic"
  consumerGroup: "dataflow-group"
  username: "kafka-user"
  password: "kafka-password"
---
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-example
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
        mechanism: "scram-sha-256"
        usernameSecretRef:
          name: kafka-credentials
          key: username
        passwordSecretRef:
          name: kafka-credentials
          key: password
```

#### Пример 2: PostgreSQL с connection string из secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: postgres-credentials
  namespace: default
type: Opaque
stringData:
  connectionString: "postgres://user:password@postgres:5432/dbname?sslmode=disable"
  table: "output_table"
---
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: postgres-example
spec:
  source:
    type: postgresql
    postgresql:
      connectionStringSecretRef:
        name: postgres-credentials
        key: connectionString
      tableSecretRef:
        name: postgres-credentials
        key: table
```

#### Пример 3: Kafka → PostgreSQL с secrets

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-credentials
type: Opaque
stringData:
  brokers: "localhost:9092"
  topic: "input-topic"
  consumerGroup: "dataflow-group"
---
apiVersion: v1
kind: Secret
metadata:
  name: postgres-credentials
type: Opaque
stringData:
  connectionString: "postgres://dataflow:dataflow@postgres:5432/dataflow?sslmode=disable"
  table: "output_table"
---
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-to-postgres-secrets
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
  sink:
    type: postgresql
    postgresql:
      connectionStringSecretRef:
        name: postgres-credentials
        key: connectionString
      tableSecretRef:
        name: postgres-credentials
        key: table
```

#### Пример 4: TLS сертификаты из secrets

Для TLS конфигурации оператор автоматически определяет, является ли значение из secret путем к файлу или содержимым сертификата.

**Определение типа значения:**
- Если значение начинается с `-----BEGIN` (например, `-----BEGIN CERTIFICATE-----` или `-----BEGIN PRIVATE KEY-----`), оператор считает его содержимым сертификата и создает временный файл
- Если значение не начинается с `-----BEGIN` и существует как файл в файловой системе, оно используется как путь к файлу
- Если значение не начинается с `-----BEGIN` и файл не существует, оно также обрабатывается как содержимое сертификата

**Поддерживаемые форматы:**
1. **Содержимое сертификата** (PEM формат):
   ```yaml
   ca.crt: |
     -----BEGIN CERTIFICATE-----
     MIIDXTCCAkWgAwIBAgIJAK...
     -----END CERTIFICATE-----
   ```

2. **Base64-кодированное содержимое** (в поле `data` секрета):
   ```yaml
   apiVersion: v1
   kind: Secret
   metadata:
     name: kafka-tls-certs
   type: Opaque
   data:
     ca.crt: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0t...  # base64
   ```
   Kubernetes автоматически декодирует base64 при чтении из секрета.

3. **Путь к файлу**:
   ```yaml
   ca.crt: /etc/kafka/ca.crt
   ```

**Пример с содержимым сертификата:**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-tls-certs
type: Opaque
stringData:
  # Содержимое CA сертификата
  ca.crt: |
    -----BEGIN CERTIFICATE-----
    ...
    -----END CERTIFICATE-----
  # Содержимое клиентского сертификата
  client.crt: |
    -----BEGIN CERTIFICATE-----
    ...
    -----END CERTIFICATE-----
  # Содержимое приватного ключа
  client.key: |
    -----BEGIN PRIVATE KEY-----
    ...
    -----END PRIVATE KEY-----
---
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-tls-example
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

**Важно:**
- Временные файлы создаются автоматически для содержимого сертификатов и удаляются после использования
- При использовании base64-кодированных значений в поле `data`, Kubernetes автоматически декодирует их при чтении
- Убедитесь, что сертификаты в правильном PEM формате с заголовками `-----BEGIN` и `-----END`

### Приоритет значений

Если указаны и прямое значение, и `SecretRef`, приоритет имеет `SecretRef`. Значение из secret будет использовано вместо прямого значения.

### Secrets в разных namespace

По умолчанию оператор ищет secrets в том же namespace, где находится DataFlow ресурс. Вы можете указать другой namespace:

```yaml
connectionStringSecretRef:
  name: postgres-credentials
  namespace: shared-secrets
  key: connectionString
```

### Безопасность

- **RBAC**: Оператору требуются права на чтение secrets (`get`, `list`, `watch`)
- **Изоляция**: Secrets должны быть в том же namespace или в namespace, к которому у оператора есть доступ
- **Временные файлы**: Для TLS сертификатов оператор создает временные файлы, которые автоматически удаляются после использования
  - Файлы создаются только для содержимого сертификатов (начинающихся с `-----BEGIN`)
  - Файлы удаляются автоматически при завершении работы оператора
  - Временные файлы создаются в системной временной директории с уникальными именами

### Устранение неполадок

#### Secret не найден

```bash
# Проверьте существование secret
kubectl get secret <secret-name> -n <namespace>

# Проверьте права оператора
kubectl auth can-i get secrets --as=system:serviceaccount:default:dataflow-operator
```

#### Ключ не найден в secret

```bash
# Проверьте ключи в secret
kubectl get secret <secret-name> -n <namespace> -o jsonpath='{.data}' | jq 'keys'
```

#### Ошибка разрешения secrets

Проверьте логи оператора:

```bash
kubectl logs -l app.kubernetes.io/name=dataflow-operator | grep -i secret
```

Убедитесь, что:
1. Secret существует в указанном namespace
2. Указанный ключ существует в secret
3. У оператора есть права на чтение secrets

#### Проблемы с TLS сертификатами

Если возникают ошибки при работе с TLS сертификатами:

1. **Ошибка "file name too long"**: Убедитесь, что сертификат в секрете хранится правильно:
   - Если используете `stringData`, сертификат должен быть в PEM формате с заголовками `-----BEGIN` и `-----END`
   - Если используете `data` (base64), убедитесь, что значение корректно закодировано
   - Оператор автоматически определяет содержимое сертификата по префиксу `-----BEGIN`

2. **Ошибка "failed to read CA file"**: Проверьте формат сертификата:
   ```bash
   # Проверьте содержимое секрета
   kubectl get secret <secret-name> -n <namespace> -o jsonpath='{.data.ca\.crt}' | base64 -d
   ```
   Убедитесь, что сертификат начинается с `-----BEGIN CERTIFICATE-----`

3. **Ошибка создания временного файла**: Проверьте права оператора на создание файлов в временной директории

## Рекомендации по производительности

### Kafka

- Используйте несколько брокеров для отказоустойчивости
- Настройте правильный размер consumer group для параллельной обработки
- Используйте батчи при записи для повышения throughput

### PostgreSQL

- Увеличьте `batchSize` для приемника (рекомендуется 50-100)
- Используйте индексы на часто запрашиваемых полях
- Настройте `pollInterval` в зависимости от частоты обновления данных

### RabbitMQ

- Используйте persistent queues для надежности
- Настройте prefetch count для оптимизации производительности
- Используйте exchanges для гибкой маршрутизации

### Iceberg

- Используйте батчи для записи (автоматически, размер 10 сообщений)
- Настройте правильный размер партиций для ваших данных
- Используйте compaction для оптимизации чтения

## Устранение неполадок

### Проблемы с подключением

1. Проверьте доступность источника данных из кластера
2. Убедитесь, что credentials корректны
3. Проверьте сетевые политики Kubernetes
4. Для TLS проверьте сертификаты

### Проблемы с производительностью

1. Увеличьте размер батчей для приемников
2. Настройте правильный `pollInterval` для источников
3. Используйте несколько инстансов оператора для масштабирования
4. Мониторьте метрики обработки сообщений

### Логирование

Включите детальное логирование для отладки:

```bash
kubectl logs -l app.kubernetes.io/name=dataflow-operator -f --tail=100
```

Проверьте статус DataFlow ресурса:

```bash
kubectl describe dataflow <name>
```
