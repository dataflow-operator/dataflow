# Connectors

DataFlow Operator поддерживает различные коннекторы для источников и приемников данных. Каждый коннектор реализует стандартный интерфейс и может использоваться как источник (source) или приемник (sink) данных.

## Обзор коннекторов

| Коннектор | Источник | Приемник | Особенности |
|-----------|----------|----------|-------------|
| Kafka | ✅ | ✅ | Consumer groups, TLS, SASL, Avro, Schema Registry |
| PostgreSQL | ✅ | ✅ | SQL запросы, батч-вставки, автосоздание таблиц, UPSERT режим |
| Trino | ✅ | ✅ | SQL запросы, аутентификация Keycloak OAuth2, батч-вставки |

## Kafka

Kafka коннектор поддерживает чтение и запись сообщений из/в топики Apache Kafka. Поддерживает consumer groups для масштабирования, TLS и SASL аутентификацию, а также работу с Avro форматом через Confluent Schema Registry или статическую схему.

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

    # Формат сообщений (опционально, по умолчанию: "json")
    # Поддерживаемые форматы: "json", "avro"
    format: json

    # Конфигурация Avro (требуется если format: "avro")
    # Вариант 1: Использование Confluent Schema Registry (рекомендуется)
    schemaRegistry:
      # URL Schema Registry (обязательно)
      url: https://schema-registry:8081
      # Basic Auth для Schema Registry (опционально)
      basicAuth:
        username: schema-user
        password: schema-password
      # TLS конфигурация для Schema Registry (опционально)
      tls:
        insecureSkipVerify: false
        caFile: /path/to/schema-registry-ca.crt

    # Вариант 2: Статическая Avro схема (альтернатива Schema Registry)
    # avroSchema: |
    #   {
    #     "type": "record",
    #     "name": "MyRecord",
    #     "fields": [
    #       {"name": "id", "type": "long"},
    #       {"name": "name", "type": "string"}
    #     ]
    #   }
    # Или путь к файлу со схемой:
    # avroSchemaFile: /path/to/schema.avsc
```

#### Особенности Kafka источника

- **Consumer Groups**: Используйте разные consumer groups для масштабирования обработки
- **Начальная позиция**: По умолчанию читает с самого старого сообщения (`OffsetOldest`)
- **Форматы данных**: Поддерживает JSON (по умолчанию) и Avro форматы
- **Avro поддержка**:
  - **Confluent Schema Registry**: Автоматическое получение схем по ID из сообщений (формат: magic byte + schema ID + data)
  - **Статическая схема**: Использование предопределенной схемы из конфигурации или файла
  - **Кэширование схем**: Схемы из Schema Registry кэшируются для повышения производительности
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

#### Пример с Avro и Schema Registry

```yaml
source:
  type: kafka
  kafka:
    brokers:
      - kafka:9092
    topic: avro-topic
    consumerGroup: avro-group
    format: avro
    schemaRegistry:
      url: https://schema-registry:8081
      basicAuth:
        username: schema-user
        password: schema-password
      tls:
        insecureSkipVerify: true  # Для self-signed сертификатов
        # caFile: /path/to/ca.crt  # Или укажите CA сертификат
```

#### Пример с Avro и статической схемой

```yaml
source:
  type: kafka
  kafka:
    brokers:
      - kafka:9092
    topic: avro-topic
    consumerGroup: avro-group
    format: avro
    avroSchema: |
      {
        "type": "record",
        "name": "Stock",
        "namespace": "com.example",
        "fields": [
          {"name": "id", "type": "long"},
          {"name": "symbol", "type": "string"},
          {"name": "price", "type": "double"},
          {"name": "quantity", "type": "int"}
        ]
      }
    # Или используйте файл:
    # avroSchemaFile: /path/to/schema.avsc
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

    # Режим UPSERT (опционально, по умолчанию: false)
    # Если true, существующие записи будут обновляться вместо пропуска
    # Используется ON CONFLICT DO UPDATE для обновления записей
    upsertMode: true

    # Ключ конфликта для UPSERT (опционально, по умолчанию: PRIMARY KEY)
    # Указывает колонку(ы) для определения конфликта при UPSERT
    # Если не указан, используется PRIMARY KEY таблицы
    conflictKey: "id"
```

#### Особенности PostgreSQL приемника

- **Батч-вставки**: Группирует сообщения для эффективной записи
- **Автосоздание таблиц**: Автоматически создает таблицы с JSONB полем
- **Гибкая схема**: Поддерживает как JSONB (для автосозданных таблиц), так и колоночный формат
- **Индексы**: Автоматически создает GIN индекс на JSONB поле для быстрого поиска
- **UPSERT режим**: Поддерживает обновление существующих записей при конфликте по PRIMARY KEY или указанному ключу

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

#### Пример с UPSERT для обновления существующих записей

```yaml
sink:
  type: postgresql
  postgresql:
    connectionString: "postgres://user:password@localhost:5432/analytics"
    table: orders
    upsertMode: true  # Включает обновление существующих записей
    conflictKey: "id"  # Использует поле 'id' для определения конфликта
    batchSize: 100
```

При включенном `upsertMode`:
- Если запись с таким же `id` (или указанным `conflictKey`) уже существует, она будет обновлена
- Если записи нет, она будет вставлена
- Это особенно полезно для синхронизации данных, когда источник периодически отправляет обновленные записи

**Важно:** Для работы UPSERT таблица должна иметь PRIMARY KEY или UNIQUE constraint на указанном `conflictKey`.

## Trino

Trino коннектор поддерживает чтение из таблиц и запись в таблицы Trino (ранее PrestoSQL). Поддерживает SQL запросы, аутентификацию через Keycloak OAuth2/OIDC и батч-вставки.

### Источник (Source)

Конфигурация Trino источника:

```yaml
source:
  type: trino
  trino:
    # URL сервера Trino (обязательно)
    serverURL: "http://trino:8080"

    # Каталог для использования (обязательно)
    catalog: hive

    # Схема для использования (обязательно)
    schema: default

    # Таблица для чтения (обязательно, если не указан query)
    table: source_table

    # Кастомный SQL запрос (опционально)
    # Если указан, используется вместо чтения всей таблицы
    query: "SELECT * FROM hive.default.source_table WHERE id > 100"

    # Интервал опроса в секундах (опционально, по умолчанию: 5)
    # Используется для периодического чтения новых данных
    pollInterval: 60

    # Аутентификация Keycloak (опционально)
    keycloak:
      # Вариант 1: Использование долгоживущего токена напрямую (рекомендуется для долгоживущих токенов)
      token: "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..."

      # Вариант 2: Использование OAuth2 flow (альтернатива прямому токену)
      # serverURL: "https://keycloak.example.com"
      # realm: myrealm
      # clientID: trino-client
      # clientSecret: client-secret
      # username: trino-user
      # password: trino-password
```

#### Особенности Trino источника

- **SQL запросы**: Поддержка кастомных SQL запросов с WHERE, JOIN и т.д.
- **Периодический опрос**: Регулярно опрашивает таблицы на наличие новых данных
- **Аутентификация Keycloak**: OAuth2/OIDC аутентификация через Keycloak
  - **Прямой токен**: Использование долгоживущего токена, полученного из Keycloak (рекомендуется для долгоживущих токенов)
  - **Password Grant**: Использование username/password для аутентификации
  - **Client Credentials**: Использование client ID/secret для сервис-к-сервис аутентификации
- **Автоматическое обновление токенов**: Токены автоматически обновляются до истечения срока действия (только для OAuth2 flow, не для прямых токенов)

#### Получение токена из Keycloak

Для использования долгоживущего токена вы можете получить его из Keycloak следующими способами:

**Способ 1: Использование curl (Password Grant)**
```bash
curl -X POST "https://keycloak.example.com/realms/myrealm/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "client_id=trino-client" \
  -d "client_secret=client-secret" \
  -d "username=trino-user" \
  -d "password=trino-password"
```

**Способ 2: Использование curl (Client Credentials)**
```bash
curl -X POST "https://keycloak.example.com/realms/myrealm/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=trino-client" \
  -d "client_secret=client-secret"
```

Ответ будет содержать поле `access_token`. Используйте это значение токена в поле `token` конфигурации Keycloak.

**Примечание**: Для долгоживущих токенов настройте время жизни токена в настройках realm или клиента Keycloak.
- **Метаданные**: Каждое сообщение содержит метаданные:
  - `catalog` - название каталога
  - `schema` - название схемы
  - `table` - название таблицы

#### Пример с кастомным запросом

```yaml
source:
  type: trino
  trino:
    serverURL: "http://trino:8080"
    catalog: hive
    schema: analytics
    query: |
      SELECT
        u.id,
        u.email,
        o.order_id,
        o.total
      FROM hive.analytics.users u
      JOIN hive.analytics.orders o ON u.id = o.user_id
      WHERE o.created_at > CURRENT_TIMESTAMP - INTERVAL '1' DAY
    pollInterval: 300  # Опрос каждые 5 минут
    keycloak:
      serverURL: "https://keycloak.example.com"
      realm: myrealm
      clientID: trino-client
      clientSecret: client-secret
      username: trino-user
      password: trino-password
```

### Приемник (Sink)

Конфигурация Trino приемника:

```yaml
sink:
  type: trino
  trino:
    # URL сервера Trino (обязательно)
    serverURL: "http://trino:8080"

    # Каталог для использования (обязательно)
    catalog: hive

    # Схема для использования (обязательно)
    schema: default

    # Таблица для записи (обязательно)
    table: target_table

    # Размер батча для вставки (опционально, по умолчанию: 1)
    # Увеличьте для повышения производительности
    batchSize: 100

    # Автоматическое создание таблицы (опционально, по умолчанию: false)
    # Если true, создает таблицу с VARCHAR колонкой для JSON данных
    autoCreateTable: true

    # Аутентификация Keycloak (опционально)
    keycloak:
      serverURL: "https://keycloak.example.com"
      realm: myrealm
      clientID: trino-client
      clientSecret: client-secret
      username: trino-user
      password: trino-password
```

#### Особенности Trino приемника

- **Батч-вставки**: Группирует сообщения для эффективной записи
- **Автосоздание таблиц**: Автоматически создает таблицы, если они не существуют
- **Аутентификация Keycloak**: OAuth2/OIDC аутентификация через Keycloak
- **Автоматическое обновление токенов**: Токены автоматически обновляются

#### Пример: Kafka → Trino с Keycloak

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-to-trino
spec:
  source:
    type: kafka
    kafka:
      brokers:
        - kafka:9092
      topic: input-topic
      consumerGroup: dataflow-group
  sink:
    type: trino
    trino:
      serverURL: "http://trino:8080"
      catalog: hive
      schema: default
      table: output_table
      batchSize: 100
      keycloak:
        # Использование долгоживущего токена, полученного из Keycloak
        token: "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..."

        # Альтернатива: Использование OAuth2 flow
        # serverURL: "https://keycloak.example.com"
        # realm: myrealm
        # clientID: trino-client
        # clientSecret: client-secret
        # username: trino-user
        # password: trino-password
```

#### Пример: Trino с secrets

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: trino-credentials
  namespace: default
type: Opaque
stringData:
  serverURL: "http://trino:8080"
  catalog: "hive"
  schema: "default"
  table: "output_table"
---
apiVersion: v1
kind: Secret
metadata:
  name: keycloak-credentials
  namespace: default
type: Opaque
stringData:
  serverURL: "https://keycloak.example.com"
  realm: "myrealm"
  clientID: "trino-client"
  clientSecret: "client-secret"
  username: "trino-user"
  password: "trino-password"
---
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-to-trino-secrets
spec:
  source:
    type: kafka
    kafka:
      brokers:
        - kafka:9092
      topic: input-topic
      consumerGroup: dataflow-group
  sink:
    type: trino
    trino:
      serverURLSecretRef:
        name: trino-credentials
        key: serverURL
      catalogSecretRef:
        name: trino-credentials
        key: catalog
      schemaSecretRef:
        name: trino-credentials
        key: schema
      tableSecretRef:
        name: trino-credentials
        key: table
      batchSize: 100
      keycloak:
        # Вариант 1: Использование токена из secret (рекомендуется для долгоживущих токенов)
        tokenSecretRef:
          name: keycloak-credentials
          key: token

        # Вариант 2: Использование OAuth2 flow из secrets (альтернатива)
        # serverURLSecretRef:
        #   name: keycloak-credentials
        #   key: serverURL
        # realmSecretRef:
        #   name: keycloak-credentials
        #   key: realm
        # clientIDSecretRef:
        #   name: keycloak-credentials
        #   key: clientID
        # clientSecretSecretRef:
        #   name: keycloak-credentials
        #   key: clientSecret
        # usernameSecretRef:
        #   name: keycloak-credentials
        #   key: username
        # passwordSecretRef:
        #   name: keycloak-credentials
        #   key: password
```

## Обработка ошибок (Error Sink)

DataFlow Operator поддерживает настройку отдельного приемника для сообщений, которые не удалось записать в основной sink. Это позволяет сохранять проблемные сообщения для последующего анализа и обработки.

### Конфигурация Error Sink

Секция `errors` в спецификации DataFlow определяет приемник для сообщений об ошибках:

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: example-with-errors
spec:
  source:
    type: kafka
    kafka:
      brokers:
        - localhost:9092
      topic: input-topic
  sink:
    type: postgresql
    postgresql:
      connectionString: "postgres://user:pass@postgres:5432/db"
      table: output_table
  errors:
    type: kafka
    kafka:
      brokers:
        - localhost:9092
      topic: error-topic
```

### Структура сообщения об ошибке

Когда сообщение не удается записать в основной sink, оно отправляется в error sink со следующей структурой:

```json
{
  "error": {
    "message": "текст ошибки",
    "timestamp": "2026-01-24T12:34:56Z",
    "original_sink": "postgresql",
    "metadata": {
      // Метаданные из оригинального сообщения (если были)
    }
  },
  "original_message": {
    // Оригинальные данные сообщения
    // Если оригинальное сообщение было JSON, оно будет здесь как объект
    // Если нет - будет поле "original_data" со строкой
  }
}
```

### Примеры использования

#### Kafka как Error Sink

```yaml
errors:
  type: kafka
  kafka:
    brokers:
      - localhost:9092
    topic: error-topic
```

#### PostgreSQL как Error Sink

```yaml
errors:
  type: postgresql
  postgresql:
    connectionString: "postgres://user:pass@postgres:5432/db"
    table: error_messages
    autoCreateTable: true
```

#### Trino как Error Sink

```yaml
errors:
  type: trino
  trino:
    serverURL: "http://trino:8080"
    catalog: hive
    schema: default
    table: error_messages
    keycloak:
      token: "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..."
```

### Особенности

- **Любой тип sink**: Error sink может быть любого типа (Kafka, PostgreSQL, Trino)
- **Сохранение данных**: Оригинальные данные сообщения сохраняются в поле `original_message`
- **Метаданные**: Информация об ошибке встраивается в структуру сообщения, что гарантирует её сохранение независимо от типа error sink
- **Обработка ошибок**: Если `errors` не указан, ошибки записи будут приводить к остановке обработки
- **Router sinks**: Error sink также работает для сообщений, отправляемых через router transformation

### Пример полного конфига

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-to-postgres-with-errors
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
  errors:
    type: kafka
    kafka:
      brokers:
        - localhost:9092
      topic: error-topic
```

### Использование Secrets для Error Sink

Error sink также поддерживает использование Kubernetes Secrets:

```yaml
errors:
  type: kafka
  kafka:
    brokersSecretRef:
      name: kafka-credentials
      key: brokers
    topicSecretRef:
      name: kafka-credentials
      key: error-topic
    sasl:
      mechanism: scram-sha-256
      usernameSecretRef:
        name: kafka-credentials
        key: username
      passwordSecretRef:
        name: kafka-credentials
        key: password
```

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
- `schemaRegistry.urlSecretRef` - URL Schema Registry
- `schemaRegistry.basicAuth.usernameSecretRef` - имя пользователя для Schema Registry
- `schemaRegistry.basicAuth.passwordSecretRef` - пароль для Schema Registry
- `avroSchemaSecretRef` - Avro схема из секрета (для статической схемы)

#### PostgreSQL
- `connectionStringSecretRef` - строка подключения
- `tableSecretRef` - название таблицы

#### Trino
- `serverURLSecretRef` - URL сервера Trino
- `catalogSecretRef` - название каталога
- `schemaSecretRef` - название схемы
- `tableSecretRef` - название таблицы
- `keycloak.serverURLSecretRef` - URL сервера Keycloak
- `keycloak.realmSecretRef` - название realm в Keycloak
- `keycloak.clientIDSecretRef` - OAuth2 client ID
- `keycloak.clientSecretSecretRef` - OAuth2 client secret
- `keycloak.usernameSecretRef` - имя пользователя для password grant
- `keycloak.passwordSecretRef` - пароль для password grant
- `keycloak.tokenSecretRef` - OAuth2 токен (для долгоживущих токенов)

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
