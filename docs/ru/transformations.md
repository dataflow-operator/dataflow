# Transformations

DataFlow Operator поддерживает различные трансформации сообщений, которые применяются последовательно к каждому сообщению в порядке, указанном в конфигурации. Все трансформации поддерживают JSONPath для работы с вложенными структурами данных.

## Обзор трансформаций

| Трансформация | Описание | Вход | Выход |
|---------------|----------|------|-------|
| Timestamp | Добавляет временную метку | 1 сообщение | 1 сообщение |
| Flatten | Развертывает массивы | 1 сообщение | N сообщений |
| Filter | Фильтрует по условию | 1 сообщение | 0 или 1 сообщение |
| Mask | Маскирует данные | 1 сообщение | 1 сообщение |
| Router | Маршрутизирует в разные приемники | 1 сообщение | 0 или 1 сообщение |
| Select | Выбирает поля | 1 сообщение | 1 сообщение |
| Remove | Удаляет поля | 1 сообщение | 1 сообщение |
| SnakeCase | Преобразует ключи в snake_case | 1 сообщение | 1 сообщение |
| CamelCase | Преобразует ключи в CamelCase | 1 сообщение | 1 сообщение |
| ReplaceField | Переименовывает поля | 1 сообщение | 1 сообщение |
| HeaderFrom | Извлекает данные из заголовков Kafka | 1 сообщение | 1 сообщение |

## Timestamp

Добавляет поле с временной меткой к каждому сообщению. Полезно для отслеживания времени обработки сообщений.

### Конфигурация

```yaml
transformations:
  - type: timestamp
    timestamp:
      # Имя поля для временной метки (опционально, по умолчанию: created_at)
      fieldName: created_at

      # Формат временной метки (опционально, по умолчанию: RFC3339)
      # Поддерживаются все форматы Go time package
      format: RFC3339
```

### Поддерживаемые форматы

- `RFC3339` - `2006-01-02T15:04:05Z07:00` (по умолчанию)
- `RFC3339Nano` - `2006-01-02T15:04:05.999999999Z07:00`
- `Unix` - Unix timestamp в секундах
- `UnixMilli` - Unix timestamp в миллисекундах
- Любой кастомный формат Go time package

### Примеры

#### Базовое использование

```yaml
transformations:
  - type: timestamp
    timestamp:
      fieldName: processed_at
```

**Входное сообщение:**
```json
{
  "id": 1,
  "name": "Test"
}
```

**Выходное сообщение:**
```json
{
  "id": 1,
  "name": "Test",
  "processed_at": "2024-01-15T10:30:00Z"
}
```

#### Кастомный формат

```yaml
transformations:
  - type: timestamp
    timestamp:
      fieldName: timestamp
      format: "2006-01-02 15:04:05"
```

**Выходное сообщение:**
```json
{
  "id": 1,
  "timestamp": "2024-01-15 10:30:00"
}
```

#### Unix timestamp

```yaml
transformations:
  - type: timestamp
    timestamp:
      fieldName: unix_time
      format: Unix
```

**Выходное сообщение:**
```json
{
  "id": 1,
  "unix_time": "1705312200"
}
```

## Flatten

Развертывает массив в отдельные сообщения, сохраняя все родительские поля. Полезно для обработки вложенных структур данных.

### Конфигурация

```yaml
transformations:
  - type: flatten
    flatten:
      # JSONPath к массиву для развертывания (обязательно)
      field: rowsStock
```

### Примеры

#### Простое развертывание

```yaml
transformations:
  - type: flatten
    flatten:
      field: items
```

**Входное сообщение:**
```json
{
  "order_id": 12345,
  "customer": "John Doe",
  "items": [
    {"product": "Apple", "quantity": 5},
    {"product": "Banana", "quantity": 3}
  ]
}
```

**Выходные сообщения:**
```json
{
  "order_id": 12345,
  "customer": "John Doe",
  "product": "Apple",
  "quantity": 5
}
```

```json
{
  "order_id": 12345,
  "customer": "John Doe",
  "product": "Banana",
  "quantity": 3
}
```

#### Вложенные массивы

```yaml
transformations:
  - type: flatten
    flatten:
      field: orders.items
```

**Входное сообщение:**
```json
{
  "customer_id": 100,
  "orders": {
    "items": [
      {"sku": "SKU001", "price": 10.99},
      {"sku": "SKU002", "price": 5.99}
    ]
  }
}
```

**Выходные сообщения:**
```json
{
  "customer_id": 100,
  "orders": {},
  "sku": "SKU001",
  "price": 10.99
}
```

#### Комбинация с другими трансформациями

```yaml
transformations:
  - type: flatten
    flatten:
      field: rowsStock
  - type: timestamp
    timestamp:
      fieldName: created_at
```

Это создаст отдельное сообщение для каждого элемента массива, каждое с добавленной временной меткой.

## Filter

Фильтрует сообщения на основе JSONPath условий. Сообщения, не соответствующие условию, удаляются из потока.

### Конфигурация

```yaml
transformations:
  - type: filter
    filter:
      # JSONPath выражение, которое должно быть истинным (обязательно)
      condition: "$.type == 'stock'"
```

### JSONPath синтаксис

Filter использует библиотеку `gjson` для оценки условий. Поддерживаются следующие операции:

- `$.field` - доступ к полю
- `$.nested.field` - доступ к вложенным полям
- `$.array[0]` - доступ к элементам массива
- `$.array[*]` - все элементы массива

### Примеры

#### Простая фильтрация

```yaml
transformations:
  - type: filter
    filter:
      condition: "$.active"
```

**Входные сообщения:**
```json
{"id": 1, "active": true}   // ✅ Проходит
{"id": 2, "active": false}  // ❌ Отфильтровывается
{"id": 3}                   // ❌ Отфильтровывается (нет поля)
```

#### Фильтрация по значению

```yaml
transformations:
  - type: filter
    filter:
      condition: "$.level"
```

**Входные сообщения:**
```json
{"level": "error"}    // ✅ Проходит (непустая строка)
{"level": "warning"}  // ✅ Проходит
{"level": ""}        // ❌ Отфильтровывается
{"level": null}      // ❌ Отфильтровывается
```

#### Фильтрация по числовому значению

```yaml
transformations:
  - type: filter
    filter:
      condition: "$.amount"
```

**Входные сообщения:**
```json
{"amount": 100}  // ✅ Проходит (не ноль)
{"amount": 0}    // ❌ Отфильтровывается
{"amount": -5}   // ✅ Проходит (не ноль)
```

#### Комплексная фильтрация

```yaml
transformations:
  - type: filter
    filter:
      condition: "$.user.status"
```

**Входное сообщение:**
```json
{
  "user": {
    "status": "active"
  }
}
```

**Результат:** Сообщение проходит, если `user.status` существует и не пусто.

### Ограничения

Текущая реализация Filter проверяет существование поля и его "truthiness". Для сложных логических выражений (AND, OR, сравнения) рассмотрите использование Router трансформации.

## Mask

Маскирует чувствительные данные в указанных полях. Поддерживает сохранение длины или полное замещение символами.

### Конфигурация

```yaml
transformations:
  - type: mask
    mask:
      # Список JSONPath выражений к полям для маскирования (обязательно)
      fields:
        - password
        - email
        - creditCard

      # Символ для маскирования (опционально, по умолчанию: *)
      maskChar: "*"

      # Сохранять оригинальную длину (опционально, по умолчанию: false)
      keepLength: true
```

### Примеры

#### Маскирование с сохранением длины

```yaml
transformations:
  - type: mask
    mask:
      fields:
        - password
        - email
      keepLength: true
```

**Входное сообщение:**
```json
{
  "id": 1,
  "username": "john",
  "password": "secret123",
  "email": "john@example.com"
}
```

**Выходное сообщение:**
```json
{
  "id": 1,
  "username": "john",
  "password": "*********",
  "email": "****************"
}
```

#### Маскирование с фиксированной длиной

```yaml
transformations:
  - type: mask
    mask:
      fields:
        - password
      keepLength: false
      maskChar: "X"
```

**Входное сообщение:**
```json
{
  "password": "verylongpassword123"
}
```

**Выходное сообщение:**
```json
{
  "password": "XXX"
}
```

#### Маскирование вложенных полей

```yaml
transformations:
  - type: mask
    mask:
      fields:
        - user.password
        - payment.cardNumber
      keepLength: true
```

**Входное сообщение:**
```json
{
  "user": {
    "password": "secret"
  },
  "payment": {
    "cardNumber": "1234567890123456"
  }
}
```

**Выходное сообщение:**
```json
{
  "user": {
    "password": "******"
  },
  "payment": {
    "cardNumber": "****************"
  }
}
```

## Router

Маршрутизирует сообщения в разные приемники на основе условий. Сообщения, соответствующие условию, отправляются в указанный приемник вместо основного.

### Конфигурация

```yaml
transformations:
  - type: router
    router:
      routes:
        # Первое совпавшее условие определяет приемник
        - condition: "$.level"
          sink:
            type: kafka
            kafka:
              brokers: ["localhost:9092"]
              topic: error-topic
        - condition: "$.priority"
          sink:
            type: postgresql
            postgresql:
              connectionString: "..."
              table: high_priority
```

### Особенности

- Условия проверяются в порядке указания
- Первое совпавшее условие определяет приемник
- Если ни одно условие не совпало, сообщение отправляется в основной приемник
- Router использует ту же логику фильтрации, что и Filter трансформация

### Примеры

#### Маршрутизация по уровню логирования

```yaml
transformations:
  - type: router
    router:
      routes:
        - condition: "$.level"
          sink:
            type: kafka
            kafka:
              brokers: ["localhost:9092"]
              topic: error-logs
```

**Входные сообщения:**
```json
{"level": "error", "message": "Critical error"}     // → error-logs топик
{"level": "info", "message": "Info message"}       // → основной приемник
{"level": "warning", "message": "Warning"}          // → основной приемник
```

#### Множественная маршрутизация

```yaml
transformations:
  - type: router
    router:
      routes:
        - condition: "$.type"
          sink:
            type: kafka
            kafka:
              brokers: ["localhost:9092"]
              topic: events-topic
        - condition: "$.priority"
          sink:
            type: postgresql
            postgresql:
              connectionString: "postgres://..."
              table: high_priority_events
```

**Входные сообщения:**
```json
{"type": "event", "data": "..."}           // → events-topic
{"priority": "high", "data": "..."}       // → high_priority_events таблица
{"data": "..."}                           // → основной приемник
```

#### Комбинация с другими трансформациями

```yaml
transformations:
  - type: timestamp
    timestamp:
      fieldName: processed_at
  - type: router
    router:
      routes:
        - condition: "$.level"
          sink:
            type: kafka
            kafka:
              brokers: ["localhost:9092"]
              topic: errors
```

Сначала добавляется временная метка, затем сообщение маршрутизируется.

## Select

Выбирает только указанные поля из сообщения. Полезно для уменьшения размера данных и повышения производительности.

### Конфигурация

```yaml
transformations:
  - type: select
    select:
      # Список JSONPath выражений к полям для выбора (обязательно)
      fields:
        - id
        - name
        - email
```

### Примеры

#### Простой выбор полей

```yaml
transformations:
  - type: select
    select:
      fields:
        - id
        - name
        - email
```

**Входное сообщение:**
```json
{
  "id": 1,
  "name": "John Doe",
  "email": "john@example.com",
  "password": "secret",
  "internal_id": 999
}
```

**Выходное сообщение:**
```json
{
  "id": 1,
  "name": "John Doe",
  "email": "john@example.com"
}
```

#### Выбор вложенных полей

```yaml
transformations:
  - type: select
    select:
      fields:
        - user.id
        - user.name
        - metadata.timestamp
```

**Входное сообщение:**
```json
{
  "user": {
    "id": 1,
    "name": "John",
    "email": "john@example.com"
  },
  "metadata": {
    "timestamp": "2024-01-15T10:30:00Z",
    "source": "api"
  }
}
```

**Выходное сообщение:**
```json
{
  "user": {
    "id": 1,
    "name": "John"
  },
  "metadata": {
    "timestamp": "2024-01-15T10:30:00Z"
  }
}
```

## Remove

Удаляет указанные поля из сообщения. Полезно для очистки данных перед отправкой.

### Конфигурация

```yaml
transformations:
  - type: remove
    remove:
      # Список JSONPath выражений к полям для удаления (обязательно)
      fields:
        - password
        - internal_id
        - secret_token
```

### Примеры

#### Удаление чувствительных полей

```yaml
transformations:
  - type: remove
    remove:
      fields:
        - password
        - creditCard
        - ssn
```

**Входное сообщение:**
```json
{
  "id": 1,
  "name": "John Doe",
  "password": "secret",
  "creditCard": "1234-5678-9012-3456",
  "ssn": "123-45-6789"
}
```

**Выходное сообщение:**
```json
{
  "id": 1,
  "name": "John Doe"
}
```

#### Удаление вложенных полей

```yaml
transformations:
  - type: remove
    remove:
      fields:
        - user.password
        - metadata.internal
```

**Входное сообщение:**
```json
{
  "user": {
    "id": 1,
    "name": "John",
    "password": "secret"
  },
  "metadata": {
    "timestamp": "2024-01-15",
    "internal": "secret"
  }
}
```

**Выходное сообщение:**
```json
{
  "user": {
    "id": 1,
    "name": "John"
  },
  "metadata": {
    "timestamp": "2024-01-15"
  }
}
```

## Порядок применения

Трансформации применяются последовательно в порядке, указанном в списке `transformations`. Каждая трансформация получает результат предыдущей.

### Пример последовательности

```yaml
transformations:
  # 1. Развернуть массив
  - type: flatten
    flatten:
      field: items

  # 2. Добавить временную метку
  - type: timestamp
    timestamp:
      fieldName: created_at

  # 3. Отфильтровать неактивные
  - type: filter
    filter:
      condition: "$.active"

  # 4. Удалить внутренние поля
  - type: remove
    remove:
      fields:
        - internal_id
        - debug_info

  # 5. Выбрать только нужные поля
  - type: select
    select:
      fields:
        - id
        - name
        - created_at
```

### Рекомендации по порядку

1. **Flatten** должен быть первым, если нужно развернуть массивы
2. **Filter** применяйте рано, чтобы уменьшить объем обрабатываемых данных
3. **SnakeCase/CamelCase** применяйте после Select/Remove, но перед отправкой в приемник
4. **Mask/Remove** применяйте перед Select для безопасности
5. **Select** применяйте в конце для финальной очистки
6. **Timestamp** можно применять в любом месте, но обычно в начале или конце
7. **Router** обычно применяется в конце, после всех других трансформаций

## Комбинированные примеры

### Обработка заказов

```yaml
transformations:
  # Развернуть товары в отдельные сообщения
  - type: flatten
    flatten:
      field: items

  # Добавить временную метку
  - type: timestamp
    timestamp:
      fieldName: processed_at

  # Фильтровать только оплаченные заказы
  - type: filter
    filter:
      condition: "$.status"

  # Удалить чувствительные данные
  - type: remove
    remove:
      fields:
        - customer.creditCard
        - customer.cvv
```

### Обработка логов

```yaml
transformations:
  # Добавить временную метку
  - type: timestamp
    timestamp:
      fieldName: timestamp

  # Маскировать IP адреса
  - type: mask
    mask:
      fields:
        - ip_address
      keepLength: true

  # Маршрутизировать ошибки
  - type: router
    router:
      routes:
        - condition: "$.level"
          sink:
            type: kafka
            kafka:
              brokers: ["localhost:9092"]
              topic: error-logs
```

### Нормализация имен полей

```yaml
transformations:
  # Выбрать нужные поля
  - type: select
    select:
      fields:
        - firstName
        - lastName
        - email
        - address

  # Преобразовать в snake_case для PostgreSQL
  - type: snakeCase
    snakeCase:
      deep: true
```

**Входное сообщение:**
```json
{
  "firstName": "John",
  "lastName": "Doe",
  "email": "john@example.com",
  "address": {
    "streetName": "Main St",
    "zipCode": "12345"
  }
}
```

**Выходное сообщение:**
```json
{
  "first_name": "John",
  "last_name": "Doe",
  "email": "john@example.com",
  "address": {
    "street_name": "Main St",
    "zip_code": "12345"
  }
}
```

## JSONPath поддержка

Все трансформации, работающие с полями, поддерживают JSONPath синтаксис:

- `$.field` - корневое поле
- `$.nested.field` - вложенное поле
- `$.array[0]` - элемент массива по индексу
- `$.array[*]` - все элементы массива
- `$.*` - все поля корневого уровня

## Производительность

- **Filter** - применяйте рано для уменьшения объема данных
- **Select** - уменьшает размер сообщений и повышает производительность
- **Flatten** - может увеличить количество сообщений, используйте осторожно
- **Router** - создает дополнительные подключения, минимизируйте количество маршрутов

## SnakeCase

Преобразует все ключи JSON-объекта в формат `snake_case`. Полезно для нормализации имен полей при интеграции с системами, использующими snake_case (например, PostgreSQL, Python API).

### Конфигурация

```yaml
transformations:
  - type: snakeCase
    snakeCase:
      # Рекурсивно преобразовывать вложенные объекты (опционально, по умолчанию: false)
      deep: true
```

### Примеры

#### Простое преобразование

```yaml
transformations:
  - type: snakeCase
    snakeCase:
      deep: false
```

**Входное сообщение:**
```json
{
  "firstName": "John",
  "lastName": "Doe",
  "userName": "johndoe",
  "isActive": true,
  "itemCount": 42
}
```

**Выходное сообщение:**
```json
{
  "first_name": "John",
  "last_name": "Doe",
  "user_name": "johndoe",
  "is_active": true,
  "item_count": 42
}
```

#### Рекурсивное преобразование

```yaml
transformations:
  - type: snakeCase
    snakeCase:
      deep: true
```

**Входное сообщение:**
```json
{
  "firstName": "John",
  "address": {
    "streetName": "Main St",
    "houseNumber": 123,
    "zipCode": "12345"
  },
  "items": [
    {
      "itemName": "Product",
      "itemPrice": 99.99
    }
  ]
}
```

**Выходное сообщение:**
```json
{
  "first_name": "John",
  "address": {
    "street_name": "Main St",
    "house_number": 123,
    "zip_code": "12345"
  },
  "items": [
    {
      "item_name": "Product",
      "item_price": 99.99
    }
  ]
}
```

#### Преобразование PascalCase

```yaml
transformations:
  - type: snakeCase
    snakeCase:
      deep: false
```

**Входное сообщение:**
```json
{
  "FirstName": "John",
  "LastName": "Doe",
  "UserID": 123
}
```

**Выходное сообщение:**
```json
{
  "first_name": "John",
  "last_name": "Doe",
  "user_id": 123
}
```

### Особенности

- Преобразует `camelCase` → `snake_case`
- Преобразует `PascalCase` → `snake_case`
- Обрабатывает последовательные заглавные буквы (например, `XMLHttpRequest` → `xml_http_request`)
- Сохраняет уже существующие ключи в `snake_case` без изменений
- При `deep: false` преобразует только ключи верхнего уровня
- При `deep: true` рекурсивно преобразует все вложенные объекты и массивы

## CamelCase

Преобразует все ключи JSON-объекта в формат `CamelCase` (PascalCase). Полезно для нормализации имен полей при интеграции с системами, использующими CamelCase (например, Java, C# API).

### Конфигурация

```yaml
transformations:
  - type: camelCase
    camelCase:
      # Рекурсивно преобразовывать вложенные объекты (опционально, по умолчанию: false)
      deep: true
```

### Примеры

#### Простое преобразование

```yaml
transformations:
  - type: camelCase
    camelCase:
      deep: false
```

**Входное сообщение:**
```json
{
  "first_name": "John",
  "last_name": "Doe",
  "user_name": "johndoe",
  "is_active": true,
  "item_count": 42
}
```

**Выходное сообщение:**
```json
{
  "FirstName": "John",
  "LastName": "Doe",
  "UserName": "johndoe",
  "IsActive": true,
  "ItemCount": 42
}
```

#### Рекурсивное преобразование

```yaml
transformations:
  - type: camelCase
    camelCase:
      deep: true
```

**Входное сообщение:**
```json
{
  "first_name": "John",
  "address": {
    "street_name": "Main St",
    "house_number": 123,
    "zip_code": "12345"
  },
  "items": [
    {
      "item_name": "Product",
      "item_price": 99.99
    }
  ]
}
```

**Выходное сообщение:**
```json
{
  "FirstName": "John",
  "Address": {
    "StreetName": "Main St",
    "HouseNumber": 123,
    "ZipCode": "12345"
  },
  "Items": [
    {
      "ItemName": "Product",
      "ItemPrice": 99.99
    }
  ]
}
```

#### Преобразование одиночных слов

```yaml
transformations:
  - type: camelCase
    camelCase:
      deep: false
```

**Входное сообщение:**
```json
{
  "name": "John",
  "id": 123
}
```

**Выходное сообщение:**
```json
{
  "Name": "John",
  "Id": 123
}
```

### Особенности

- Преобразует `snake_case` → `CamelCase`
- Все слова начинаются с заглавной буквы (PascalCase)
- Сохраняет уже существующие ключи в `CamelCase` без изменений
- При `deep: false` преобразует только ключи верхнего уровня
- При `deep: true` рекурсивно преобразует все вложенные объекты и массивы

## ReplaceField

Переименовывает поля в сообщениях. Полезно для нормализации структуры данных и изменения путей к полям.

### Конфигурация

```yaml
transformations:
  - type: replaceField
    replaceField:
      # Список правил переименования в формате "старый.путь:новый.путь" (обязательно)
      renames:
        - key.sku:sku
        - body.lc:lc
```

### Формат правил переименования

Правила переименования имеют формат `старый.путь:новый.путь`:
- Левая часть (до `:`) - JSONPath к существующему полю
- Правая часть (после `:`) - JSONPath к новому полю
- Поддерживается JSONPath синтаксис (можно использовать `$.` префикс)

### Примеры

#### Простое переименование

```yaml
transformations:
  - type: replaceField
    replaceField:
      renames:
        - oldName:newName
```

**Входное сообщение:**
```json
{
  "oldName": "value",
  "other": "otherValue"
}
```

**Выходное сообщение:**
```json
{
  "newName": "value",
  "other": "otherValue"
}
```

#### Переименование вложенных полей

```yaml
transformations:
  - type: replaceField
    replaceField:
      renames:
        - key.sku:sku
        - body.lc:lc
```

**Входное сообщение:**
```json
{
  "key": {
    "sku": "12345"
  },
  "body": {
    "lc": "en"
  }
}
```

**Выходное сообщение:**
```json
{
  "sku": "12345",
  "lc": "en"
}
```

#### Переименование в вложенный путь

```yaml
transformations:
  - type: replaceField
    replaceField:
      renames:
        - oldName:metadata.newName
```

**Входное сообщение:**
```json
{
  "oldName": "value"
}
```

**Выходное сообщение:**
```json
{
  "metadata": {
    "newName": "value"
  }
}
```

#### Множественное переименование

```yaml
transformations:
  - type: replaceField
    replaceField:
      renames:
        - key.sku:sku
        - body.lc:language
        - metadata.timestamp:ts
```

**Входное сообщение:**
```json
{
  "key": {
    "sku": "12345"
  },
  "body": {
    "lc": "en"
  },
  "metadata": {
    "timestamp": "2024-01-15T10:30:00Z"
  }
}
```

**Выходное сообщение:**
```json
{
  "sku": "12345",
  "language": "en",
  "ts": "2024-01-15T10:30:00Z"
}
```

### Особенности

- Если старое поле не существует, переименование пропускается
- Если новое поле уже существует, оно будет перезаписано
- Поддерживается JSONPath синтаксис для работы с вложенными структурами
- Можно переименовывать поля в корневой уровень или в вложенные структуры

## HeaderFrom

Извлекает данные из заголовков Kafka сообщений и добавляет их в тело сообщения. Полезно для обогащения сообщений метаданными из заголовков.

### Конфигурация

```yaml
transformations:
  - type: headerFrom
    headerFrom:
      # Список маппингов в формате "имяЗаголовка:путь.к.полю" (обязательно)
      mappings:
        - X-Request-Id:requestId
        - X-Language:metadata.language
```

### Формат маппингов

Маппинги имеют формат `имяЗаголовка:путь.к.полю`:
- Левая часть (до `:`) - имя заголовка Kafka сообщения
- Правая часть (после `:`) - JSONPath к полю в теле сообщения, куда будет записано значение заголовка
- Поддерживается JSONPath синтаксис (можно использовать `$.` префикс)

### Примеры

#### Извлечение одного заголовка

```yaml
transformations:
  - type: headerFrom
    headerFrom:
      mappings:
        - X-Request-Id:requestId
```

**Входное сообщение (с заголовком `X-Request-Id: req-123`):**
```json
{
  "data": "value"
}
```

**Выходное сообщение:**
```json
{
  "data": "value",
  "requestId": "req-123"
}
```

#### Извлечение в вложенное поле

```yaml
transformations:
  - type: headerFrom
    headerFrom:
      mappings:
        - X-Language:metadata.language
        - X-User-Id:metadata.userId
```

**Входное сообщение (с заголовками `X-Language: en`, `X-User-Id: user-456`):**
```json
{
  "data": "value"
}
```

**Выходное сообщение:**
```json
{
  "data": "value",
  "metadata": {
    "language": "en",
    "userId": "user-456"
  }
}
```

#### Извлечение нескольких заголовков

```yaml
transformations:
  - type: headerFrom
    headerFrom:
      mappings:
        - X-Request-Id:requestId
        - X-User-Id:userId
        - X-Source:source
```

**Входное сообщение (с заголовками):**
```json
{
  "data": "value"
}
```

**Выходное сообщение:**
```json
{
  "data": "value",
  "requestId": "req-123",
  "userId": "user-456",
  "source": "api"
}
```

### Особенности

- Если заголовок не существует, маппинг пропускается
- Заголовки доступны только для сообщений из Kafka источников
- Если заголовки отсутствуют в метаданных сообщения, трансформация возвращает исходное сообщение без изменений
- Значения заголовков всегда добавляются как строки

## Ограничения

- JSONPath выражения оцениваются последовательно
- Сложные логические выражения (AND, OR) не поддерживаются напрямую в Filter
- Router проверяет условия последовательно, первое совпадение определяет маршрут
- Flatten работает только с массивами, не с объектами
- SnakeCase и CamelCase работают только с валидным JSON, бинарные данные возвращаются без изменений
