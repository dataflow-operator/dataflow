# Интеграционные тесты

Этот пакет содержит интеграционные тесты для всех коннекторов и трансформеров с использованием [testcontainers](https://testcontainers.com/).

## Предварительные требования

- Docker должен быть запущен и доступен
- Go 1.21+
- Достаточно ресурсов для запуска контейнеров (Kafka, PostgreSQL, RabbitMQ)

## Запуск тестов

### Все интеграционные тесты со статистикой

```bash
# С использованием скрипта (рекомендуется)
./scripts/run-tests-with-stats.sh ./test/integration/...

# Или через Makefile
make test-integration

# Или через Taskfile
task test-integration
```

Скрипт выведет:
- Общее количество тестов
- Количество успешных тестов
- Количество проваленных тестов
- Количество пропущенных тестов
- Список проваленных тестов (если есть)

### Все интеграционные тесты (простой вывод)

```bash
go test ./test/integration/... -v

```

### Конкретные тесты

```bash
# Только тесты коннекторов
go test ./test/integration/... -v -run TestKafkaConnectorIntegration
go test ./test/integration/... -v -run TestPostgreSQLConnectorIntegration
go test ./test/integration/... -v -run TestRabbitMQConnectorIntegration

# Только тесты трансформеров
go test ./test/integration/... -v -run TestTransformersIntegration
go test ./test/integration/... -v -run TestTransformersChainIntegration
```

## Что тестируется

### Коннекторы

#### Kafka
- ✅ Source коннектор: чтение сообщений из Kafka топика
- ✅ Sink коннектор: запись сообщений в Kafka топик

#### PostgreSQL
- ✅ Source коннектор: чтение данных из таблицы PostgreSQL
- ✅ Sink коннектор: запись данных в таблицу PostgreSQL

#### RabbitMQ
- ✅ Source коннектор: чтение сообщений из RabbitMQ очереди
- ✅ Sink коннектор: запись сообщений в RabbitMQ exchange

### Трансформеры

- ✅ **Timestamp**: добавление временной метки к сообщениям
- ✅ **Flatten**: разворачивание массивов в отдельные сообщения
- ✅ **Filter**: фильтрация сообщений по условиям
- ✅ **Mask**: маскирование чувствительных данных
- ✅ **Select**: выбор определенных полей
- ✅ **Remove**: удаление определенных полей
- ✅ **Router**: маршрутизация сообщений по условиям
- ✅ **Chain**: цепочка трансформеров (Select -> Mask -> Timestamp)

## Структура тестов

```
test/integration/
├── connectors_test.go    # Тесты для всех коннекторов
├── transformers_test.go   # Тесты для всех трансформеров
└── README.md             # Этот файл
```

## Как работают тесты

1. **testcontainers** автоматически запускает Docker контейнеры для каждого теста
2. Тесты создают необходимые ресурсы (топики, таблицы, очереди)
3. Тесты проверяют чтение/запись данных через коннекторы
4. Контейнеры автоматически останавливаются после завершения теста

## Примеры использования

### Тест Kafka коннектора

```go
func TestKafkaConnectorIntegration(t *testing.T) {
    // Запускается Kafka контейнер
    kafkaContainer, err := kafka.RunContainer(ctx, ...)

    // Создается source коннектор
    sourceConnector := connectors.NewKafkaSourceConnector(sourceSpec)

    // Отправляется тестовое сообщение
    producer.SendMessage(...)

    // Читается через source коннектор
    msgChan, _ := sourceConnector.Read(ctx)
    msg := <-msgChan

    // Проверяется содержимое
    assert.Equal(t, expected, msg.Data)
}
```

### Тест трансформера

```go
func TestTransformersIntegration(t *testing.T) {
    // Создается трансформер
    transformer := transformers.NewTimestampTransformer(config)

    // Создается тестовое сообщение
    message := types.NewMessage(jsonData)

    // Применяется трансформация
    result, _ := transformer.Transform(ctx, message)

    // Проверяется результат
    assert.Contains(t, result[0].Data, "created_at")
}
```

## Отладка

Если тесты падают, проверьте:

1. **Docker запущен**: `docker ps`
2. **Достаточно ресурсов**: контейнеры требуют памяти и CPU
3. **Порты свободны**: тесты используют случайные порты, но могут конфликтовать
4. **Логи контейнеров**: testcontainers логирует запуск контейнеров

## Производительность

Интеграционные тесты медленнее unit тестов, так как:
- Запускаются Docker контейнеры
- Устанавливаются соединения с базами данных
- Выполняются реальные операции чтения/записи

Ожидаемое время выполнения:
- Kafka тесты: ~30-60 секунд
- PostgreSQL тесты: ~20-40 секунд
- RabbitMQ тесты: ~30-60 секунд
- Трансформеры: ~5-10 секунд

## CI/CD

Для запуска в CI/CD убедитесь, что:
- Docker доступен в CI окружении
- Достаточно ресурсов для контейнеров
- Тесты могут запускаться параллельно (используйте `-parallel` флаг с осторожностью)

## Известные ограничения

- Iceberg коннектор не тестируется (требует REST Catalog сервер)
- Некоторые тесты могут быть нестабильными из-за таймаутов
- Требуется Docker для всех тестов

