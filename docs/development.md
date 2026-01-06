# Development

Руководство для разработчиков, желающих внести вклад в DataFlow Operator или настроить локальную среду разработки.

## Предварительные требования

- Go 1.21 или выше
- Docker и Docker Compose
- kubectl настроен для работы с кластером
- Helm 3.0+ (для тестирования установки)
- Make (опционально, для использования Makefile)

## Настройка окружения

### Клонирование репозитория

```bash
git clone <repository-url>
cd dataflow
```

### Установка зависимостей

```bash
go mod download
go mod tidy
```

### Установка инструментов разработки

```bash
# Установка controller-gen
make controller-gen

# Установка envtest
make envtest
```

## Локальная разработка

### Запуск зависимостей

Запустите все необходимые сервисы через docker-compose:

```bash
docker-compose up -d
```

Это запустит:
- Kafka (порт 9092) с Kafka UI (порт 8080)
- PostgreSQL (порт 5432) с pgAdmin (порт 5050)
- RabbitMQ (порт 5672) с Management UI (порт 15672)
- Iceberg REST Catalog (порт 8181) с UI (порт 8081)
- MinIO (порты 9000, 9001) для хранения данных Iceberg

### Запуск оператора локально

```bash
# Генерация кода и манифестов
make generate
make manifests

# Установка CRD в кластер (если используете kind/minikube)
make install

# Запуск оператора
make run
```

Или используйте скрипт:

```bash
./scripts/run-local.sh
```

### Настройка kind кластера

Для полноценного тестирования используйте kind:

```bash
# Создать kind кластер
./scripts/setup-kind.sh

# Установить CRD
make install

# Запустить оператор локально
make run
```

## Структура проекта

```
dataflow/
├── api/v1/                    # CRD определения
│   ├── dataflow_types.go      # Типы DataFlow ресурсов
│   └── groupversion_info.go    # Версия API
├── internal/
│   ├── connectors/            # Коннекторы для источников/приемников
│   │   ├── interface.go       # Интерфейсы коннекторов
│   │   ├── factory.go         # Фабрика коннекторов
│   │   ├── kafka.go           # Kafka коннектор
│   │   ├── postgresql.go      # PostgreSQL коннектор
│   │   ├── rabbitmq.go        # RabbitMQ коннектор
│   │   └── iceberg.go         # Iceberg коннектор
│   ├── transformers/          # Трансформации сообщений
│   │   ├── interface.go       # Интерфейс трансформаций
│   │   ├── factory.go         # Фабрика трансформаций
│   │   ├── timestamp.go        # Timestamp трансформация
│   │   ├── flatten.go         # Flatten трансформация
│   │   ├── filter.go          # Filter трансформация
│   │   ├── mask.go            # Mask трансформация
│   │   ├── router.go          # Router трансформация
│   │   ├── select.go          # Select трансформация
│   │   └── remove.go          # Remove трансформация
│   ├── processor/             # Процессор сообщений
│   │   └── processor.go       # Оркестрация обработки
│   ├── controller/            # Kubernetes контроллер
│   │   └── dataflow_controller.go
│   └── types/                 # Внутренние типы
│       └── message.go         # Тип Message
├── config/                    # Конфигурация Kubernetes
│   ├── crd/                   # CRD манифесты
│   ├── rbac/                  # RBAC манифесты
│   └── samples/              # Примеры DataFlow ресурсов
├── helm/                      # Helm Chart
│   └── dataflow-operator/
├── docs/                      # Документация MkDocs
├── test/                      # Тесты
│   └── fixtures/             # Тестовые данные
├── scripts/                   # Вспомогательные скрипты
├── main.go                    # Точка входа
├── Makefile                   # Команды сборки
└── go.mod                     # Зависимости Go
```

## Генерация кода

### Генерация CRD и RBAC

```bash
make manifests
```

Эта команда генерирует:
- CRD манифесты в `config/crd/bases/`
- RBAC манифесты в `config/rbac/`

### Генерация DeepCopy методов

```bash
make generate
```

Генерирует методы `DeepCopy` для всех типов в `api/v1/`.

### Обновление controller-gen

Если возникают проблемы с генерацией:

```bash
# Обновить controller-gen
go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest

# Затем
make generate
make manifests
```

## Тестирование

### Unit тесты

```bash
# Запустить все unit тесты
make test-unit

# Запустить тесты с покрытием
make test

# Запустить тесты конкретного пакета
go test ./internal/connectors/... -v

# Запустить тесты с покрытием для конкретного пакета
go test ./internal/transformers/... -coverprofile=coverage.out
go tool cover -html=coverage.out
```

### Интеграционные тесты

```bash
# Настроить kind кластер
./scripts/setup-kind.sh

# Запустить интеграционные тесты
make test-integration
```

### Запуск тестов вручную

```bash
# Unit тесты без envtest
go test ./... -v

# Тесты с envtest (требует kubebuilder)
KUBEBUILDER_ASSETS="$(make envtest use 1.28.0 -p path)" go test ./... -coverprofile cover.out
```

## Сборка

### Локальная сборка

```bash
# Сборка бинарного файла
make build

# Бинарный файл будет в bin/manager
./bin/manager
```

### Сборка Docker образа

```bash
# Сборка образа
make docker-build IMG=your-registry/dataflow-operator:v1.0.0

# Отправка образа
make docker-push IMG=your-registry/dataflow-operator:v1.0.0
```

Или вручную:

```bash
docker build -t your-registry/dataflow-operator:v1.0.0 .
docker push your-registry/dataflow-operator:v1.0.0
```

## Добавление нового коннектора

### 1. Определение типов в API

Добавьте спецификацию в `api/v1/dataflow_types.go`:

```go
// NewConnectorSourceSpec defines new connector source configuration
type NewConnectorSourceSpec struct {
    // Configuration fields
    Endpoint string `json:"endpoint"`
    // ...
}

// Добавьте в SourceSpec
type SourceSpec struct {
    // ...
    NewConnector *NewConnectorSourceSpec `json:"newConnector,omitempty"`
}
```

### 2. Реализация коннектора

Создайте файл `internal/connectors/newconnector.go`:

```go
package connectors

import (
    "context"
    v1 "github.com/dataflow-operator/dataflow/api/v1"
    "github.com/dataflow-operator/dataflow/internal/types"
)

type NewConnectorSourceConnector struct {
    config *v1.NewConnectorSourceSpec
    // connection fields
}

func NewNewConnectorSourceConnector(config *v1.NewConnectorSourceSpec) *NewConnectorSourceConnector {
    return &NewConnectorSourceConnector{config: config}
}

func (n *NewConnectorSourceConnector) Connect(ctx context.Context) error {
    // Implement connection logic
    return nil
}

func (n *NewConnectorSourceConnector) Read(ctx context.Context) (<-chan *types.Message, error) {
    // Implement read logic
    return nil, nil
}

func (n *NewConnectorSourceConnector) Close() error {
    // Implement close logic
    return nil
}
```

### 3. Регистрация в фабрике

Добавьте в `internal/connectors/factory.go`:

```go
func CreateSourceConnector(source *v1.SourceSpec) (SourceConnector, error) {
    switch source.Type {
    // ...
    case "newconnector":
        if source.NewConnector == nil {
            return nil, fmt.Errorf("newconnector source configuration is required")
        }
        return NewNewConnectorSourceConnector(source.NewConnector), nil
    // ...
    }
}
```

### 4. Генерация кода

```bash
make generate
make manifests
```

### 5. Тестирование

Создайте тесты в `internal/connectors/newconnector_test.go`:

```go
func TestNewConnectorSourceConnector(t *testing.T) {
    // Test implementation
}
```

## Добавление новой трансформации

### 1. Определение типов в API

Добавьте в `api/v1/dataflow_types.go`:

```go
// NewTransformation defines new transformation configuration
type NewTransformation struct {
    Field string `json:"field"`
    // ...
}

// Добавьте в TransformationSpec
type TransformationSpec struct {
    // ...
    NewTransformation *NewTransformation `json:"newTransformation,omitempty"`
}
```

### 2. Реализация трансформации

Создайте файл `internal/transformers/newtransformation.go`:

```go
package transformers

import (
    "context"
    v1 "github.com/dataflow-operator/dataflow/api/v1"
    "github.com/dataflow-operator/dataflow/internal/types"
)

type NewTransformer struct {
    config *v1.NewTransformation
}

func NewNewTransformer(config *v1.NewTransformation) *NewTransformer {
    return &NewTransformer{config: config}
}

func (n *NewTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
    // Implement transformation logic
    return []*types.Message{message}, nil
}
```

### 3. Регистрация в фабрике

Добавьте в `internal/transformers/factory.go`:

```go
func CreateTransformer(transformation *v1.TransformationSpec) (Transformer, error) {
    switch transformation.Type {
    // ...
    case "newtransformation":
        if transformation.NewTransformation == nil {
            return nil, fmt.Errorf("newtransformation configuration is required")
        }
        return NewNewTransformer(transformation.NewTransformation), nil
    // ...
    }
}
```

### 4. Генерация и тестирование

```bash
make generate
make test
```

## Отладка

### Логирование

Используйте структурированное логирование:

```go
import "github.com/go-logr/logr"

logger.Info("Processing message", "messageId", msg.ID)
logger.Error(err, "Failed to process", "messageId", msg.ID)
logger.V(1).Info("Debug information", "data", data)
```

### Отладка контроллера

```bash
# Запустить с детальным логированием
go run ./main.go --zap-log-level=debug
```

### Отладка коннекторов

Добавьте логирование в методы коннекторов:

```go
func (k *KafkaSourceConnector) Read(ctx context.Context) (<-chan *types.Message, error) {
    logger.Info("Starting to read from Kafka", "topic", k.config.Topic)
    // ...
}
```

## Форматирование и линтинг

### Форматирование кода

```bash
make fmt
```

Или вручную:

```bash
go fmt ./...
```

### Проверка кода

```bash
make vet
```

Или вручную:

```bash
go vet ./...
```

### Линтинг (опционально)

Установите golangci-lint:

```bash
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
```

Запустите:

```bash
golangci-lint run
```

## CI/CD

### GitHub Actions

Пример workflow для CI:

```yaml
name: CI

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4
        with:
          go-version: '1.21'
      - run: go mod download
      - run: make test-unit
      - run: make vet
      - run: make fmt
```

## Вклад в проект

### Процесс разработки

1. Создайте issue для обсуждения изменений
2. Создайте feature branch: `git checkout -b feature/new-feature`
3. Внесите изменения и добавьте тесты
4. Убедитесь, что все тесты проходят: `make test`
5. Отформатируйте код: `make fmt`
6. Создайте Pull Request

### Стандарты кода

- Следуйте Go code review comments
- Добавляйте комментарии для публичных функций
- Пишите тесты для нового функционала
- Обновляйте документацию при необходимости

### Коммиты

Используйте понятные сообщения коммитов:

```
feat: add new connector for Redis
fix: handle connection errors in Kafka connector
docs: update getting started guide
test: add tests for filter transformation
```

## Полезные команды

```bash
# Просмотр всех доступных команд
make help

# Очистка сгенерированных файлов
make clean

# Обновление зависимостей
go mod tidy

# Просмотр зависимостей
go list -m all

# Проверка устаревших зависимостей
go list -u -m all
```

## Ресурсы

- [Kubebuilder Book](https://book.kubebuilder.io/) - руководство по созданию Kubernetes операторов
- [controller-runtime](https://github.com/kubernetes-sigs/controller-runtime) - библиотека для контроллеров
- [Go Documentation](https://golang.org/doc/) - документация Go

## Получение помощи

- Создайте issue в репозитории
- Проверьте существующие issues и PR
- Изучите примеры в `config/samples/`

