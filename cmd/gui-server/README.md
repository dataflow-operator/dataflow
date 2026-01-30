# DataFlow GUI Server

Веб-интерфейс для управления DataFlow манифестами, просмотра логов и мониторинга метрик.

## Возможности

- **Управление манифестами**: Создание, просмотр, обновление и удаление DataFlow ресурсов
- **Просмотр логов**: Получение логов из подов обработки в реальном времени
- **Метрики и статус**: Мониторинг текущего состояния обработки, количества обработанных сообщений и ошибок

## Запуск

### Локально

```bash
go run cmd/gui-server/main.go --bind-address :8080
```

### С параметрами

```bash
# Используя переменную окружения для kubeconfig
export KUBECONFIG=~/.kube/config
go run cmd/gui-server/main.go \
  --bind-address :8080 \
  --log-level debug
```

Или если плагин auth зарегистрировал флаг `--kubeconfig`:

```bash
go run cmd/gui-server/main.go \
  --bind-address :8080 \
  --kubeconfig ~/.kube/config \
  --log-level debug
```

### В Kubernetes

GUI сервер можно запустить как отдельный deployment в кластере Kubernetes.

## API Endpoints

### DataFlow манифесты

- `GET /api/dataflows?namespace=<namespace>` - Список всех DataFlow
- `GET /api/dataflows/<name>?namespace=<namespace>` - Получить конкретный DataFlow
- `POST /api/dataflows?namespace=<namespace>` - Создать новый DataFlow
- `PUT /api/dataflows/<name>?namespace=<namespace>` - Обновить DataFlow
- `DELETE /api/dataflows/<name>?namespace=<namespace>` - Удалить DataFlow

### Логи

- `GET /api/logs?namespace=<namespace>&name=<name>&tailLines=<n>&follow=<true|false>` - Получить логи

### Статус

- `GET /api/status?namespace=<namespace>&name=<name>` - Получить статус обработки

### Метрики

- `GET /api/metrics?namespace=<namespace>&name=<name>` - Получить метрики

## Использование

1. Откройте браузер и перейдите на `http://localhost:8080`
2. Используйте вкладки для переключения между:
   - **Манифесты**: Управление DataFlow ресурсами
   - **Логи**: Просмотр логов обработки
   - **Метрики**: Мониторинг статуса и метрик

## Требования

- Доступ к Kubernetes кластеру
- Права на чтение/запись DataFlow ресурсов
- Права на чтение логов подов
