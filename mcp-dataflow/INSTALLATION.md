# Установка и настройка DataFlow MCP Server

> **Примечание:** После установки см. [USAGE.md](USAGE.md) для примеров использования.

## Предварительные требования

- Доступ к Kubernetes кластеру
- kubeconfig файл с правами на создание/чтение/удаление DataFlow ресурсов
- Docker (для варианта с Docker) или предкомпилированный бинарник
- [Task](https://taskfile.dev/) (опционально, для удобной сборки) - установите через `brew install go-task/tap/go-task` или скачайте с [taskfile.dev](https://taskfile.dev/installation/)

## Варианты установки

Выберите один из двух вариантов установки:

### Вариант 1: Запуск через Docker (Рекомендуется)

#### 1.1. Сборка Docker образа

**Используя Task:**
```bash
cd mcp-dataflow
task docker-build
```

**Или напрямую через Docker:**
```bash
cd mcp-dataflow
docker build -t mcp-dataflow:latest .
```

**Или используйте готовый образ из реестра:**
```bash
docker pull ghcr.io/ilyario/mcp-dataflow:latest
```

#### 1.2. Настройка в Cursor

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

Если kubeconfig находится в другом месте, измените путь в `-v` параметре.

**Альтернативный вариант с переменной окружения:**
```json
{
  "mcpServers": {
    "dataflow": {
      "command": "docker",
      "args": [
        "run",
        "--rm",
        "-i",
        "-v", "${HOME}/.kube:/root/.kube:ro",
        "-e", "KUBECONFIG=/root/.kube/config",
        "mcp-dataflow:latest"
      ]
    }
  }
}
```

### Вариант 2: Запуск через бинарник

#### 2.1. Скачивание предкомпилированного бинарника

Скачайте бинарник для вашей платформы из [релизов](https://github.com/ilyario/dataflow/releases) или соберите самостоятельно:

**Используя Task (рекомендуется):**
```bash
cd mcp-dataflow
task build-all
```

Бинарники будут в директории `dist/`:
- `mcp-dataflow-linux-amd64` - Linux x86_64
- `mcp-dataflow-linux-arm64` - Linux ARM64
- `mcp-dataflow-darwin-amd64` - macOS Intel
- `mcp-dataflow-darwin-arm64` - macOS Apple Silicon
- `mcp-dataflow-windows-amd64.exe` - Windows x86_64

**Сборка для текущей платформы:**
```bash
cd mcp-dataflow
task build
```

**Или сборка для конкретной платформы:**
```bash
task build-linux      # Linux amd64
task build-darwin     # macOS Intel
task build-darwin-arm # macOS Apple Silicon
task build-windows    # Windows amd64
```

**Ручная сборка (без Task):**
```bash
cd mcp-dataflow
go mod download
go mod tidy
go build -o mcp-dataflow cmd/server/main.go
```

#### 2.2. Установка бинарника

**macOS/Linux:**
```bash
# Переместите бинарник в удобное место
sudo mv dist/mcp-dataflow-darwin-amd64 /usr/local/bin/mcp-dataflow
sudo chmod +x /usr/local/bin/mcp-dataflow
```

**Windows:**
```powershell
# Переместите бинарник в PATH или используйте полный путь
# Например, в C:\tools\mcp-dataflow.exe
```

#### 2.3. Настройка в Cursor

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

Если kubeconfig находится в стандартном месте (`~/.kube/config` на Unix или `%USERPROFILE%\.kube\config` на Windows), можно не указывать `--kubeconfig`:

```json
{
  "mcpServers": {
    "dataflow": {
      "command": "/usr/local/bin/mcp-dataflow"
    }
  }
}
```

## Настройка в Cursor

Добавьте конфигурацию MCP сервера в настройки Cursor:

**macOS/Linux:**
```json
{
  "mcpServers": {
    "dataflow": {
      "command": "/absolute/path/to/mcp-dataflow/mcp-dataflow",
      "args": ["--kubeconfig", "/path/to/your/kubeconfig"]
    }
  }
}
```

**Windows:**
```json
{
  "mcpServers": {
    "dataflow": {
      "command": "C:\\path\\to\\mcp-dataflow\\mcp-dataflow.exe",
      "args": ["--kubeconfig", "C:\\path\\to\\your\\kubeconfig"]
    }
  }
}
```

### 3. Перезапуск Cursor

После добавления конфигурации перезапустите Cursor для применения изменений.

## Проверка установки

После перезапуска Cursor, MCP сервер должен быть доступен. Вы можете проверить это, попросив AI-ассистента:

- "Покажи список доступных DataFlow ресурсов"
- "Создай DataFlow из Kafka в PostgreSQL"

**Подробные примеры использования:** См. [USAGE.md](USAGE.md)

## Разрешения Kubernetes

MCP серверу требуются следующие разрешения в Kubernetes:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: dataflow-mcp-server
rules:
- apiGroups: ["dataflow.dataflow.io"]
  resources: ["dataflows"]
  verbs: ["get", "list", "create", "update", "patch", "delete", "watch"]
- apiGroups: [""]
  resources: ["pods", "secrets"]
  verbs: ["get", "list"]
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: dataflow-mcp-server
  namespace: default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: dataflow-mcp-server
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: dataflow-mcp-server
subjects:
- kind: ServiceAccount
  name: dataflow-mcp-server
  namespace: default
```

## Устранение неполадок

### Ошибка подключения к Kubernetes

Убедитесь, что:
- kubeconfig файл существует и доступен
- У вас есть права на доступ к кластеру
- Кластер доступен

### Ошибка создания DataFlow ресурса

Проверьте:
- Установлен ли CRD DataFlow в кластере
- Есть ли права на создание ресурсов в namespace
- Корректна ли конфигурация DataFlow

### MCP сервер не запускается

**Для Docker:**
- Убедитесь, что Docker запущен
- Проверьте, что образ собран: `docker images | grep mcp-dataflow`
- Проверьте логи: `docker logs <container_id>`
- Убедитесь, что путь к kubeconfig правильный в volume mount

**Для бинарника:**
- Убедитесь, что бинарник имеет права на выполнение: `chmod +x mcp-dataflow`
- Проверьте, что путь к бинарнику правильный
- Попробуйте запустить вручную: `./mcp-dataflow --help`

## Разработка

Для разработки и тестирования используйте Task:

```bash
# Показать все доступные команды
task help

# Запуск тестов
task test

# Запуск тестов с покрытием
task test-coverage

# Форматирование кода
task fmt

# Проверка кода
task vet

# Запуск линтера (если установлен)
task lint

# Запуск с отладкой
go run cmd/server/main.go --kubeconfig ~/.kube/config

# Сборка Docker образа для тестирования
task docker-build

# Запуск Docker контейнера для тестирования
task docker-run

# Очистка артефактов сборки
task clean
```

## Примечания

- MCP сервер использует стандартный Kubernetes client-go
- Для работы с метриками Prometheus требуется дополнительная настройка (см. TODO в коде)
- YAML парсинг в CreateDataFlow требует доработки (сейчас ожидается JSON)
