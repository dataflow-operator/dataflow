# Getting Started

Это руководство поможет вам начать работу с DataFlow Operator. Вы узнаете, как установить оператор, создать первый поток данных и настроить локальную среду разработки.

## Предварительные требования

### Для production установки

- Kubernetes кластер (версия 1.24+)
- Helm 3.0+
- kubectl настроен для работы с кластером
- Доступ к источникам данных (Kafka, PostgreSQL, RabbitMQ, Iceberg)

### Для локальной разработки

- Go 1.21+
- Docker и Docker Compose
- Make (опционально, для использования Makefile команд)
- Доступ к портам: 8080, 5050, 15672, 8081, 5432, 9092, 5672

## Установка

### Установка CRD

Перед установкой оператора необходимо установить Custom Resource Definition (CRD):

```bash
kubectl apply -f https://raw.githubusercontent.com/ilyario/dataflow/refs/heads/main/config/crd/bases/dataflow.dataflow.io_dataflows.yaml
```

Или используйте локальный файл:

```bash
kubectl apply -f config/crd/bases/dataflow.dataflow.io_dataflows.yaml
```

### Установка через Helm (рекомендуется)

#### Базовая установка

Самый простой способ установить оператор:

```bash
helm install dataflow-operator ./helm/dataflow-operator
```

Эта команда установит оператор с настройками по умолчанию в namespace `default`.

#### Установка в конкретный namespace

```bash
helm install dataflow-operator ./helm/dataflow-operator \
  --namespace dataflow-system \
  --create-namespace
```

#### Установка с кастомными настройками

Вы можете переопределить значения по умолчанию через флаги:

```bash
helm install dataflow-operator ./helm/dataflow-operator \
  --set image.repository=your-registry/controller \
  --set image.tag=v1.0.0 \
  --set replicaCount=2 \
  --set leaderElection.enabled=true \
  --set resources.limits.memory=1Gi \
  --set resources.limits.cpu=500m \
  --set resources.requests.memory=256Mi \
  --set resources.requests.cpu=100m
```

#### Установка с файлом values

Для более сложных конфигураций создайте файл `my-values.yaml`:

```yaml
image:
  repository: your-registry/controller
  tag: v1.0.0

replicaCount: 2

leaderElection:
  enabled: true

resources:
  limits:
    cpu: 1000m
    memory: 1Gi
  requests:
    cpu: 500m
    memory: 512Mi

# Настройки для работы с Kubernetes API
serviceAccount:
  create: true
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/dataflow-operator

# Настройки безопасности
securityContext:
  runAsNonRoot: true
  runAsUser: 1000
  fsGroup: 1000
```

Затем установите:

```bash
helm install dataflow-operator ./helm/dataflow-operator -f my-values.yaml
```

#### Проверка установки

После установки проверьте статус:

```bash
# Проверьте статус подов
kubectl get pods -l app.kubernetes.io/name=dataflow-operator

# Проверьте CRD
kubectl get crd dataflows.dataflow.dataflow.io

# Проверьте логи оператора
kubectl logs -l app.kubernetes.io/name=dataflow-operator --tail=50

# Проверьте статус deployment
kubectl get deployment dataflow-operator
```

Ожидаемый вывод:

```
NAME                                  READY   STATUS    RESTARTS   AGE
dataflow-operator-7d8f9c4b5d-xxxxx   1/1     Running   0          1m
```

### Обновление

Для обновления оператора до новой версии:

```bash
helm upgrade dataflow-operator ./helm/dataflow-operator
```

С кастомными значениями:

```bash
helm upgrade dataflow-operator ./helm/dataflow-operator -f my-values.yaml
```

Для обновления до конкретной версии:

```bash
helm upgrade dataflow-operator ./helm/dataflow-operator \
  --set image.tag=v1.1.0
```

### Удаление

Для удаления оператора:

```bash
helm uninstall dataflow-operator
```

**Внимание**: Удаление оператора не удаляет созданные ресурсы `DataFlow`, но они перестанут обрабатываться. Если вы хотите удалить все ресурсы:

```bash
# Удалить все DataFlow ресурсы
kubectl delete dataflow --all

# Затем удалить оператор
helm uninstall dataflow-operator
```

## Первый DataFlow

### Простой пример: Kafka → PostgreSQL

Создайте простой DataFlow ресурс для передачи данных из Kafka в PostgreSQL:

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-to-postgres
  namespace: default
spec:
  source:
    type: kafka
    kafka:
      brokers:
        - kafka-broker:9092
      topic: input-topic
      consumerGroup: dataflow-group
  sink:
    type: postgresql
    postgresql:
      connectionString: "postgres://user:password@postgres-host:5432/dbname?sslmode=disable"
      table: output_table
      autoCreateTable: true
```

Примените ресурс:

```bash
kubectl apply -f config/samples/kafka-to-postgres.yaml
```

### Пример с Kubernetes Secrets

Для безопасного хранения credentials используйте Kubernetes Secrets. См. пример:

```bash
kubectl apply -f config/samples/kafka-to-postgres-secrets.yaml
```

Этот пример демонстрирует использование `SecretRef` для конфигурации коннекторов. Подробнее см. раздел [Использование Secrets в Kubernetes](connectors.md#использование-secrets-в-kubernetes) в документации по коннекторам.

### Проверка статуса

Проверьте статус созданного потока данных:

```bash
# Получить информацию о DataFlow
kubectl get dataflow kafka-to-postgres

# Детальная информация
kubectl describe dataflow kafka-to-postgres

# Посмотреть статус в формате YAML
kubectl get dataflow kafka-to-postgres -o yaml
```

Ожидаемый статус:

```yaml
status:
  phase: Running
  processedCount: 150
  errorCount: 0
  lastProcessedTime: "2024-01-15T10:30:00Z"
  message: "Processing messages successfully"
```

### Отправка тестового сообщения

Для тестирования потока данных отправьте сообщение в Kafka топик:

```bash
# Используя kafka-console-producer
kafka-console-producer --broker-list localhost:9092 --topic input-topic
# Введите JSON сообщение и нажмите Enter
{"id": 1, "name": "Test", "value": 100}
```

Или используйте скрипт из проекта:

```bash
./scripts/send-test-message.sh
```

### Проверка данных в PostgreSQL

Подключитесь к PostgreSQL и проверьте данные:

```bash
psql postgres://user:password@postgres-host:5432/dbname

# Проверьте таблицу
SELECT * FROM output_table;
```

## Локальная разработка

### Запуск зависимостей

Используйте docker-compose для запуска всех зависимостей локально:

```bash
docker-compose up -d
```

Эта команда запустит:

- **Kafka** (порт 9092) с Kafka UI (порт 8080)
- **PostgreSQL** (порт 5432) с pgAdmin (порт 5050)
- **RabbitMQ** (порт 5672) с Management UI (порт 15672)
- **Iceberg REST Catalog** (порт 8181) с UI (порт 8081)
- **MinIO** (порт 9000, 9001) для хранения данных Iceberg

### Доступ к UI интерфейсам

После запуска доступны следующие UI:

- **Kafka UI**: http://localhost:8080
  - Просмотр топиков, сообщений, consumer groups
- **pgAdmin**: http://localhost:5050
  - Логин: `admin@admin.com`, пароль: `admin`
  - Управление PostgreSQL базами данных
- **RabbitMQ Management**: http://localhost:15672
  - Логин: `guest`, пароль: `guest`
  - Управление очередями и exchanges
- **Iceberg REST UI**: http://localhost:8081
  - Просмотр таблиц и namespaces
- **MinIO Console**: http://localhost:9001
  - Логин: `minioadmin`, пароль: `minioadmin`
  - Управление объектным хранилищем

### Запуск оператора локально

Для разработки запустите оператор локально:

```bash
# Установите CRD в кластер (если используете kind или minikube)
make install

# Запустите оператор
make run
```

Или используйте скрипт:

```bash
./scripts/run-local.sh
```

### Настройка локального кластера (опционально)

Для полноценного тестирования используйте kind (Kubernetes in Docker):

```bash
# Создать kind кластер
./scripts/setup-kind.sh

# Установить CRD
make install

# Запустить оператор локально
make run
```

### Отладка

Для отладки используйте логи оператора:

```bash
# Если оператор запущен локально, логи выводятся в консоль
# Для оператора в кластере:
kubectl logs -l app.kubernetes.io/name=dataflow-operator -f
```

Проверьте события Kubernetes:

```bash
kubectl get events --sort-by='.lastTimestamp' | grep dataflow
```

## Следующие шаги

Теперь, когда вы установили оператор и создали первый поток данных:

1. Изучите [Connectors](connectors.md) для понимания всех доступных источников и приемников
2. Ознакомьтесь с [Transformations](transformations.md) для работы с трансформациями сообщений
3. Посмотрите [Examples](examples.md) для практических примеров использования
4. Прочитайте [Development](development.md) для участия в разработке

## Устранение неполадок

### Оператор не запускается

```bash
# Проверьте логи
kubectl logs -l app.kubernetes.io/name=dataflow-operator

# Проверьте события
kubectl describe pod -l app.kubernetes.io/name=dataflow-operator

# Проверьте CRD
kubectl get crd dataflows.dataflow.dataflow.io -o yaml
```

### DataFlow не обрабатывает сообщения

1. Проверьте статус DataFlow:
   ```bash
   kubectl describe dataflow <name>
   ```

2. Проверьте подключение к источнику данных:
   ```bash
   # Для Kafka
   kafka-console-consumer --bootstrap-server localhost:9092 --topic <topic>

   # Для PostgreSQL
   psql <connection-string> -c "SELECT * FROM <table> LIMIT 10;"
   ```

3. Проверьте логи оператора на наличие ошибок

### Проблемы с подключением

- Убедитесь, что источники данных доступны из кластера
- Проверьте сетевые политики Kubernetes
- Проверьте правильность connection strings и credentials
- Для локальной разработки используйте `localhost` или `host.docker.internal`



