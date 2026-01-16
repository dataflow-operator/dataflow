# Метрики DataFlow Operator

DataFlow Operator экспортирует метрики Prometheus для мониторинга работы оператора и обработки данных.

## Доступные метрики

### Метрики DataFlow манифестов

- `dataflow_messages_received_total` - общее количество полученных сообщений по манифесту
  - Метки: `namespace`, `name`, `source_type`

- `dataflow_messages_sent_total` - общее количество отправленных сообщений по манифесту
  - Метки: `namespace`, `name`, `sink_type`, `route`

- `dataflow_processing_duration_seconds` - время обработки сообщений (гистограмма)
  - Метки: `namespace`, `name`

- `dataflow_status` - статус DataFlow манифеста (1 = Running, 0 = Stopped/Error)
  - Метки: `namespace`, `name`, `phase`

### Метрики коннекторов

- `dataflow_connector_messages_read_total` - количество прочитанных сообщений из source коннектора
  - Метки: `namespace`, `name`, `connector_type`, `connector_name`

- `dataflow_connector_messages_written_total` - количество записанных сообщений в sink коннектор
  - Метки: `namespace`, `name`, `connector_type`, `connector_name`, `route`

- `dataflow_connector_errors_total` - количество ошибок в коннекторах
  - Метки: `namespace`, `name`, `connector_type`, `connector_name`, `operation`, `error_type`

- `dataflow_connector_connection_status` - статус подключения коннектора (1 = connected, 0 = disconnected)
  - Метки: `namespace`, `name`, `connector_type`, `connector_name`

### Метрики трансформеров

- `dataflow_transformer_executions_total` - количество выполнений трансформера
  - Метки: `namespace`, `name`, `transformer_type`, `transformer_index`

- `dataflow_transformer_errors_total` - количество ошибок в трансформерах
  - Метки: `namespace`, `name`, `transformer_type`, `transformer_index`, `error_type`

- `dataflow_transformer_duration_seconds` - время выполнения трансформера (гистограмма)
  - Метки: `namespace`, `name`, `transformer_type`, `transformer_index`

- `dataflow_transformer_messages_in_total` - количество входящих сообщений в трансформер
  - Метки: `namespace`, `name`, `transformer_type`, `transformer_index`

- `dataflow_transformer_messages_out_total` - количество исходящих сообщений из трансформера
  - Метки: `namespace`, `name`, `transformer_type`, `transformer_index`

## Настройка мониторинга

### Prometheus ServiceMonitor

Для автоматического обнаружения метрик Prometheus создайте ServiceMonitor:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: dataflow-operator
  labels:
    app: dataflow-operator
spec:
  selector:
    matchLabels:
      app: dataflow-operator
  endpoints:
    - port: metrics
      path: /metrics
      interval: 30s
```

Или включите ServiceMonitor в Helm chart:

```yaml
serviceMonitor:
  enabled: true
  interval: 30s
  scrapeTimeout: 10s
```

### Grafana Dashboard

Импортируйте дашборд из файла `grafana-dashboard.json` в Grafana для визуализации метрик.

Дашборд включает:
- Графики количества полученных/отправленных сообщений
- Графики ошибок в коннекторах и трансформерах
- Время обработки сообщений
- Статус подключения коннекторов
- Статус DataFlow манифестов
- Статистику по трансформерам

## Примеры запросов Prometheus

### Количество сообщений в секунду по манифесту

```promql
sum(rate(dataflow_messages_received_total[5m])) by (namespace, name)
```

### Процент ошибок в трансформерах

```promql
sum(rate(dataflow_transformer_errors_total[5m])) by (namespace, name, transformer_type)
/
sum(rate(dataflow_transformer_executions_total[5m])) by (namespace, name, transformer_type)
* 100
```

### p95 время обработки сообщений

```promql
histogram_quantile(0.95, sum(rate(dataflow_processing_duration_seconds_bucket[5m])) by (namespace, name, le))
```

### Количество активных DataFlow манифестов

```promql
sum(dataflow_status) by (namespace, name)
```

### Средняя скорость обработки сообщений

```promql
avg(rate(dataflow_messages_received_total[5m])) by (namespace, name)
```

### Топ манифестов по количеству ошибок

```promql
topk(10, sum(rate(dataflow_connector_errors_total[5m])) by (namespace, name))
```

### Время выполнения трансформеров (p99)

```promql
histogram_quantile(0.99, sum(rate(dataflow_transformer_duration_seconds_bucket[5m])) by (namespace, name, transformer_type, transformer_index, le))
```

## Интерпретация метрик

### Метрики производительности

- **dataflow_messages_received_total** - показывает общую нагрузку на систему. Резкий рост может указывать на увеличение трафика.
- **dataflow_processing_duration_seconds** - время обработки сообщений. Высокие значения могут указывать на узкие места в обработке.
- **dataflow_transformer_duration_seconds** - время выполнения трансформеров. Помогает выявить медленные трансформации.

### Метрики ошибок

- **dataflow_connector_errors_total** - ошибки подключения или записи/чтения в коннекторах. Высокое значение требует внимания.
- **dataflow_transformer_errors_total** - ошибки в трансформерах. Может указывать на проблемы с данными или конфигурацией.

### Метрики состояния

- **dataflow_connector_connection_status** - статус подключения коннекторов. Значение 0 означает, что коннектор отключен.
- **dataflow_status** - статус DataFlow манифеста. Значение 1 означает, что манифест работает.

## Алерты

Рекомендуется настроить следующие алерты:

### Высокий процент ошибок

```promql
(
  sum(rate(dataflow_connector_errors_total[5m])) by (namespace, name)
  +
  sum(rate(dataflow_transformer_errors_total[5m])) by (namespace, name)
)
/
(
  sum(rate(dataflow_messages_received_total[5m])) by (namespace, name)
)
> 0.01
```

### Медленная обработка сообщений

```promql
histogram_quantile(0.95, sum(rate(dataflow_processing_duration_seconds_bucket[5m])) by (namespace, name, le)) > 1
```

### Отключенные коннекторы

```promql
dataflow_connector_connection_status == 0
```

### Остановленные манифесты

```promql
dataflow_status == 0
```
