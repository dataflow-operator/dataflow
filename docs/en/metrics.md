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

### Детальные метрики выполнения задач

- `dataflow_task_stage_duration_seconds` - время выполнения отдельных этапов задачи (гистограмма)
  - Метки: `namespace`, `name`, `stage`
  - Этапы: `read`, `transformation`, `write`, `sink_write`, `error_sink_write`, `transformer_0`, `transformer_1`, и т.д.

- `dataflow_task_message_size_bytes` - размер сообщений на разных этапах обработки (гистограмма)
  - Метки: `namespace`, `name`, `stage`
  - Этапы: `input`, `output`, `transformer_0_input`, `transformer_0_output`, и т.д.

- `dataflow_task_stage_latency_seconds` - задержка между этапами обработки (гистограмма)
  - Метки: `namespace`, `name`, `from_stage`, `to_stage`

- `dataflow_task_throughput_messages_per_second` - текущая пропускная способность (сообщений в секунду)
  - Метки: `namespace`, `name`

- `dataflow_task_success_rate` - процент успешных задач (0.0 до 1.0)
  - Метки: `namespace`, `name`

- `dataflow_task_end_to_end_latency_seconds` - полное время жизни сообщения от получения до отправки (гистограмма)
  - Метки: `namespace`, `name`

- `dataflow_task_active_messages` - количество активных сообщений в обработке
  - Метки: `namespace`, `name`

- `dataflow_task_queue_size` - текущий размер очереди сообщений
  - Метки: `namespace`, `name`, `queue_type`
  - Типы очередей: `routing`, `output`, `default`, и маршруты роутера

- `dataflow_task_queue_wait_time_seconds` - время ожидания сообщений в очереди (гистограмма)
  - Метки: `namespace`, `name`, `queue_type`

- `dataflow_task_operations_total` - общее количество операций по типу
  - Метки: `namespace`, `name`, `operation`, `status`
  - Операции: `transform`, `write`, `sink_write`, `error_sink_write`
  - Статусы: `success`, `error`, `cancelled`

- `dataflow_task_stage_errors_total` - количество ошибок на каждом этапе
  - Метки: `namespace`, `name`, `stage`, `error_type`

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

### Пропускная способность задач

```promql
dataflow_task_throughput_messages_per_second
```

### Процент успешных задач

```promql
dataflow_task_success_rate * 100
```

### p95 время выполнения этапа чтения

```promql
histogram_quantile(0.95, sum(rate(dataflow_task_stage_duration_seconds_bucket{stage="read"}[5m])) by (namespace, name, le))
```

### Средний размер сообщений на входе

```promql
avg(dataflow_task_message_size_bytes{stage="input"}) by (namespace, name)
```

### p99 end-to-end latency

```promql
histogram_quantile(0.99, sum(rate(dataflow_task_end_to_end_latency_seconds_bucket[5m])) by (namespace, name, le))
```

### Количество активных сообщений в обработке

```promql
dataflow_task_active_messages
```

### Размер очереди сообщений

```promql
dataflow_task_queue_size
```

### Среднее время ожидания в очереди

```promql
avg(rate(dataflow_task_queue_wait_time_seconds_sum[5m])) by (namespace, name, queue_type)
/
avg(rate(dataflow_task_queue_wait_time_seconds_count[5m])) by (namespace, name, queue_type)
```

### Процент ошибок по этапам

```promql
sum(rate(dataflow_task_stage_errors_total[5m])) by (namespace, name, stage)
/
sum(rate(dataflow_task_operations_total[5m])) by (namespace, name)
* 100
```
