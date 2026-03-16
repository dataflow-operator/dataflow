## [1.0.8](https://github.com/dataflow-operator/dataflow/compare/v1.0.7...v1.0.8) (2026-03-16)

## [1.0.7](https://github.com/dataflow-operator/dataflow/compare/v1.0.6...v1.0.7) (2026-03-15)

## [1.0.6](https://github.com/dataflow-operator/dataflow/compare/v1.0.5...v1.0.6) (2026-03-15)

## [1.0.5](https://github.com/dataflow-operator/dataflow/compare/v1.0.4...v1.0.5) (2026-03-15)

## [1.0.4](https://github.com/dataflow-operator/dataflow/compare/v1.0.3...v1.0.4) (2026-03-15)

### BREAKING CHANGE

Удалена поддержка legacy-формата конфигурации. Source, sink и transformations теперь используют только формат `type` + `config`.

**Миграция:**
```yaml
# Было (legacy):
source:
  type: kafka
  kafka:
    brokers: [localhost:9092]
    topic: my-topic

# Стало:
source:
  type: kafka
  config:
    brokers: [localhost:9092]
    topic: my-topic
```

Структура внутри `config` совпадает со структурой внутри `kafka`/`postgresql`/`clickhouse`/`trino` — меняется только ключ верхнего уровня.

## [1.0.3](https://github.com/dataflow-operator/dataflow/compare/v1.0.2...v1.0.3) (2026-03-09)

## [1.0.2](https://github.com/dataflow-operator/dataflow/compare/v1.0.1...v1.0.2) (2026-03-09)

## [1.0.1](https://github.com/dataflow-operator/dataflow/compare/v1.0.0...v1.0.1) (2026-03-09)

# 1.0.0 (2026-03-09)
