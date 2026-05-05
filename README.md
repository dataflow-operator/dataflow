# DataFlow Operator

Kubernetes operator for streaming data between different sources and sinks with support for message transformations.

**[Online documentation](https://dataflow-operator.github.io/docs/)**

## Architecture

The operator watches `DataFlow` custom resources and creates processor pods. Each processor reads from a source, applies optional transformations, and writes to a sink. Configuration uses the `type` + `config` format:

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-to-postgres
spec:
  source:
    type: kafka
    config:
      brokers:
        - localhost:9092
      topic: input-topic
      consumerGroup: dataflow-group
  sink:
    type: postgresql
    config:
      connectionString: "postgres://user:pass@localhost:5432/db?sslmode=disable"
      table: output_table
```

### Supported connectors

| Source | Sink |
|--------|------|
| Kafka | Kafka |
| PostgreSQL | PostgreSQL |
| — | ClickHouse |
| — | Trino |
| — | Nessie (Apache Iceberg) |

### Supported transformations

Filter, Select, Remove, Mask, Flatten, Timestamp, SnakeCase, Router, Chain.

### DataFlowCron

`DataFlowCron` runs a pipeline by cron schedule.

- `processor` (`source -> transformations -> sink`) runs first.
- `triggers` run after successful `processor` completion.
- Polling sources (`postgresql`, `trino`, `clickhouse`, `nessie`) can finish a run on `source exhausted`.
- Kafka is a streaming source and is not considered exhausted by default.

## Prerequisites

- Kubernetes 1.24+
- Helm 3.0+
- kubectl
- Go 1.25+ (for local development)
- Docker and docker-compose (for local development)

## Quick Start

### Install via Helm

```bash
helm repo add dataflow-operator https://dataflow-operator.github.io/helm-charts
helm repo update
helm install dataflow-operator dataflow-operator/dataflow-operator
```

### Local Development Setup

1. Start dependencies:

```bash
docker-compose up -d
```

Available UIs:
- **Kafka UI**: http://localhost:8080
- **ClickHouse**: http://localhost:8123

2. Run the operator:

```bash
task run
```

## Development

### Code Generation

```bash
task generate   # DeepCopy, CRD, RBAC manifests
task manifests  # CRD and RBAC only
```

If you encounter issues with `task generate`:

```bash
go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest
task generate
```

### Building

```bash
task build  # builds bin/manager
```

Docker image (builds both operator and processor binaries):

```bash
docker build -t dataflow:local .
```

### Testing

```bash
# Unit tests
task test

# Integration tests (requires Docker — uses testcontainers)
task test-integration
```

### Taskfile commands

| Command | Description |
|---------|-------------|
| `task build` | Build `bin/manager` |
| `task run` | Run operator locally |
| `task generate` | Generate DeepCopy and manifests |
| `task manifests` | Generate CRD and RBAC |
| `task test` | Unit tests |
| `task test-integration` | Integration tests (Docker required) |
| `task install` | Install CRDs into cluster |
| `task uninstall` | Remove CRDs from cluster |

## Configuration samples

Example manifests are in `config/samples/`:

- `kafka-to-postgres.yaml` — Kafka to PostgreSQL
- `kafka-to-clickhouse.yaml` — Kafka to ClickHouse
- `kafka-to-trino.yaml` — Kafka to Trino
- `postgres-to-kafka-router.yaml` — PostgreSQL to Kafka with Router
- `clickhouse-to-clickhouse.yaml` — ClickHouse to ClickHouse
- `dataflowcron-example.yaml` — Scheduled run with post-processor triggers

See all samples: `ls config/samples/`

## License

Apache License 2.0
