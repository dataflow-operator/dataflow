# DataFlow Operator

A production-ready Kubernetes operator for streaming data pipelines with support for multiple sources, sinks, and comprehensive message transformations.

**[📖 Full Documentation](https://dataflow-operator.github.io/docs/)**

---

## Overview

DataFlow Operator automates the deployment and management of data streaming pipelines in Kubernetes. It watches custom `DataFlow` resources and orchestrates processor pods that read from sources, apply optional transformations, and write to sinks. The operator handles fault tolerance, checkpointing, scheduling, and comprehensive monitoring out of the box.

### Key Features

- **Multi-Source/Sink Support**: Kafka, PostgreSQL, ClickHouse, Trino, and Apache Iceberg (Nessie)
- **Rich Transformations**: Filter, Select, Remove, Mask, Flatten, Timestamp, SnakeCase, Router, Chain
- **Stateful Processing**: Checkpoint persistence for exactly-once semantics
- **Scheduled Pipelines**: `DataFlowCron` for time-based pipeline execution with triggers
- **High Availability**: Leader election for multi-replica deployments
- **Observable**: Prometheus metrics, structured logging, health probes, pprof profiling
- **Kubernetes Native**: Custom Resource Definitions, RBAC, Helm charts

---

## Architecture

### How It Works

1. **Operator Controller**: Watches `DataFlow` and `DataFlowCron` resources in your cluster
2. **Processor Pod**: Creates ephemeral or long-running pods that execute the data pipeline
3. **Pipeline Execution**: `source → transformations → sink` flow with built-in error handling
4. **State Management**: Stores checkpoint data in ConfigMaps for recovery

### Configuration Example

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
        - kafka-broker:9092
      topic: input-events
      consumerGroup: dataflow-group
  
  transformations:
    - type: filter
      config:
        expression: "event_type == 'purchase'"
    - type: mask
      config:
        fields:
          - credit_card
  
  sink:
    type: postgresql
    config:
      connectionString: "postgres://user:pass@db:5432/warehouse?sslmode=disable"
      table: events
```

### Supported Connectors

| Source | Sink |
|--------|------|
| ✅ Kafka | ✅ Kafka |
| ✅ PostgreSQL | ✅ PostgreSQL |
| — | ✅ ClickHouse |
| — | ✅ Trino |
| — | ✅ Nessie (Apache Iceberg) |

### Supported Transformations

- **Filter**: Conditional message filtering based on expressions
- **Select**: Extract specific fields from messages
- **Remove**: Drop specified fields
- **Mask**: Redact sensitive data (PII protection)
- **Flatten**: Expand nested arrays into separate messages
- **Timestamp**: Add/update timestamp fields
- **SnakeCase**: Normalize field names to snake_case
- **Router**: Route messages to different destinations based on conditions
- **Chain**: Compose multiple transformations in sequence

### DataFlowCron: Scheduled Pipelines

For batch processing and scheduled workflows:

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlowCron
metadata:
  name: daily-sync
spec:
  schedule: "0 2 * * *"  # 2 AM daily
  processor:
    # Standard DataFlow spec
  triggers:
    - name: notify-slack
      type: webhook
      config:
        url: https://hooks.slack.com/...
```

**Key Behaviors**:
- `processor` runs first (source → transformations → sink)
- `triggers` execute after successful processor completion
- Polling sources (PostgreSQL, Trino, ClickHouse, Nessie) can complete on "source exhausted"
- Kafka is a streaming source and runs indefinitely by default

---

## Prerequisites

- **Kubernetes** 1.24+
- **Helm** 3.0+ (for cluster deployment)
- **kubectl** (for management)
- **Go** 1.25+ (for local development)
- **Docker & docker-compose** (for local development and integration tests)

---

## Quick Start

### 1. Install via Helm

```bash
# Add the Helm repository
helm repo add dataflow-operator https://dataflow-operator.github.io/helm-charts
helm repo update

# Install the operator
helm install dataflow-operator dataflow-operator/dataflow-operator \
  --namespace dataflow-system \
  --create-namespace
```

### 2. Deploy Your First Pipeline

```bash
# Create a sample Kafka-to-PostgreSQL pipeline
kubectl apply -f - <<EOF
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: my-pipeline
  namespace: default
spec:
  source:
    type: kafka
    config:
      brokers:
        - kafka:9092
      topic: events
      consumerGroup: my-app
  sink:
    type: postgresql
    config:
      connectionString: "postgres://user:pass@postgres:5432/db"
      table: events
EOF

# Monitor the pipeline
kubectl logs -f deployment/my-pipeline-processor
```

### 3. Local Development Setup

```bash
# Start local infrastructure (Kafka, PostgreSQL, ClickHouse)
docker-compose up -d

# Available UIs:
# - Kafka UI: http://localhost:8080
# - ClickHouse: http://localhost:8123

# Run the operator locally
task run

# In another terminal, apply a sample configuration
kubectl apply -f config/samples/kafka-to-postgres.yaml
```

---

## Development

### Project Structure

```
dataflow-operator/dataflow/
├── api/v1/                     # Custom Resource Definitions
├── internal/
│   ├── controller/             # Reconciliation logic
│   ├── processor/              # Pipeline execution engine
│   ├── connectors/             # Source & sink implementations
│   ├── transformers/           # Transformation implementations
│   ├── checkpoint/             # State management
│   └── metrics/                # Prometheus instrumentation
├── cmd/
│   └── processor/              # Processor pod entrypoint
├── config/
│   ├── crd/                    # CRD manifests
│   ├── samples/                # Example pipelines
│   └── manager/                # Controller deployment
└── test/
    ├── integration/            # Integration tests with testcontainers
    └── unit/                   # Unit tests
```

### Common Development Commands

All commands use **[Task](https://taskfile.dev/)** for consistency:

```bash
# Code generation
task generate              # DeepCopy, CRD, and RBAC manifests
task manifests             # CRD and RBAC only

# Building
task build                 # Build operator binary → bin/manager
task run                   # Run operator locally with hot reload

# Testing
task test                  # Unit tests (requires envtest)
task test-unit             # Fast unit tests with statistics
task test-integration      # Integration tests (requires Docker)

# Code quality
task fmt                   # Format code
task vet                   # Run go vet

# Kubernetes deployment
task install               # Install CRDs into cluster
task deploy                # Deploy operator to cluster
```

### Code Generation

To regenerate manifests after modifying `api/` types:

```bash
task generate

# If controller-gen is missing:
go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest
task generate
```

### Testing Strategy

#### Unit Tests (Fast)
```bash
task test-unit
# No external dependencies, runs in ~30s
```

#### Integration Tests (Requires Docker)
```bash
task test-integration
# Spins up Kafka, PostgreSQL, ClickHouse containers
# Tests real connector implementations
# Expected runtime: 2-5 minutes
```

**Test Coverage**:
- ✅ Kafka source & sink connectors
- ✅ PostgreSQL source & sink connectors
- ✅ All transformer implementations (Filter, Mask, Flatten, etc.)
- ✅ Chained transformations
- ✅ Error handling and retries

### Building Docker Image

```bash
# Builds both operator and processor binaries
docker build -t dataflow:latest .
```

---

## Configuration Examples

Sample configurations are available in `config/samples/`:

| File | Description |
|------|-------------|
| `kafka-to-postgres.yaml` | Basic Kafka to PostgreSQL pipeline |
| `kafka-to-clickhouse.yaml` | Kafka to ClickHouse with aggregations |
| `kafka-to-trino.yaml` | Kafka to Trino with real-time analytics |
| `postgres-to-kafka-router.yaml` | PostgreSQL source with message routing |
| `clickhouse-to-clickhouse.yaml` | ClickHouse-to-ClickHouse replication |
| `dataflowcron-example.yaml` | Scheduled pipeline with post-processor triggers |

**Browse all examples**:
```bash
ls -la config/samples/
```

---

## Production Deployment

### Helm Configuration

```yaml
# values.yaml
replicaCount: 3  # HA setup

operator:
  resources:
    requests:
      memory: "256Mi"
      cpu: "100m"
    limits:
      memory: "512Mi"
      cpu: "500m"

processor:
  resources:
    requests:
      memory: "512Mi"
      cpu: "200m"
    limits:
      memory: "2Gi"
      cpu: "1000m"

# Enable webhook validation
webhooks:
  enabled: true

# Prometheus monitoring
prometheus:
  enabled: true
```

### High Availability

The operator supports multiple replicas with automatic leader election:

```bash
helm install dataflow-operator dataflow-operator/dataflow-operator \
  --set replicaCount=3 \
  --set operator.leaderElection.enabled=true
```

### Monitoring

The operator exposes Prometheus metrics on `:9090/metrics`:

```bash
kubectl port-forward svc/dataflow-operator 9090:9090
# Browse: http://localhost:9090/metrics
```

**Key Metrics**:
- `dataflow_processor_messages_processed_total` - Total messages processed
- `dataflow_processor_errors_total` - Total processing errors
- `dataflow_processor_latency_seconds` - End-to-end latency
- `dataflow_checkpoint_saves_total` - Checkpoint persistence

### Logging

Control log levels via environment variables:

```bash
# In Helm values
operator:
  env:
    - name: LOG_LEVEL
      value: "debug"  # debug, info, warn, error
```

---

## Troubleshooting

### Processor Pod Fails to Start

```bash
# Check operator logs
kubectl logs -n dataflow-system deployment/dataflow-operator

# Check processor pod logs
kubectl logs -l dataflow.io/name=<pipeline-name>

# Inspect DataFlow resource for validation errors
kubectl describe dataflow <pipeline-name>
```

### ClickHouse SESSION_IS_LOCKED Error

This occurs when the same session is used by concurrent queries. Solutions:

1. **Use separate connections** (recommended for DBeaver/DataGrip)
2. **Use HTTP instead of Native protocol** (port 8123)
3. **Increase session limits** in ClickHouse configuration

See `config/clickhouse/README.md` for detailed troubleshooting.

### Processor Restarting

Check for transient errors:

```bash
kubectl logs --tail=100 <processor-pod> | grep -i "transient\|timeout"
```

The processor has built-in retry logic for transient errors (timeouts, network blips).

---

## Contributing

We welcome contributions! Please:

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/my-feature`
3. Run tests: `task test-integration`
4. Format code: `task fmt`
5. Submit a pull request with clear description

### Development Workflow

```bash
# Setup
git clone https://github.com/dataflow-operator/dataflow.git
cd dataflow
docker-compose up -d
go mod download

# Make changes
# ... edit code ...

# Validate
task fmt && task vet && task test

# Generate manifests if you modified types
task generate

# Test locally
task run
# Apply test resources: kubectl apply -f config/samples/...
```

---

## License

Apache License 2.0 — See [LICENSE](LICENSE) file for details.

---

## Resources

- **Documentation**: https://dataflow-operator.github.io/docs/
- **GitHub Issues**: [Report bugs or request features](https://github.com/dataflow-operator/dataflow/issues)
- **Helm Charts**: https://github.com/dataflow-operator/helm-charts
- **Kubernetes Operator Pattern**: https://kubernetes.io/docs/concepts/extend-kubernetes/operator/

---

## Roadmap

- [ ] AWS S3 connector
- [ ] Google Cloud Storage (GCS) connector
- [ ] MongoDB source connector
- [ ] Real-time schema inference
- [ ] Web UI for pipeline management
- [ ] Advanced metrics and alerting integration

---

## Support

For questions and support:

- 📖 Check the [official documentation](https://dataflow-operator.github.io/docs/)
- 🐛 [Search existing issues](https://github.com/dataflow-operator/dataflow/issues)
- 💬 Open a GitHub discussion or issue
- 📧 For commercial support, contact the maintainers

---

**Made with ❤️ by the DataFlow Operator team**
