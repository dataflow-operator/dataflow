# DataFlow Operator

A production-ready Kubernetes operator for streaming data pipelines with support for multiple sources, sinks, and comprehensive message transformations.

📖 **[Full Documentation](https://dataflow-operator.github.io/docs/)**

---

## Overview

DataFlow Operator automates the deployment and management of data streaming pipelines in Kubernetes. It watches custom `DataFlow` resources and orchestrates processor pods that read from sources, apply optional transformations, and write to sinks. The operator handles fault tolerance, checkpointing, scheduling, and comprehensive monitoring out of the box.

### Key Features

- **Multi-Source/Sink Support**: Kafka, PostgreSQL, ClickHouse, Trino, and Apache Iceberg (Nessie). Securely configure connectors using Kubernetes Secrets via `SecretRef` fields.
- **Rich Transformations**: Filter, Select, Remove, Mask, Flatten, Timestamp, SnakeCase, Router, Chain.
- **Web GUI**: Built-in browser-based interface to manage manifests, stream logs, and monitor metrics without `kubectl`.
- **MCP Server**: Model Context Protocol server for AI agents to validate, generate, and migrate DataFlow manifests.
- **Agent Skills**: Portable AI guides ([github.com/dataflow-operator/skills](https://github.com/dataflow-operator/skills), [docs](https://dataflow-operator.github.io/docs/agent-skills/)).
- **Error Handling**: Route failed messages to a dedicated error sink (e.g., Dead Letter Queue) with full error context and original payloads to prevent data loss.
- **Fault Tolerance**: At-least-once delivery semantics with checkpoint persistence, graceful shutdown, and support for idempotent sinks (e.g., UPSERT, ReplacingMergeTree) to safely handle duplicates.
- **Scheduled Pipelines**: `DataFlowCron` for time-based pipeline execution with triggers.
- **High Availability**: Leader election for multi-replica deployments.
- **Observable**: Prometheus metrics, structured logging, Sentry integration, health probes, and native Kubernetes Events for lifecycle audit trails.
- **Kubernetes Native**: Custom Resource Definitions, RBAC, Helm charts.

---

## Architecture

### How It Works

1. **Operator Controller**: Watches `DataFlow` and `DataFlowCron` resources in your cluster.
2. **Processor Pod**: Creates ephemeral or long-running pods that execute the data pipeline.
3. **Pipeline Execution**: `source → transformations → sink` flow with built-in error handling and error sink routing.
4. **State Management**: Stores checkpoint data in ConfigMaps (or native offsets for Kafka) for recovery.
5. **Observability**: Emits standard Kubernetes Events (`Normal`/`Warning`) for reconciliation lifecycle auditing.

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
        - kafka-broker:9092      topic: input-events
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
      connectionStringSecretRef: 
        name: db-credentials
        key: url
      table: events

  # Error sink catches failed writes (e.g. constraint violations) and saves them for replay
  errors:
    type: postgresql
    config:
      connectionStringSecretRef: 
        name: db-credentials
        key: url
      table: events_dead_letter
      autoCreateTable: true
```

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

# Monitor the pipeline status and Kubernetes events
kubectl describe dataflow my-pipeline
kubectl get events --watch
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

## Resources

- **Documentation**: https://dataflow-operator.github.io/docs/- **GitHub Issues**: Report bugs or request features
- **Helm Charts**: https://github.com/dataflow-operator/helm-charts
- **Kubernetes Operator Pattern**: https://kubernetes.io/docs/concepts/extend-kubernetes/operator/