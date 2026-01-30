# DataFlow Operator

Kubernetes operator for streaming data between different sources (Kafka, PostgreSQL, Trino) with support for message transformations.

## Features

- **Multiple Data Sources**: Kafka, PostgreSQL, Trino
- **Message Transformations**:
  - Timestamp - add timestamp to messages
  - Flatten - expand arrays into separate messages
  - Filter - filter by conditions
  - Mask - mask sensitive data
  - Router - route to different sinks
  - Select - select specific fields
  - Remove - remove fields
  - SnakeCase - convert field names to snake_case
  - CamelCase - convert field names to CamelCase
- **Kubernetes Secrets Support**: Configure connectors using `SecretRef` for secure credential management
- **Per-Resource Pod Deployment**: Each DataFlow resource creates a separate pod (Deployment) for processing
- **Resource Management**: Configure CPU and memory resources for processor pods
- **Pod Placement Control**: Configure nodeSelector, affinity, and tolerations for fine-grained pod placement

## Quick Start

### Prerequisites

- Kubernetes 1.24+
- Helm 3.0+
- kubectl
- Go 1.21+ (for local development)
- Docker and docker-compose (for local development)

### Installation

#### Installing CRD

Before installing the operator, you need to install the Custom Resource Definition (CRD):

```bash
kubectl apply -f https://raw.githubusercontent.com/ilyario/dataflow/refs/heads/main/config/crd/bases/dataflow.dataflow.io_dataflows.yaml
```

Or use a local file:

```bash
kubectl apply -f config/crd/bases/dataflow.dataflow.io_dataflows.yaml
```

#### Installation via Helm (Recommended)

1. Install the operator from OCI registry:
```bash
helm install dataflow-operator oci://ghcr.io/ilyario/helm-charts/dataflow-operator
```

2. For installation with custom settings:
```bash
helm install dataflow-operator oci://ghcr.io/ilyario/helm-charts/dataflow-operator \
  --set image.repository=your-registry/controller \
  --set image.tag=v1.0.0 \
  --set replicaCount=2
```

3. For installation in a specific namespace:
```bash
helm install dataflow-operator oci://ghcr.io/ilyario/helm-charts/dataflow-operator \
  --namespace dataflow-system \
  --create-namespace
```

4. Check installation status:
```bash
kubectl get pods -l app.kubernetes.io/name=dataflow-operator
```

**Note**: For local development, you can also use the local chart:
```bash
helm install dataflow-operator ./helm/dataflow-operator
```

#### Updating

```bash
helm upgrade dataflow-operator oci://ghcr.io/ilyario/helm-charts/dataflow-operator
```

#### Uninstallation

```bash
helm uninstall dataflow-operator
```

#### Local Development

For local development, you can run the operator locally:
```bash
make run
```

Or use the script:
```bash
./scripts/run-local.sh
```

### Local Development Setup

1. Start dependencies with UI interfaces:
```bash
docker-compose up -d
```

Available UIs:
- **Kafka UI**: http://localhost:8080
- **pgAdmin**: http://localhost:5050 (admin@admin.com / admin)

2. Run the operator:
```bash
make run
```

### Usage Examples

See `config/samples/` for CRD manifest examples.

#### Example with Kubernetes Secrets

For secure credential storage, use Kubernetes Secrets:

```bash
kubectl apply -f config/samples/kafka-to-postgres-secrets.yaml
```

This example demonstrates using `SecretRef` for connector configuration. All connectors support configuration from Kubernetes Secrets.

## Project Structure

```
dataflow/
├── api/v1/              # CRD definitions
├── internal/
│   ├── connectors/      # Connectors for sources/sinks
│   ├── transformers/    # Message transformations
│   ├── processor/       # Message processor
│   └── controller/      # Kubernetes controller
├── helm/dataflow-operator/  # Helm Chart for installation
├── config/samples/      # CRD examples
├── docs/                # MkDocs documentation
├── test/                # Tests and utilities
└── scripts/             # Helper scripts
```

## Documentation

Full documentation is available in `docs/`. To view:

```bash
mkdocs serve
```

Or from the project root:

```bash
cd docs && mkdocs serve
```

Documentation is available in two languages:
- **English**: Default language
- **Russian**: Available in the navigation menu

## Development

### Code Generation

If you encounter issues with `make generate`, try:

```bash
# Update controller-gen
go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest

# Then
make generate
```

### Testing

```bash
# Unit tests
make test

# Integration tests (requires kind)
./scripts/setup-kind.sh
make test-integration
```

## Web GUI

DataFlow включает веб-интерфейс для управления манифестами, просмотра логов и мониторинга метрик.

### Запуск GUI сервера

```bash
go run cmd/gui-server/main.go --bind-address :8080
```

Или с параметрами:

```bash
go run cmd/gui-server/main.go \
  --bind-address :8080 \
  --kubeconfig ~/.kube/config \
  --log-level info
```

### Использование

1. Откройте браузер и перейдите на `http://localhost:8080`
2. Используйте вкладки для:
   - **Манифесты**: Управление DataFlow ресурсами (создание, просмотр, обновление, удаление)
   - **Логи**: Просмотр логов обработки в реальном времени
   - **Метрики**: Мониторинг статуса обработки, количества обработанных сообщений и ошибок

Подробнее см. [cmd/gui-server/README.md](cmd/gui-server/README.md)

## Security

DataFlow Operator supports configuring connectors from Kubernetes Secrets through `SecretRef` fields. This allows:

- Secure storage of sensitive data (passwords, tokens, connection strings)
- Centralized credential management
- Secret rotation without changing DataFlow resources
- Access control through Kubernetes RBAC

See the [Connectors documentation](docs/en/connectors.md#using-kubernetes-secrets) for details.

## License

Apache License 2.0
