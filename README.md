# DataFlow Operator

Kubernetes operator for streaming data between different sources (Kafka, PostgreSQL, Trino) with support for message transformations.

**[Online documentation](https://dataflow-operator.github.io/docs/)**

## Quick Start

### Prerequisites

- Kubernetes 1.24+
- Helm 3.0+
- kubectl
- Go 1.21+ (for local development)
- Docker and docker-compose (for local development)

#### Local Development

For local development, you can run the operator locally:
```bash
task run
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
task run
```

## Development

### Code Generation

If you encounter issues with `task generate`, try:

```bash
# Update controller-gen
go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest

# Then
task generate
```

### Testing

```bash
# Unit tests
task test

# Integration tests (requires kind)
./scripts/setup-kind.sh
task test-integration
```

## License

Apache License 2.0
