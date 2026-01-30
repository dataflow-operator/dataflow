# Getting Started

This guide will help you get started with DataFlow Operator. You'll learn how to install the operator, create your first data flow, and set up a local development environment.

## Prerequisites

### For Production Installation

- Kubernetes cluster (version 1.24+)
- Helm 3.0+
- kubectl configured to work with the cluster
- Access to data sources (Kafka, PostgreSQL)

### For Local Development

- Go 1.21+
- Docker and Docker Compose
- Make (optional, for using Makefile commands)
- Access to ports: 8080, 5050, 15672, 8081, 5432, 9092, 5672

## Installation

### Installing CRD

Before installing the operator, you need to install the Custom Resource Definition (CRD):

```bash
kubectl apply -f https://raw.githubusercontent.com/ilyario/dataflow/refs/heads/main/config/crd/bases/dataflow.dataflow.io_dataflows.yaml
```

Or use a local file:

```bash
kubectl apply -f config/crd/bases/dataflow.dataflow.io_dataflows.yaml
```

### Installation via Helm (Recommended)

#### Basic Installation

The simplest way to install the operator from OCI registry:

```bash
helm install dataflow-operator oci://ghcr.io/ilyario/helm-charts/dataflow-operator
```

This command will install the operator with default settings in the `default` namespace.

#### Installation in a Specific Namespace

```bash
helm install dataflow-operator oci://ghcr.io/ilyario/helm-charts/dataflow-operator \
  --namespace dataflow-system \
  --create-namespace
```

**Note**: For local development, you can also use the local chart:
```bash
helm install dataflow-operator ./helm/dataflow-operator
```

#### Installation with Custom Settings

You can override default values via flags:

```bash
helm install dataflow-operator oci://ghcr.io/ilyario/helm-charts/dataflow-operator \
  --set image.repository=your-registry/controller \
  --set image.tag=v1.0.0 \
  --set replicaCount=2 \
  --set leaderElection.enabled=true \
  --set resources.limits.memory=1Gi \
  --set resources.limits.cpu=500m \
  --set resources.requests.memory=256Mi \
  --set resources.requests.cpu=100m
```

#### Installation with Values File

For more complex configurations, create a `my-values.yaml` file:

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

# Settings for working with Kubernetes API
serviceAccount:
  create: true
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/dataflow-operator

# Security settings
securityContext:
  runAsNonRoot: true
  runAsUser: 1000
  fsGroup: 1000
```

Then install:

```bash
helm install dataflow-operator oci://ghcr.io/ilyario/helm-charts/dataflow-operator -f my-values.yaml
```

#### Verification

After installation, check the status:

```bash
# Check pod status
kubectl get pods -l app.kubernetes.io/name=dataflow-operator

# Check CRD
kubectl get crd dataflows.dataflow.dataflow.io

# Check operator logs
kubectl logs -l app.kubernetes.io/name=dataflow-operator --tail=50

# Check deployment status
kubectl get deployment dataflow-operator
```

Expected output:

```
NAME                                  READY   STATUS    RESTARTS   AGE
dataflow-operator-7d8f9c4b5d-xxxxx   1/1     Running   0          1m
```

### Updating

To update the operator to a new version:

```bash
helm upgrade dataflow-operator oci://ghcr.io/ilyario/helm-charts/dataflow-operator
```

With custom values:

```bash
helm upgrade dataflow-operator oci://ghcr.io/ilyario/helm-charts/dataflow-operator -f my-values.yaml
```

To update to a specific version:

```bash
helm upgrade dataflow-operator oci://ghcr.io/ilyario/helm-charts/dataflow-operator \
  --set image.tag=v1.1.0
```

### Uninstallation

To uninstall the operator:

```bash
helm uninstall dataflow-operator
```

**Warning**: Uninstalling the operator does not delete created `DataFlow` resources, but they will stop being processed. If you want to delete all resources:

```bash
# Delete all DataFlow resources
kubectl delete dataflow --all

# Then uninstall the operator
helm uninstall dataflow-operator
```

## First DataFlow

### Simple Example: Kafka → PostgreSQL

Create a simple DataFlow resource to transfer data from Kafka to PostgreSQL:

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

Apply the resource:

```bash
kubectl apply -f config/samples/kafka-to-postgres.yaml
```

**Note**: Each DataFlow resource creates a separate pod (Deployment) for processing. You can configure resources, node selection, affinity, and tolerations. See [Examples](examples.md#configuring-pod-resources-and-placement) for details.

### Example with Kubernetes Secrets

For secure credential storage, use Kubernetes Secrets. See example:

```bash
kubectl apply -f config/samples/kafka-to-postgres-secrets.yaml
```

This example demonstrates using `SecretRef` for connector configuration. For more details, see the [Using Kubernetes Secrets](connectors.md#using-kubernetes-secrets) section in the connectors documentation.

### Checking Status

Check the status of the created data flow:

```bash
# Get DataFlow information
kubectl get dataflow kafka-to-postgres

# Detailed information
kubectl describe dataflow kafka-to-postgres

# View status in YAML format
kubectl get dataflow kafka-to-postgres -o yaml
```

Expected status:

```yaml
status:
  phase: Running
  processedCount: 150
  errorCount: 0
  lastProcessedTime: "2024-01-15T10:30:00Z"
  message: "Processing messages successfully"
```

### Sending Test Message

To test the data flow, send a message to the Kafka topic:

```bash
# Using kafka-console-producer
kafka-console-producer --broker-list localhost:9092 --topic input-topic
# Enter JSON message and press Enter
{"id": 1, "name": "Test", "value": 100}
```

Or use the project script:

```bash
./scripts/send-test-message.sh
```

### Checking Data in PostgreSQL

Connect to PostgreSQL and check the data:

```bash
psql postgres://user:password@postgres-host:5432/dbname

# Check the table
SELECT * FROM output_table;
```

## Local Development

### Starting Dependencies

Use docker-compose to start all dependencies locally:

```bash
docker-compose up -d
```

This command will start:

- **Kafka** (port 9092) with Kafka UI (port 8080)
- **PostgreSQL** (port 5432) with pgAdmin (port 5050)

### Accessing UI Interfaces

After starting, the following UIs are available:

- **Kafka UI**: http://localhost:8080
  - View topics, messages, consumer groups
- **pgAdmin**: http://localhost:5050
  - Login: `admin@admin.com`, password: `admin`
  - PostgreSQL database management
  - Queue and exchange management

### Running Operator Locally

For development, run the operator locally:

```bash
# Install CRD in cluster (if using kind or minikube)
make install

# Run the operator
make run
```

Or use the script:

```bash
./scripts/run-local.sh
```

### Setting Up Local Cluster (Optional)

For full testing, use kind (Kubernetes in Docker):

```bash
# Create kind cluster
./scripts/setup-kind.sh

# Install CRD
make install

# Run operator locally
make run
```

### Debugging

For debugging, use operator logs:

```bash
# If operator is running locally, logs are output to console
# For operator in cluster:
kubectl logs -l app.kubernetes.io/name=dataflow-operator -f
```

Check Kubernetes events:

```bash
kubectl get events --sort-by='.lastTimestamp' | grep dataflow
```

## Next Steps

Now that you've installed the operator and created your first data flow:

1. Study [Connectors](connectors.md) to understand all available sources and sinks
2. Familiarize yourself with [Transformations](transformations.md) for working with message transformations
3. Check out [Examples](examples.md) for practical usage examples
4. Read [Development](development.md) to participate in development

## Troubleshooting

### Operator Not Starting

```bash
# Check logs
kubectl logs -l app.kubernetes.io/name=dataflow-operator

# Check events
kubectl describe pod -l app.kubernetes.io/name=dataflow-operator

# Check CRD
kubectl get crd dataflows.dataflow.dataflow.io -o yaml
```

### DataFlow Not Processing Messages

1. Check DataFlow status:
   ```bash
   kubectl describe dataflow <name>
   ```

2. Check connection to data source:
   ```bash
   # For Kafka
   kafka-console-consumer --bootstrap-server localhost:9092 --topic <topic>

   # For PostgreSQL
   psql <connection-string> -c "SELECT * FROM <table> LIMIT 10;"
   ```

3. Check operator logs for errors

### Connection Issues

- Ensure data sources are accessible from the cluster
- Check Kubernetes network policies
- Verify connection strings and credentials are correct
- For local development, use `localhost` or `host.docker.internal`
