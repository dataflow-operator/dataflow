# Examples

Practical examples of using DataFlow Operator for various data processing scenarios.

> **Note**: This is a simplified English version. For complete documentation, see the [Russian version](../ru/examples.md).

## Simple Kafka → PostgreSQL Flow

Basic example of transferring data from a Kafka topic to a PostgreSQL table.

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-to-postgres
spec:
  source:
    type: kafka
    kafka:
      brokers:
        - localhost:9092
      topic: input-topic
      consumerGroup: dataflow-group
  sink:
    type: postgresql
    postgresql:
      connectionString: "postgres://dataflow:dataflow@postgres:5432/dataflow?sslmode=disable"
      table: output_table
      autoCreateTable: true
```

**Apply:**
```bash
kubectl apply -f config/samples/kafka-to-postgres.yaml
```

## Error Handling with Error Sink

Example of configuring a separate sink for messages that failed to be written to the main sink.

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-to-postgres-with-errors
spec:
  source:
    type: kafka
    kafka:
      brokers:
        - localhost:9092
      topic: input-topic
      consumerGroup: dataflow-group
  sink:
    type: postgresql
    postgresql:
      connectionString: "postgres://dataflow:dataflow@postgres:5432/dataflow?sslmode=disable"
      table: output_table
      autoCreateTable: true
  errors:
    type: kafka
    kafka:
      brokers:
        - localhost:9092
      topic: error-topic
```

**Error Message Structure:**

When a message fails to be written to the main sink, it is sent to the error sink with the following structure:

```json
{
  "error": {
    "message": "error text (e.g., failed to send message: connection refused)",
    "timestamp": "2026-01-24T12:34:56Z",
    "original_sink": "postgresql",
    "metadata": {
      // Metadata from the original message (if present)
    }
  },
  "original_message": {
    // Original message data
    // If the original message was JSON, it will be here as an object
    // If not - there will be an "original_data" field with a string
  }
}
```

**Example Error Message:**

If the original message was:
```json
{
  "id": 1,
  "name": "test",
  "value": 100
}
```

Then the error sink will contain:
```json
{
  "error": {
    "message": "failed to send message: connection refused",
    "timestamp": "2026-01-24T12:34:56Z",
    "original_sink": "postgresql"
  },
  "original_message": {
    "id": 1,
    "name": "test",
    "value": 100
  }
}
```

**Important:**
- If `errors` is not specified, write errors will cause processing to stop
- Error sink can be of any type (Kafka, PostgreSQL, Trino)
- Original message data is preserved in the `original_message` field
- Error information is embedded in the message structure, ensuring it is preserved regardless of the error sink type

**Apply:**
```bash
kubectl apply -f config/samples/kafka-to-postgres-with-errors.yaml
```

## PostgreSQL → PostgreSQL (replication / ETL)

Example of reading data from one PostgreSQL database and writing transformed data into another PostgreSQL database.

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: postgres-to-postgres
spec:
  source:
    type: postgresql
    postgresql:
      connectionString: "postgres://dataflow:dataflow@source-postgres:5432/source_db?sslmode=disable"
      table: source_orders
      query: "SELECT * FROM source_orders WHERE updated_at > NOW() - INTERVAL '5 minutes'"
      pollInterval: 60
  sink:
    type: postgresql
    postgresql:
      connectionString: "postgres://dataflow:dataflow@target-postgres:5432/target_db?sslmode=disable"
      table: target_orders
      autoCreateTable: true
      batchSize: 100
      upsertMode: true  # Enables updating existing records instead of skipping them
  transformations:
    # Keep only required fields
    - type: select
      select:
        fields:
          - id
          - customer_id
          - total
          - status
          - updated_at
    # Add sync time
    - type: timestamp
      timestamp:
        fieldName: synced_at
```

**Typical use cases:**

- **Online replication**: periodically copying updated rows from OLTP database to analytics database
- **ETL pipeline**: cleaning and reshaping data when moving between PostgreSQL schemas/clusters

**Important:** With `upsertMode: true`, existing records in the target table will be updated on conflict with PRIMARY KEY (or specified `conflictKey`). Without `upsertMode`, updated records from the source will be skipped if they already exist in the target table.

## Using Secrets for Credentials

DataFlow Operator supports configuring connectors from Kubernetes Secrets through `*SecretRef` fields. This allows secure storage of credentials without explicitly specifying them in the DataFlow specification.

### Example: Kafka → PostgreSQL with Secrets

#### Step 1: Create Secrets

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-credentials
  namespace: default
type: Opaque
stringData:
  brokers: "kafka1:9092,kafka2:9092"
  topic: "input-topic"
  consumerGroup: "dataflow-group"
  username: "kafka-user"
  password: "kafka-password"
---
apiVersion: v1
kind: Secret
metadata:
  name: postgres-credentials
  namespace: default
type: Opaque
stringData:
  connectionString: "postgres://user:password@postgres:5432/dbname?sslmode=disable"
  table: "output_table"
```

#### Step 2: DataFlow with SecretRef

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: secure-dataflow
spec:
  source:
    type: kafka
    kafka:
      brokersSecretRef:
        name: kafka-credentials
        key: brokers
      topicSecretRef:
        name: kafka-credentials
        key: topic
      consumerGroupSecretRef:
        name: kafka-credentials
        key: consumerGroup
      sasl:
        mechanism: scram-sha-256
        usernameSecretRef:
          name: kafka-credentials
          key: username
        passwordSecretRef:
          name: kafka-credentials
          key: password
  sink:
    type: postgresql
    postgresql:
      connectionStringSecretRef:
        name: postgres-credentials
        key: connectionString
      tableSecretRef:
        name: postgres-credentials
        key: table
      autoCreateTable: true
```

**Apply:**
```bash
kubectl apply -f config/samples/kafka-to-postgres-secrets.yaml
```

### Example: TLS Certificates from Secrets

For TLS configuration, the operator automatically determines whether the value from the secret is a file path or certificate content.

**How it works:**
- If the value starts with `-----BEGIN` (e.g., `-----BEGIN CERTIFICATE-----`), the operator recognizes it as certificate content and creates a temporary file
- If the value doesn't start with `-----BEGIN` and exists as a file, it's used as a file path
- Certificates can be stored in secrets either as plain text (PEM format) or as base64-encoded values (in the secret's `data` field)

**Supported formats:**

1. **Certificate content** (PEM format):
   ```yaml
   ca.crt: |
     -----BEGIN CERTIFICATE-----
     MIIDXTCCAkWgAwIBAgIJAK...
     -----END CERTIFICATE-----
   ```

2. **Base64-encoded content** (in secret's `data` field):
   ```yaml
   apiVersion: v1
   kind: Secret
   metadata:
     name: kafka-tls-certs
   type: Opaque
   data:
     ca.crt: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0t...  # base64
   ```
   Kubernetes automatically decodes base64 when reading from the secret.

3. **File path**:
   ```yaml
   ca.crt: /etc/kafka/ca.crt
   ```

**Example:**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-tls-certs
type: Opaque
stringData:
  ca.crt: |
    -----BEGIN CERTIFICATE-----
    ...
    -----END CERTIFICATE-----
---
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-tls-secure
spec:
  source:
    type: kafka
    kafka:
      brokers:
        - secure-kafka:9093
      topic: secure-topic
      tls:
        caSecretRef:
          name: kafka-tls-certs
          key: ca.crt
```

**Important:**
- Temporary files are automatically created for certificate content and cleaned up after use
- When using base64-encoded values in the `data` field, Kubernetes automatically decodes them when reading
- Ensure certificates are in proper PEM format with `-----BEGIN` and `-----END` headers

### Benefits of Using SecretRef

- **Security**: Credentials are not stored in the DataFlow specification
- **Management**: Centralized credential management through Kubernetes
- **Rotation**: Update secrets without changing DataFlow resources
- **RBAC**: Access control through Kubernetes RBAC

For more details, see the [Using Kubernetes Secrets](../ru/connectors.md#использование-secrets-в-kubernetes) section in the connectors documentation.

## Configuring Pod Resources and Placement

Each DataFlow resource creates a separate pod (Deployment) for processing. You can configure resources, node selection, affinity, and tolerations for these pods.

### Example: Custom Resources and Node Selection

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-to-postgres-with-resources
spec:
  source:
    type: kafka
    kafka:
      brokers:
        - localhost:9092
      topic: input-topic
      consumerGroup: dataflow-group
  sink:
    type: postgresql
    postgresql:
      connectionString: "postgres://dataflow:dataflow@postgres:5432/dataflow?sslmode=disable"
      table: output_table
  # Configure resources for the processor pod
  resources:
    requests:
      cpu: "200m"
      memory: "256Mi"
    limits:
      cpu: "1000m"
      memory: "1Gi"
  # Select nodes for pod placement
  nodeSelector:
    node-type: compute
    zone: us-east-1
  # Affinity rules for more precise placement control
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: kubernetes.io/arch
            operator: In
            values:
            - amd64
      preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        preference:
          matchExpressions:
          - key: node-type
            operator: In
            values:
            - compute
    podAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 50
        podAffinityTerm:
          labelSelector:
            matchExpressions:
            - key: app
              operator: In
              values:
              - dataflow-processor
          topologyKey: kubernetes.io/hostname
  # Tolerations for working with tainted nodes
  tolerations:
  - key: dedicated
    operator: Equal
    value: dataflow
    effect: NoSchedule
  - key: workload-type
    operator: Equal
    value: batch
    effect: NoSchedule
```

**Apply:**
```bash
kubectl apply -f config/samples/kafka-to-postgres-with-resources.yaml
```

### Resource Configuration

- **resources**: Defines CPU and memory requests and limits for the processor pod
  - If not specified, defaults are used: `100m` CPU / `128Mi` memory (requests), `500m` CPU / `512Mi` memory (limits)
  - Use this to ensure pods have sufficient resources for high-throughput processing

### Node Selection

- **nodeSelector**: Simple key-value pairs to select specific nodes
  - Example: `node-type: compute` ensures pods run only on nodes labeled with `node-type=compute`

### Affinity Rules

- **affinity**: Advanced placement rules using Kubernetes affinity
  - **nodeAffinity**: Control which nodes pods can run on
  - **podAffinity**: Prefer to run pods near other pods (e.g., other dataflow processors)
  - **podAntiAffinity**: Avoid running pods near other pods (e.g., spread across nodes)

### Tolerations

- **tolerations**: Allow pods to run on tainted nodes
  - Useful for dedicated compute nodes or specialized hardware
  - Example: Run dataflow processors on nodes dedicated to batch workloads

### Default Behavior

If resources, nodeSelector, affinity, or tolerations are not specified:
- Default resources are applied (100m CPU / 128Mi memory requests, 500m CPU / 512Mi memory limits)
- Pods can run on any node (no nodeSelector)
- No affinity rules are applied
- Pods cannot run on tainted nodes (no tolerations)

### Checking Pod Status

After creating a DataFlow with custom resources, check the pod:

```bash
# List pods created by DataFlow
kubectl get pods -l app=dataflow-processor

# Describe a specific pod
kubectl describe pod dataflow-<name>-<hash>

# Check resource usage
kubectl top pod dataflow-<name>-<hash>
```

For more examples, see the [Russian version](../ru/examples.md) or check `config/samples/` directory.

