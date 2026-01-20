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

For more examples, see the [Russian version](../ru/examples.md) or check `config/samples/` directory.

