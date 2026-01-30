# Connectors

DataFlow Operator supports various connectors for data sources and sinks. Each connector implements a standard interface and can be used as a data source (source) or sink.

## Connector Overview

| Connector | Source | Sink | Features |
|-----------|--------|------|----------|
| Kafka | ✅ | ✅ | Consumer groups, TLS, SASL, Avro, Schema Registry |
| PostgreSQL | ✅ | ✅ | SQL queries, batch inserts, auto-create tables, UPSERT mode |
| Trino | ✅ | ✅ | SQL queries, Keycloak OAuth2 authentication, batch inserts |

> **Note**: This is a simplified English version. For complete documentation, see the [Russian version](../ru/connectors.md) or refer to the code examples in `config/samples/`.

## Using Kubernetes Secrets

DataFlow Operator supports configuring connectors from Kubernetes Secrets. This allows secure storage of sensitive data (passwords, tokens, connection strings) without explicitly specifying them in the DataFlow specification.

### Overview

Instead of specifying values directly in the configuration, you can use references to Kubernetes Secrets through `*SecretRef` fields. The operator automatically reads values from secrets and substitutes them into connector configuration.

### SecretRef Structure

Each secret reference has the following structure:

```yaml
secretRef:
  name: my-secret          # Secret name (required)
  namespace: my-namespace  # Secret namespace (optional, defaults to DataFlow namespace)
  key: my-key              # Key in secret (required)
```

### Supported Fields

All connectors support secret references for the following fields:

#### Kafka
- `brokersSecretRef` - broker list (comma-separated)
- `topicSecretRef` - topic name
- `consumerGroupSecretRef` - consumer group
- `sasl.usernameSecretRef` - SASL username
- `sasl.passwordSecretRef` - SASL password
- `tls.certSecretRef` - client certificate
- `tls.keySecretRef` - private key
- `tls.caSecretRef` - CA certificate
- `schemaRegistry.urlSecretRef` - Schema Registry URL
- `schemaRegistry.basicAuth.usernameSecretRef` - Schema Registry username
- `schemaRegistry.basicAuth.passwordSecretRef` - Schema Registry password
- `avroSchemaSecretRef` - Avro schema from secret (for static schema)

#### PostgreSQL
- `connectionStringSecretRef` - connection string
- `tableSecretRef` - table name

#### Trino
- `serverURLSecretRef` - Trino server URL
- `catalogSecretRef` - catalog name
- `schemaSecretRef` - schema name
- `tableSecretRef` - table name
- `keycloak.serverURLSecretRef` - Keycloak server URL
- `keycloak.realmSecretRef` - Keycloak realm name
- `keycloak.clientIDSecretRef` - OAuth2 client ID
- `keycloak.clientSecretSecretRef` - OAuth2 client secret
- `keycloak.usernameSecretRef` - username for password grant
- `keycloak.passwordSecretRef` - password for password grant
- `keycloak.tokenSecretRef` - OAuth2 token (for long-lived tokens)

### Usage Examples

#### Example 1: Kafka with SASL Authentication

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-credentials
  namespace: default
type: Opaque
stringData:
  brokers: "kafka1:9092,kafka2:9092,kafka3:9092"
  topic: "input-topic"
  consumerGroup: "dataflow-group"
  username: "kafka-user"
  password: "kafka-password"
---
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-example
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
        mechanism: "scram-sha-256"
        usernameSecretRef:
          name: kafka-credentials
          key: username
        passwordSecretRef:
          name: kafka-credentials
          key: password
```

#### Example 2: PostgreSQL with Connection String from Secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: postgres-credentials
  namespace: default
type: Opaque
stringData:
  connectionString: "postgres://user:password@postgres:5432/dbname?sslmode=disable"
  table: "output_table"
---
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: postgres-example
spec:
  source:
    type: postgresql
    postgresql:
      connectionStringSecretRef:
        name: postgres-credentials
        key: connectionString
      tableSecretRef:
        name: postgres-credentials
        key: table
```

### Priority of Values

If both a direct value and `SecretRef` are specified, `SecretRef` takes priority. The value from the secret will be used instead of the direct value.

### Secrets in Different Namespaces

By default, the operator looks for secrets in the same namespace where the DataFlow resource is located. You can specify a different namespace:

```yaml
connectionStringSecretRef:
  name: postgres-credentials
  namespace: shared-secrets
  key: connectionString
```

### TLS Certificates from Secrets

For TLS configuration, the operator automatically determines whether the value from the secret is a file path or certificate content.

**How it works:**
- If the value starts with `-----BEGIN` (e.g., `-----BEGIN CERTIFICATE-----` or `-----BEGIN PRIVATE KEY-----`), the operator recognizes it as certificate content and creates a temporary file
- If the value doesn't start with `-----BEGIN` and exists as a file, it's used as a file path
- If the value doesn't start with `-----BEGIN` and the file doesn't exist, it's also treated as certificate content

**Supported formats:**
1. **Certificate content** (PEM format) - stored in `stringData` or decoded from `data`
2. **Base64-encoded content** - stored in secret's `data` field (Kubernetes automatically decodes it)
3. **File path** - path to an existing certificate file

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
  client.crt: |
    -----BEGIN CERTIFICATE-----
    ...
    -----END CERTIFICATE-----
  client.key: |
    -----BEGIN PRIVATE KEY-----
    ...
    -----END PRIVATE KEY-----
---
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-tls-example
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
        certSecretRef:
          name: kafka-tls-certs
          key: client.crt
        keySecretRef:
          name: kafka-tls-certs
          key: client.key
```

**Important:**
- Temporary files are automatically created for certificate content and cleaned up after use
- When using base64-encoded values in the `data` field, Kubernetes automatically decodes them when reading
- Ensure certificates are in proper PEM format with `-----BEGIN` and `-----END` headers

### Security

- **RBAC**: The operator requires permissions to read secrets (`get`, `list`, `watch`)
- **Isolation**: Secrets should be in the same namespace or in a namespace the operator has access to
- **Temporary Files**: For TLS certificates, the operator creates temporary files that are automatically deleted

### Troubleshooting

#### Secret Not Found

```bash
# Check if secret exists
kubectl get secret <secret-name> -n <namespace>

# Check operator permissions
kubectl auth can-i get secrets --as=system:serviceaccount:default:dataflow-operator
```

#### Key Not Found in Secret

```bash
# Check keys in secret
kubectl get secret <secret-name> -n <namespace> -o jsonpath='{.data}' | jq 'keys'
```

#### Secret Resolution Error

Check operator logs:

```bash
kubectl logs -l app.kubernetes.io/name=dataflow-operator | grep -i secret
```

Ensure:
1. Secret exists in the specified namespace
2. The specified key exists in the secret
3. The operator has permissions to read secrets

#### TLS Certificate Issues

If you encounter errors with TLS certificates:

1. **"file name too long" error**: Ensure the certificate is stored correctly in the secret:
   - If using `stringData`, the certificate should be in PEM format with `-----BEGIN` and `-----END` headers
   - If using `data` (base64), ensure the value is correctly encoded
   - The operator automatically detects certificate content by the `-----BEGIN` prefix

2. **"failed to read CA file" error**: Check the certificate format:
   ```bash
   # Check secret content
   kubectl get secret <secret-name> -n <namespace> -o jsonpath='{.data.ca\.crt}' | base64 -d
   ```
   Ensure the certificate starts with `-----BEGIN CERTIFICATE-----`

3. **Temporary file creation error**: Check operator permissions to create files in the temporary directory

## Kafka

The Kafka connector supports reading and writing messages from/to Apache Kafka topics. It supports consumer groups for scaling, TLS and SASL authentication, as well as Avro format through Confluent Schema Registry or static schema.

### Source

```yaml
source:
  type: kafka
  kafka:
    brokers:
      - kafka1:9092
    topic: input-topic
    consumerGroup: my-group

    # Message format (optional, default: "json")
    # Supported formats: "json", "avro"
    format: json

    # Avro configuration (required if format: "avro")
    # Option 1: Use Confluent Schema Registry (recommended)
    schemaRegistry:
      url: https://schema-registry:8081
      basicAuth:
        username: schema-user
        password: schema-password
      tls:
        insecureSkipVerify: false
        caFile: /path/to/schema-registry-ca.crt

    # Option 2: Static Avro schema (alternative to Schema Registry)
    # avroSchema: |
    #   {
    #     "type": "record",
    #     "name": "MyRecord",
    #     "fields": [
    #       {"name": "id", "type": "long"},
    #       {"name": "name", "type": "string"}
    #     ]
    #   }
    # Or path to schema file:
    # avroSchemaFile: /path/to/schema.avsc
```

### Features

- **Consumer Groups**: Use different consumer groups for scaling processing
- **Initial Offset**: Reads from oldest message by default (`OffsetOldest`)
- **Data Formats**: Supports JSON (default) and Avro formats
- **Avro Support**:
  - **Confluent Schema Registry**: Automatic schema retrieval by ID from messages (format: magic byte + schema ID + data)
  - **Static Schema**: Use predefined schema from configuration or file
  - **Schema Caching**: Schemas from Schema Registry are cached for performance
- **Metadata**: Each message contains metadata:
  - `topic` - topic name
  - `partition` - partition number
  - `offset` - message offset
  - `key` - message key (if present)

### Sink

```yaml
sink:
  type: kafka
  kafka:
    brokers:
      - kafka1:9092
    topic: output-topic
    # TLS and SASL configuration similar to source
```

## Trino

The Trino connector supports reading from and writing to Trino (formerly PrestoSQL) tables. It supports SQL queries, Keycloak OAuth2/OIDC authentication, and batch inserts.

### Source

```yaml
source:
  type: trino
  trino:
    # Trino server URL (required)
    serverURL: "http://trino:8080"

    # Catalog to use (required)
    catalog: hive

    # Schema to use (required)
    schema: default

    # Table to read from (required, if not using query)
    table: source_table

    # Custom SQL query (optional)
    # If specified, used instead of reading from table
    query: "SELECT * FROM hive.default.source_table WHERE id > 100"

    # Poll interval in seconds (optional, default: 5)
    # Used for periodic reading of new data
    pollInterval: 60

    # Keycloak authentication (optional)
    keycloak:
      # Option 1: Use long-lived token directly (recommended for long-lived tokens)
      token: "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..."

      # Option 2: Use OAuth2 flow (alternative to direct token)
      # serverURL: "https://keycloak.example.com"
      # realm: myrealm
      # clientID: trino-client
      # clientSecret: client-secret
      # username: trino-user
      # password: trino-password
```

### Features

- **SQL Queries**: Support for custom SQL queries with WHERE, JOIN, etc.
- **Periodic Polling**: Regularly polls tables for new data
- **Keycloak Authentication**: OAuth2/OIDC authentication via Keycloak
  - **Direct Token**: Use a long-lived token obtained from Keycloak (recommended for long-lived tokens)
  - **Password Grant**: Use username/password for authentication
  - **Client Credentials**: Use client ID/secret for service-to-service authentication
- **Automatic Token Refresh**: Tokens are automatically refreshed before expiration (only for OAuth2 flow, not for direct tokens)
- **Metadata**: Each message contains metadata:
  - `catalog` - catalog name
  - `schema` - schema name
  - `table` - table name

#### Obtaining a Token from Keycloak

To use a long-lived token, you can obtain it from Keycloak using the following methods:

**Method 1: Using curl (Password Grant)**
```bash
curl -X POST "https://keycloak.example.com/realms/myrealm/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "client_id=trino-client" \
  -d "client_secret=client-secret" \
  -d "username=trino-user" \
  -d "password=trino-password"
```

**Method 2: Using curl (Client Credentials)**
```bash
curl -X POST "https://keycloak.example.com/realms/myrealm/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=trino-client" \
  -d "client_secret=client-secret"
```

The response will contain an `access_token` field. Use this token value in the `token` field of the Keycloak configuration.

**Note**: For long-lived tokens, configure the token lifespan in Keycloak realm settings or client settings.

### Sink

```yaml
sink:
  type: trino
  trino:
    # Trino server URL (required)
    serverURL: "http://trino:8080"

    # Catalog to use (required)
    catalog: hive

    # Schema to use (required)
    schema: default

    # Table to write to (required)
    table: target_table

    # Batch size for inserts (optional, default: 1)
    # Increase for better performance
    batchSize: 100

    # Auto-create table (optional, default: false)
    # If true, creates table with VARCHAR column for JSON data
    autoCreateTable: true

    # Keycloak authentication (optional)
    keycloak:
      serverURL: "https://keycloak.example.com"
      realm: myrealm
      clientID: trino-client
      clientSecret: client-secret
      username: trino-user
      password: trino-password
```

### Features

- **Batch Inserts**: Groups messages for efficient writing
- **Auto-create Tables**: Automatically creates tables if they don't exist
- **Keycloak Authentication**: OAuth2/OIDC authentication via Keycloak
- **Automatic Token Refresh**: Tokens are automatically refreshed

### Example: Kafka to Trino with Keycloak

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-to-trino
spec:
  source:
    type: kafka
    kafka:
      brokers:
        - kafka:9092
      topic: input-topic
      consumerGroup: dataflow-group
  sink:
    type: trino
    trino:
      serverURL: "http://trino:8080"
      catalog: hive
      schema: default
      table: output_table
      batchSize: 100
      keycloak:
        # Use long-lived token obtained from Keycloak
        token: "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..."

        # Alternative: Use OAuth2 flow
        # serverURL: "https://keycloak.example.com"
        # realm: myrealm
        # clientID: trino-client
        # clientSecret: client-secret
        # username: trino-user
        # password: trino-password
```

## Error Handling (Error Sink)

DataFlow Operator supports configuring a separate sink for messages that failed to be written to the main sink. This allows preserving problematic messages for later analysis and processing.

### Configuration

The `errors` section in the DataFlow spec defines the error sink:

```yaml
spec:
  source:
    # ... source configuration
  sink:
    # ... main sink configuration
  errors:
    type: kafka
    kafka:
      brokers:
        - localhost:9092
      topic: error-topic
```

### Error Message Structure

When a message fails to be written to the main sink, it is sent to the error sink with the following structure:

```json
{
  "error": {
    "message": "error text",
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

### Features

- **Any sink type**: Error sink can be of any type (Kafka, PostgreSQL, Trino)
- **Data preservation**: Original message data is preserved in the `original_message` field
- **Error information**: Error information is embedded in the message structure, ensuring it is preserved regardless of the error sink type
- **Error handling**: If `errors` is not specified, write errors will cause processing to stop
- **Router sinks**: Error sink also works for messages sent through router transformation

For complete connector documentation, see the [Russian version](../ru/connectors.md).

