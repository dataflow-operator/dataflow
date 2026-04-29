/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// DataFlowSpec defines the desired state of DataFlow
type DataFlowSpec struct {
	// Source defines the source of data
	Source SourceSpec `json:"source"`

	// Sink defines the destination of data
	Sink SinkSpec `json:"sink"`

	// Transformations is a list of transformations to apply to messages
	// +optional
	Transformations []TransformationSpec `json:"transformations,omitempty"`

	// Errors defines the error sink for messages that failed to be written to the main sink
	// +optional
	Errors *SinkSpec `json:"errors,omitempty"`

	// Resources defines the resource requirements for the processor pod
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// NodeSelector is a selector which must be true for the pod to fit on a node
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Affinity is a group of affinity scheduling rules
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// Tolerations are attached to tolerate any taint that matches the triple <key,value,effect>
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// ProcessorImage is the full container image for the dataflow processor (e.g. ghcr.io/org/dataflow:v1.0.0).
	// If not set, the processor runs with the same image as the controller (or ProcessorVersion is used if set).
	// +optional
	ProcessorImage string `json:"processorImage,omitempty"`

	// ProcessorVersion is the image tag for the processor when using the default image repository (same as controller).
	// Ignored if ProcessorImage is set. Example: "v1.2.3".
	// +optional
	ProcessorVersion string `json:"processorVersion,omitempty"`

	// ImagePullSecrets is a list of references to secrets in the same namespace to use for pulling the processor image from a private registry.
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// CheckpointPersistence enables persisting source checkpoint (lastReadID, lastReadChangeTime) to a ConfigMap.
	// When enabled, polling sources (PostgreSQL, ClickHouse, Trino) resume from the last committed position after restart, reducing duplicates.
	// Default: true. Set to false to disable.
	// +optional
	CheckpointPersistence *bool `json:"checkpointPersistence,omitempty"`

	// ChannelBufferSize is the buffer size for message channels between source, processor, and sink.
	// Larger values reduce blocking when sink is slower than source (e.g. high Kafka throughput).
	// Default: 100. Recommended for high throughput: 500–1000.
	// +optional
	ChannelBufferSize *int32 `json:"channelBufferSize,omitempty"`
}

// SourceSpec defines the source configuration (type + config).
// +kubebuilder:pruning:PreserveUnknownFields
type SourceSpec struct {
	// Type of source: kafka, postgresql, trino, clickhouse, nessie, or plugin type
	Type string `json:"type"`

	// Config holds connector configuration. Structure depends on type.
	// For built-in types see KafkaSourceSpec, PostgreSQLSourceSpec, etc.
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	Config *runtime.RawExtension `json:"config,omitempty"`
}

// getTypedConfig unmarshals a RawExtension into the given type T.
func getTypedConfig[T any](raw *runtime.RawExtension) (*T, error) {
	if raw == nil || len(raw.Raw) == 0 {
		return nil, nil
	}
	var cfg T
	if err := json.Unmarshal(raw.Raw, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// GetKafkaConfig returns Kafka config from Config.
func (s *SourceSpec) GetKafkaConfig() (*KafkaSourceSpec, error) {
	return getTypedConfig[KafkaSourceSpec](s.Config)
}

// GetPostgreSQLConfig returns PostgreSQL config.
func (s *SourceSpec) GetPostgreSQLConfig() (*PostgreSQLSourceSpec, error) {
	return getTypedConfig[PostgreSQLSourceSpec](s.Config)
}

// GetTrinoConfig returns Trino config.
func (s *SourceSpec) GetTrinoConfig() (*TrinoSourceSpec, error) {
	return getTypedConfig[TrinoSourceSpec](s.Config)
}

// GetClickHouseConfig returns ClickHouse config.
func (s *SourceSpec) GetClickHouseConfig() (*ClickHouseSourceSpec, error) {
	return getTypedConfig[ClickHouseSourceSpec](s.Config)
}

// GetNessieConfig returns Nessie config.
func (s *SourceSpec) GetNessieConfig() (*NessieSourceSpec, error) {
	return getTypedConfig[NessieSourceSpec](s.Config)
}

// KafkaSourceSpec defines Kafka source configuration
type KafkaSourceSpec struct {
	// Brokers is a list of Kafka broker addresses
	Brokers []string `json:"brokers"`

	// Topic to read from
	Topic string `json:"topic"`

	// ConsumerGroup for Kafka consumer
	// +optional
	ConsumerGroup string `json:"consumerGroup,omitempty"`

	// TLS configuration
	// +optional
	TLS *TLSConfig `json:"tls,omitempty"`

	// SASL configuration
	// +optional
	SASL *SASLConfig `json:"sasl,omitempty"`

	// BrokersSecretRef references a Kubernetes secret for brokers (comma-separated)
	// +optional
	BrokersSecretRef *SecretRef `json:"brokersSecretRef,omitempty"`

	// TopicSecretRef references a Kubernetes secret for topic
	// +optional
	TopicSecretRef *SecretRef `json:"topicSecretRef,omitempty"`

	// ConsumerGroupSecretRef references a Kubernetes secret for consumer group
	// +optional
	ConsumerGroupSecretRef *SecretRef `json:"consumerGroupSecretRef,omitempty"`

	// Format specifies the message format: "json" (default) or "avro"
	// +optional
	Format string `json:"format,omitempty"`

	// AvroSchema is the Avro schema as JSON string (required if format is "avro")
	// +optional
	AvroSchema string `json:"avroSchema,omitempty"`

	// AvroSchemaFile is the path to a file containing the Avro schema (alternative to avroSchema)
	// +optional
	AvroSchemaFile string `json:"avroSchemaFile,omitempty"`

	// AvroSchemaSecretRef references a Kubernetes secret for Avro schema
	// +optional
	AvroSchemaSecretRef *SecretRef `json:"avroSchemaSecretRef,omitempty"`

	// SchemaRegistry configuration for Confluent Schema Registry
	// +optional
	SchemaRegistry *SchemaRegistryConfig `json:"schemaRegistry,omitempty"`
}

// SchemaRegistryConfig defines Confluent Schema Registry configuration
type SchemaRegistryConfig struct {
	// URL is the Schema Registry base URL (e.g., http://localhost:8081)
	URL string `json:"url"`

	// BasicAuth configuration
	// +optional
	BasicAuth *BasicAuthConfig `json:"basicAuth,omitempty"`

	// TLS configuration for Schema Registry
	// +optional
	TLS *TLSConfig `json:"tls,omitempty"`

	// URLSecretRef references a Kubernetes secret for Schema Registry URL
	// +optional
	URLSecretRef *SecretRef `json:"urlSecretRef,omitempty"`
}

// BasicAuthConfig defines basic authentication configuration
type BasicAuthConfig struct {
	// Username for basic authentication (optional if UsernameSecretRef is provided)
	// +optional
	Username string `json:"username,omitempty"`

	// Password for basic authentication (optional if PasswordSecretRef is provided)
	// +optional
	Password string `json:"password,omitempty"`

	// UsernameSecretRef references a Kubernetes secret for username
	// +optional
	UsernameSecretRef *SecretRef `json:"usernameSecretRef,omitempty"`

	// PasswordSecretRef references a Kubernetes secret for password
	// +optional
	PasswordSecretRef *SecretRef `json:"passwordSecretRef,omitempty"`
}

// PostgreSQLSourceSpec defines PostgreSQL source configuration
type PostgreSQLSourceSpec struct {
	// ConnectionString for PostgreSQL database
	ConnectionString string `json:"connectionString"`

	// Table to read from
	Table string `json:"table"`

	// Query for custom SQL query (optional, if not provided, reads from table)
	// +optional
	Query string `json:"query,omitempty"`

	// PollInterval in seconds for polling mode
	// +optional
	PollInterval *int32 `json:"pollInterval,omitempty"`

	// ConnectionStringSecretRef references a Kubernetes secret for connection string
	// +optional
	ConnectionStringSecretRef *SecretRef `json:"connectionStringSecretRef,omitempty"`

	// TableSecretRef references a Kubernetes secret for table name
	// +optional
	TableSecretRef *SecretRef `json:"tableSecretRef,omitempty"`

	// ReadBatchSize limits rows per poll to reduce DB load (0 = no limit)
	// +optional
	ReadBatchSize *int32 `json:"readBatchSize,omitempty"`

	// ChangeTrackingColumn is the column used to track changes (default: updated_at).
	// Not used when Query is specified.
	// +optional
	// +kubebuilder:default="updated_at"
	ChangeTrackingColumn string `json:"changeTrackingColumn,omitempty"`

	// AutoCreateTable creates the table if it doesn't exist before reading
	// +optional
	AutoCreateTable *bool `json:"autoCreateTable,omitempty"`
}

// TrinoSourceSpec defines Trino source configuration
type TrinoSourceSpec struct {
	// ServerURL is the Trino server URL (e.g., http://trino:8080)
	ServerURL string `json:"serverURL"`

	// Catalog to use
	Catalog string `json:"catalog"`

	// Schema to use
	Schema string `json:"schema"`

	// Table to read from
	Table string `json:"table"`

	// Query for custom SQL query (optional, if not provided, reads from table)
	// +optional
	Query string `json:"query,omitempty"`

	// PollInterval in seconds for polling mode
	// +optional
	PollInterval *int32 `json:"pollInterval,omitempty"`

	// Keycloak authentication configuration
	// +optional
	Keycloak *KeycloakConfig `json:"keycloak,omitempty"`

	// ServerURLSecretRef references a Kubernetes secret for server URL
	// +optional
	ServerURLSecretRef *SecretRef `json:"serverURLSecretRef,omitempty"`

	// CatalogSecretRef references a Kubernetes secret for catalog name
	// +optional
	CatalogSecretRef *SecretRef `json:"catalogSecretRef,omitempty"`

	// SchemaSecretRef references a Kubernetes secret for schema name
	// +optional
	SchemaSecretRef *SecretRef `json:"schemaSecretRef,omitempty"`

	// TableSecretRef references a Kubernetes secret for table name
	// +optional
	TableSecretRef *SecretRef `json:"tableSecretRef,omitempty"`
}

// ClickHouseSourceSpec defines ClickHouse source configuration
type ClickHouseSourceSpec struct {
	// ConnectionString for ClickHouse database (e.g., clickhouse://host:9000?username=default&password=xxx&database=default)
	ConnectionString string `json:"connectionString"`

	// Table to read from
	Table string `json:"table"`

	// Query for custom SQL query (optional, if not provided, reads from table)
	// +optional
	Query string `json:"query,omitempty"`

	// PollInterval in seconds for polling mode
	// +optional
	PollInterval *int32 `json:"pollInterval,omitempty"`

	// ConnectionStringSecretRef references a Kubernetes secret for connection string
	// +optional
	ConnectionStringSecretRef *SecretRef `json:"connectionStringSecretRef,omitempty"`

	// TableSecretRef references a Kubernetes secret for table name
	// +optional
	TableSecretRef *SecretRef `json:"tableSecretRef,omitempty"`
}

// NessieSourceSpec defines Nessie (Iceberg REST catalog) source configuration.
// All catalog operations (load table, read) are performed in the context of the given branch.
// URI for Nessie Iceberg REST: {baseURL}/iceberg[/{branch}][|{warehouse}].
type NessieAuthenticationType string

const (
	// NessieAuthenticationAuto uses Bearer when token is configured, otherwise Basic when credentials are configured.
	NessieAuthenticationAuto NessieAuthenticationType = "AUTO"
	// NessieAuthenticationBearer forces Bearer token authentication.
	NessieAuthenticationBearer NessieAuthenticationType = "BEARER"
	// NessieAuthenticationBasic forces HTTP Basic authentication.
	NessieAuthenticationBasic NessieAuthenticationType = "BASIC"
	// NessieAuthenticationNone disables Authorization header.
	NessieAuthenticationNone NessieAuthenticationType = "NONE"
)

type NessieSourceSpec struct {
	// BaseURL is the Nessie server base URL (e.g. https://nessie:19120).
	BaseURL string `json:"baseURL"`

	// Branch is the Nessie branch to read from (default: main).
	// +optional
	Branch string `json:"branch,omitempty"`

	// Warehouse is the optional warehouse name for storage location (e.g. for /iceberg/|warehouse).
	// +optional
	Warehouse string `json:"warehouse,omitempty"`

	// Namespace is the schema/namespace of the Iceberg table.
	Namespace string `json:"namespace"`

	// Table is the Iceberg table name.
	Table string `json:"table"`

	// Query for custom filter (optional). If empty, full table scan.
	// +optional
	Query string `json:"query,omitempty"`

	// PollInterval in seconds for polling mode.
	// +optional
	PollInterval *int32 `json:"pollInterval,omitempty"`

	// BasicAuth for Nessie/Iceberg REST.
	// +optional
	BasicAuth *BasicAuthConfig `json:"basicAuth,omitempty"`

	// BearerToken for Nessie (optional if TokenSecretRef is set).
	// +optional
	BearerToken string `json:"bearerToken,omitempty"`

	// AuthenticationType controls how Authorization header is sent to Nessie/Iceberg REST.
	// Supported: AUTO (default), BEARER, BASIC, NONE.
	// +optional
	AuthenticationType NessieAuthenticationType `json:"authenticationType,omitempty"`

	// BaseURLSecretRef references a Kubernetes secret for base URL.
	// +optional
	BaseURLSecretRef *SecretRef `json:"baseURLSecretRef,omitempty"`

	// TokenSecretRef references a Kubernetes secret for bearer token.
	// +optional
	TokenSecretRef *SecretRef `json:"tokenSecretRef,omitempty"`

	// NamespaceSecretRef references a Kubernetes secret for namespace.
	// +optional
	NamespaceSecretRef *SecretRef `json:"namespaceSecretRef,omitempty"`

	// TableSecretRef references a Kubernetes secret for table name.
	// +optional
	TableSecretRef *SecretRef `json:"tableSecretRef,omitempty"`
}

// KeycloakConfig defines Keycloak OAuth2/OIDC authentication configuration
type KeycloakConfig struct {
	// ServerURL is the Keycloak server URL (e.g., https://keycloak.example.com/auth)
	ServerURL string `json:"serverURL"`

	// Realm is the Keycloak realm name
	Realm string `json:"realm"`

	// ClientID is the OAuth2 client ID
	ClientID string `json:"clientID"`

	// ClientSecret is the OAuth2 client secret (optional if ClientSecretSecretRef is provided)
	// +optional
	ClientSecret string `json:"clientSecret,omitempty"`

	// Username for password grant (optional if UsernameSecretRef is provided)
	// +optional
	Username string `json:"username,omitempty"`

	// Password for password grant (optional if PasswordSecretRef is provided)
	// +optional
	Password string `json:"password,omitempty"`

	// Token is a long-lived OAuth2 token obtained from Keycloak (optional if TokenSecretRef is provided)
	// If provided, this token will be used directly instead of OAuth2 flow
	// +optional
	Token string `json:"token,omitempty"`

	// ServerURLSecretRef references a Kubernetes secret for Keycloak server URL
	// +optional
	ServerURLSecretRef *SecretRef `json:"serverURLSecretRef,omitempty"`

	// RealmSecretRef references a Kubernetes secret for realm name
	// +optional
	RealmSecretRef *SecretRef `json:"realmSecretRef,omitempty"`

	// ClientIDSecretRef references a Kubernetes secret for client ID
	// +optional
	ClientIDSecretRef *SecretRef `json:"clientIDSecretRef,omitempty"`

	// ClientSecretSecretRef references a Kubernetes secret for client secret
	// +optional
	ClientSecretSecretRef *SecretRef `json:"clientSecretSecretRef,omitempty"`

	// UsernameSecretRef references a Kubernetes secret for username
	// +optional
	UsernameSecretRef *SecretRef `json:"usernameSecretRef,omitempty"`

	// PasswordSecretRef references a Kubernetes secret for password
	// +optional
	PasswordSecretRef *SecretRef `json:"passwordSecretRef,omitempty"`

	// TokenSecretRef references a Kubernetes secret for OAuth2 token
	// If provided, this token will be used directly instead of OAuth2 flow
	// +optional
	TokenSecretRef *SecretRef `json:"tokenSecretRef,omitempty"`
}

// SinkSpec defines the sink configuration (type + config).
// +kubebuilder:pruning:PreserveUnknownFields
type SinkSpec struct {
	// Type of sink: kafka, postgresql, trino, clickhouse, nessie, or plugin type
	Type string `json:"type"`

	// Config holds connector configuration. Structure depends on type.
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	Config *runtime.RawExtension `json:"config,omitempty"`
}

// GetKafkaConfig returns Kafka sink config.
func (s *SinkSpec) GetKafkaConfig() (*KafkaSinkSpec, error) {
	return getTypedConfig[KafkaSinkSpec](s.Config)
}

// GetPostgreSQLConfig returns PostgreSQL sink config.
func (s *SinkSpec) GetPostgreSQLConfig() (*PostgreSQLSinkSpec, error) {
	return getTypedConfig[PostgreSQLSinkSpec](s.Config)
}

// GetTrinoConfig returns Trino sink config.
func (s *SinkSpec) GetTrinoConfig() (*TrinoSinkSpec, error) {
	return getTypedConfig[TrinoSinkSpec](s.Config)
}

// GetClickHouseConfig returns ClickHouse sink config.
func (s *SinkSpec) GetClickHouseConfig() (*ClickHouseSinkSpec, error) {
	return getTypedConfig[ClickHouseSinkSpec](s.Config)
}

// GetNessieConfig returns Nessie sink config.
func (s *SinkSpec) GetNessieConfig() (*NessieSinkSpec, error) {
	return getTypedConfig[NessieSinkSpec](s.Config)
}

// NessieSinkSpec defines Nessie (Iceberg REST catalog) sink configuration.
// Writes are committed to the given branch via the catalog.
type NessieSinkSpec struct {
	// BaseURL is the Nessie server base URL (e.g. https://nessie:19120).
	BaseURL string `json:"baseURL"`

	// Branch is the Nessie branch to write to (default: main).
	// +optional
	Branch string `json:"branch,omitempty"`

	// Warehouse is the optional warehouse name.
	// +optional
	Warehouse string `json:"warehouse,omitempty"`

	// Namespace is the schema/namespace of the Iceberg table.
	Namespace string `json:"namespace"`

	// Table is the Iceberg table name.
	Table string `json:"table"`

	// BatchSize for batch appends.
	// +optional
	BatchSize *int32 `json:"batchSize,omitempty"`

	// BatchFlushIntervalSeconds flushes the batch after this many seconds even if BatchSize is not reached (default: 10; 0 disables timer).
	// +optional
	BatchFlushIntervalSeconds *int32 `json:"batchFlushIntervalSeconds,omitempty"`

	// AutoCreateTable creates the table if it does not exist.
	// +optional
	AutoCreateTable *bool `json:"autoCreateTable,omitempty"`

	// BasicAuth for Nessie/Iceberg REST.
	// +optional
	BasicAuth *BasicAuthConfig `json:"basicAuth,omitempty"`

	// BearerToken for Nessie (optional if TokenSecretRef is set).
	// +optional
	BearerToken string `json:"bearerToken,omitempty"`

	// AuthenticationType controls how Authorization header is sent to Nessie/Iceberg REST.
	// Supported: AUTO (default), BEARER, BASIC, NONE.
	// +optional
	AuthenticationType NessieAuthenticationType `json:"authenticationType,omitempty"`

	// BaseURLSecretRef references a Kubernetes secret for base URL.
	// +optional
	BaseURLSecretRef *SecretRef `json:"baseURLSecretRef,omitempty"`

	// TokenSecretRef references a Kubernetes secret for bearer token.
	// +optional
	TokenSecretRef *SecretRef `json:"tokenSecretRef,omitempty"`

	// NamespaceSecretRef references a Kubernetes secret for namespace.
	// +optional
	NamespaceSecretRef *SecretRef `json:"namespaceSecretRef,omitempty"`

	// TableSecretRef references a Kubernetes secret for table name.
	// +optional
	TableSecretRef *SecretRef `json:"tableSecretRef,omitempty"`
}

// KafkaSinkSpec defines Kafka sink configuration
type KafkaSinkSpec struct {
	// Brokers is a list of Kafka broker addresses
	Brokers []string `json:"brokers"`

	// Topic to write to
	Topic string `json:"topic"`

	// TLS configuration
	// +optional
	TLS *TLSConfig `json:"tls,omitempty"`

	// SASL configuration
	// +optional
	SASL *SASLConfig `json:"sasl,omitempty"`

	// BrokersSecretRef references a Kubernetes secret for brokers (comma-separated)
	// +optional
	BrokersSecretRef *SecretRef `json:"brokersSecretRef,omitempty"`

	// TopicSecretRef references a Kubernetes secret for topic
	// +optional
	TopicSecretRef *SecretRef `json:"topicSecretRef,omitempty"`
}

// PostgreSQLSinkSpec defines PostgreSQL sink configuration
type PostgreSQLSinkSpec struct {
	// ConnectionString for PostgreSQL database
	ConnectionString string `json:"connectionString"`

	// Table to write to
	Table string `json:"table"`

	// BatchSize for batch inserts (default: 1)
	// +optional
	// +kubebuilder:default=1
	BatchSize *int32 `json:"batchSize,omitempty"`

	// BatchFlushIntervalSeconds flushes the batch after this many seconds even if BatchSize is not reached (default: 10; 0 disables timer).
	// +optional
	BatchFlushIntervalSeconds *int32 `json:"batchFlushIntervalSeconds,omitempty"`

	// AutoCreateTable automatically creates the table if it doesn't exist
	// +optional
	AutoCreateTable *bool `json:"autoCreateTable,omitempty"`

	// UpsertMode enables UPSERT behavior (INSERT ... ON CONFLICT ... DO UPDATE)
	// If true, existing records will be updated instead of being skipped
	// +optional
	UpsertMode *bool `json:"upsertMode,omitempty"`

	// ConflictKey specifies the column(s) to use for conflict detection in UPSERT mode
	// If not specified, defaults to PRIMARY KEY
	// +optional
	ConflictKey *string `json:"conflictKey,omitempty"`

	// SoftDeleteColumn specifies column for soft delete (e.g. "deleted_at"). If set, DELETE operations will UPDATE this column instead of physical delete.
	// +optional
	SoftDeleteColumn *string `json:"softDeleteColumn,omitempty"`

	// RawMode when true, expects messages in format {"value": <data>, "_metadata": {...}}. Table is created with value JSONB and _metadata JSONB columns.
	// When false, table structure is inferred from the first message (replicates source structure).
	// +optional
	RawMode *bool `json:"rawMode,omitempty"`

	// ConnectionStringSecretRef references a Kubernetes secret for connection string
	// +optional
	ConnectionStringSecretRef *SecretRef `json:"connectionStringSecretRef,omitempty"`

	// TableSecretRef references a Kubernetes secret for table name
	// +optional
	TableSecretRef *SecretRef `json:"tableSecretRef,omitempty"`
}

// TrinoSinkSpec defines Trino sink configuration
type TrinoSinkSpec struct {
	// ServerURL is the Trino server URL (e.g., http://trino:8080)
	ServerURL string `json:"serverURL"`

	// Catalog to use
	Catalog string `json:"catalog"`

	// Schema to use
	Schema string `json:"schema"`

	// Table to write to
	Table string `json:"table"`

	// BatchSize for batch inserts
	// +optional
	BatchSize *int32 `json:"batchSize,omitempty"`

	// BatchFlushIntervalSeconds flushes the batch after this many seconds even if BatchSize is not reached (default: 10; 0 disables timer).
	// +optional
	BatchFlushIntervalSeconds *int32 `json:"batchFlushIntervalSeconds,omitempty"`

	// AutoCreateTable automatically creates the table if it doesn't exist
	// +optional
	AutoCreateTable *bool `json:"autoCreateTable,omitempty"`

	// RawMode when true, creates table with data VARCHAR column (JSON storage).
	// When false (default), uses columnar format matching message keys to table columns.
	// +optional
	RawMode *bool `json:"rawMode,omitempty"`

	// Keycloak authentication configuration
	// +optional
	Keycloak *KeycloakConfig `json:"keycloak,omitempty"`

	// ServerURLSecretRef references a Kubernetes secret for server URL
	// +optional
	ServerURLSecretRef *SecretRef `json:"serverURLSecretRef,omitempty"`

	// CatalogSecretRef references a Kubernetes secret for catalog name
	// +optional
	CatalogSecretRef *SecretRef `json:"catalogSecretRef,omitempty"`

	// SchemaSecretRef references a Kubernetes secret for schema name
	// +optional
	SchemaSecretRef *SecretRef `json:"schemaSecretRef,omitempty"`

	// TableSecretRef references a Kubernetes secret for table name
	// +optional
	TableSecretRef *SecretRef `json:"tableSecretRef,omitempty"`
}

// ClickHouseSinkSpec defines ClickHouse sink configuration
type ClickHouseSinkSpec struct {
	// ConnectionString for ClickHouse database (e.g., clickhouse://host:9000?username=default&password=xxx&database=default)
	ConnectionString string `json:"connectionString"`

	// Table to write to
	Table string `json:"table"`

	// BatchSize for batch inserts
	// +optional
	BatchSize *int32 `json:"batchSize,omitempty"`

	// BatchFlushIntervalSeconds flushes the batch after this many seconds even if BatchSize is not reached (default: 10; 0 disables timer).
	// +optional
	BatchFlushIntervalSeconds *int32 `json:"batchFlushIntervalSeconds,omitempty"`

	// AutoCreateTable automatically creates the table if it doesn't exist
	// +optional
	AutoCreateTable *bool `json:"autoCreateTable,omitempty"`

	// RawMode when true, creates table with data String and created_at columns (JSON storage).
	// When false (default), creates table from message structure (columnar, replicates source schema).
	// +optional
	RawMode *bool `json:"rawMode,omitempty"`

	// ConnectionStringSecretRef references a Kubernetes secret for connection string
	// +optional
	ConnectionStringSecretRef *SecretRef `json:"connectionStringSecretRef,omitempty"`

	// TableSecretRef references a Kubernetes secret for table name
	// +optional
	TableSecretRef *SecretRef `json:"tableSecretRef,omitempty"`
}

// SecretRef references a Kubernetes secret
type SecretRef struct {
	// Name of the secret
	Name string `json:"name"`

	// Namespace of the secret (optional, defaults to the same namespace as DataFlow)
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// Key in the secret to read the value from
	Key string `json:"key"`
}

// TLSConfig defines TLS configuration
type TLSConfig struct {
	// InsecureSkipVerify skips certificate verification
	// +optional
	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"`

	// CertFile path to certificate file
	// +optional
	CertFile string `json:"certFile,omitempty"`

	// KeyFile path to key file
	// +optional
	KeyFile string `json:"keyFile,omitempty"`

	// CAFile path to CA certificate file
	// +optional
	CAFile string `json:"caFile,omitempty"`

	// CertSecretRef references a Kubernetes secret for certificate
	// +optional
	CertSecretRef *SecretRef `json:"certSecretRef,omitempty"`

	// KeySecretRef references a Kubernetes secret for key
	// +optional
	KeySecretRef *SecretRef `json:"keySecretRef,omitempty"`

	// CASecretRef references a Kubernetes secret for CA certificate
	// +optional
	CASecretRef *SecretRef `json:"caSecretRef,omitempty"`
}

// SASLConfig defines SASL configuration
type SASLConfig struct {
	// Mechanism: plain, scram-sha-256, scram-sha-512
	Mechanism string `json:"mechanism"`

	// Username (optional if UsernameSecretRef is provided)
	// +optional
	Username string `json:"username,omitempty"`

	// Password (optional if PasswordSecretRef is provided)
	// +optional
	Password string `json:"password,omitempty"`

	// UsernameSecretRef references a Kubernetes secret for username
	// +optional
	UsernameSecretRef *SecretRef `json:"usernameSecretRef,omitempty"`

	// PasswordSecretRef references a Kubernetes secret for password
	// +optional
	PasswordSecretRef *SecretRef `json:"passwordSecretRef,omitempty"`
}

// TransformationSpec defines a transformation to apply (type + config).
// +kubebuilder:pruning:PreserveUnknownFields
type TransformationSpec struct {
	// Type of transformation: timestamp, flatten, filter, mask, router, select, remove, snakeCase, camelCase
	Type string `json:"type"`

	// Config holds transformation configuration. Structure depends on type.
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	Config *runtime.RawExtension `json:"config,omitempty"`
}

// GetTimestampConfig returns Timestamp transformation config.
func (t *TransformationSpec) GetTimestampConfig() (*TimestampTransformation, error) {
	return getTypedConfig[TimestampTransformation](t.Config)
}

// GetFlattenConfig returns Flatten transformation config.
func (t *TransformationSpec) GetFlattenConfig() (*FlattenTransformation, error) {
	return getTypedConfig[FlattenTransformation](t.Config)
}

// GetFilterConfig returns Filter transformation config.
func (t *TransformationSpec) GetFilterConfig() (*FilterTransformation, error) {
	return getTypedConfig[FilterTransformation](t.Config)
}

// GetMaskConfig returns Mask transformation config.
func (t *TransformationSpec) GetMaskConfig() (*MaskTransformation, error) {
	return getTypedConfig[MaskTransformation](t.Config)
}

// GetRouterConfig returns Router transformation config.
func (t *TransformationSpec) GetRouterConfig() (*RouterTransformation, error) {
	return getTypedConfig[RouterTransformation](t.Config)
}

// GetSelectConfig returns Select transformation config.
func (t *TransformationSpec) GetSelectConfig() (*SelectTransformation, error) {
	return getTypedConfig[SelectTransformation](t.Config)
}

// GetRemoveConfig returns Remove transformation config.
func (t *TransformationSpec) GetRemoveConfig() (*RemoveTransformation, error) {
	return getTypedConfig[RemoveTransformation](t.Config)
}

// GetSnakeCaseConfig returns SnakeCase transformation config.
func (t *TransformationSpec) GetSnakeCaseConfig() (*SnakeCaseTransformation, error) {
	return getTypedConfig[SnakeCaseTransformation](t.Config)
}

// GetCamelCaseConfig returns CamelCase transformation config.
func (t *TransformationSpec) GetCamelCaseConfig() (*CamelCaseTransformation, error) {
	return getTypedConfig[CamelCaseTransformation](t.Config)
}

// TimestampTransformation adds a timestamp field
type TimestampTransformation struct {
	// FieldName is the name of the timestamp field (default: created_at)
	// +optional
	FieldName string `json:"fieldName,omitempty"`

	// Format is the timestamp format (default: RFC3339)
	// +optional
	Format string `json:"format,omitempty"`
}

// FlattenTransformation flattens an array field
type FlattenTransformation struct {
	// Field is the JSONPath to the array field to flatten
	Field string `json:"field"`
}

// FilterTransformation filters messages based on conditions
type FilterTransformation struct {
	// Condition is a JSONPath expression that must evaluate to true
	Condition string `json:"condition"`
}

// MaskTransformation masks sensitive data
type MaskTransformation struct {
	// Fields is a list of JSONPath expressions to mask
	Fields []string `json:"fields"`

	// MaskChar is the character to use for masking (default: *)
	// +optional
	MaskChar string `json:"maskChar,omitempty"`

	// KeepLength keeps the original length of the value
	// +optional
	KeepLength bool `json:"keepLength,omitempty"`
}

// RouterTransformation routes messages to different sinks
type RouterTransformation struct {
	// Routes is a list of routing rules
	Routes []RouteRule `json:"routes"`
}

// RouteRule defines a routing rule
type RouteRule struct {
	// Condition is a JSONPath expression that must evaluate to true
	Condition string `json:"condition"`

	// Sink is the sink configuration for this route
	Sink SinkSpec `json:"sink"`
}

// SelectTransformation selects specific fields
type SelectTransformation struct {
	// Fields is a list of JSONPath expressions to select
	Fields []string `json:"fields"`
}

// RemoveTransformation removes specific fields
type RemoveTransformation struct {
	// Fields is a list of JSONPath expressions to remove
	Fields []string `json:"fields"`
}

// SnakeCaseTransformation converts field names to snake_case
type SnakeCaseTransformation struct {
	// Deep indicates whether to convert nested objects recursively
	// +optional
	Deep bool `json:"deep,omitempty"`
}

// CamelCaseTransformation converts field names to CamelCase
type CamelCaseTransformation struct {
	// Deep indicates whether to convert nested objects recursively
	// +optional
	Deep bool `json:"deep,omitempty"`
}

// DataFlowStatus defines the observed state of DataFlow
type DataFlowStatus struct {
	// Phase represents the current phase of the data flow
	// +optional
	Phase string `json:"phase,omitempty"`

	// Message provides additional information about the status
	// +optional
	Message string `json:"message,omitempty"`

	// LastProcessedTime is the timestamp of the last processed message
	// +optional
	LastProcessedTime *metav1.Time `json:"lastProcessedTime,omitempty"`

	// ProcessedCount is the number of processed messages
	// +optional
	ProcessedCount int64 `json:"processedCount,omitempty"`

	// ErrorCount is the number of errors encountered
	// +optional
	ErrorCount int64 `json:"errorCount,omitempty"`

	// Conditions represent the latest available observations of the DataFlow state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// DataFlow is the Schema for the dataflows API
type DataFlow struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DataFlowSpec   `json:"spec,omitempty"`
	Status DataFlowStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// DataFlowList contains a list of DataFlow
type DataFlowList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DataFlow `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DataFlow{}, &DataFlowList{})
}
