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
	Errors *ErrorSinkSpec `json:"errors,omitempty"`

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

	// CheckpointPersistence enables persisting source checkpoint (lastReadChangeTime, lastReadOrderByValue) to a ConfigMap.
	// When enabled, polling sources (PostgreSQL, ClickHouse, Trino) resume from the last committed position after restart, reducing duplicates.
	// Default: true. Set to false to disable.
	// +optional
	CheckpointPersistence *bool `json:"checkpointPersistence,omitempty"`

	// CheckpointSyncOnAck persists checkpoint to the ConfigMap immediately after each sink batch ack.
	// Shrinks the duplicate window on pod crash from the debounce interval to roughly one batch.
	// Default: false (debounced save only). Recommended for migration and cron workloads.
	// +optional
	CheckpointSyncOnAck *bool `json:"checkpointSyncOnAck,omitempty"`

	// CheckpointSaveInterval controls how often pending checkpoints are flushed to the ConfigMap.
	// Also used as the minimum coalesce interval when checkpointSyncOnAck is true.
	// Default: 30s.
	// +optional
	CheckpointSaveInterval *metav1.Duration `json:"checkpointSaveInterval,omitempty"`

	// AckGranularity controls when source offsets are committed relative to sink writes.
	// "batch" (default): commit after each sink batch flush.
	// "message": commit after each message is successfully written (reduces re-read window for Kafka→batch sink).
	// +kubebuilder:validation:Enum=batch;message
	// +kubebuilder:default:=batch
	// +optional
	AckGranularity string `json:"ackGranularity,omitempty"`

	// CheckpointReset clears persisted source checkpoint on the next processor start (one-shot).
	// Alternatively set annotation dataflow.dataflow.io/reset-checkpoint: "true" on the DataFlow.
	// +optional
	CheckpointReset *bool `json:"checkpointReset,omitempty"`

	// StrictIdempotency rejects polling sources paired with non-idempotent main sinks at admission.
	// When false (default), a warning is emitted instead.
	// +optional
	StrictIdempotency *bool `json:"strictIdempotency,omitempty"`

	// ChannelBufferSize is the buffer size for message channels between source, processor, and sink.
	// Larger values reduce blocking when sink is slower than source (e.g. high Kafka throughput).
	// Default: 100. Recommended for high throughput: 500–1000.
	// +optional
	ChannelBufferSize *int32 `json:"channelBufferSize,omitempty"`

	// Replicas is the number of processor pods (Deployment replicas).
	// Only supported for Kafka sources (consumer group coordinates partition assignment).
	// For polling sources (postgresql, clickhouse, trino, nessie) must be 1 or unset.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default:=1
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`
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

// GetIcebergConfig returns Iceberg REST catalog source config.
func (s *SourceSpec) GetIcebergConfig() (*IcebergSourceSpec, error) {
	return getTypedConfig[IcebergSourceSpec](s.Config)
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

	// SecurityProtocol maps to Kafka client property security.protocol.
	// Supported: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
	// If empty, TLS/SASL enable flags are inferred from tls/sasl sections (legacy behavior).
	// +optional
	SecurityProtocol string `json:"securityProtocol,omitempty"`

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

	// ConsumerMaxWait is the maximum time the broker may delay a Fetch response (Kafka fetch.max.wait.ms).
	// Increase when brokers return REQUEST_TIMED_OUT under load (e.g. "30s").
	// +optional
	ConsumerMaxWait *metav1.Duration `json:"consumerMaxWait,omitempty"`

	// FetchMinBytes is the minimum bytes the broker should accumulate before responding to Fetch.
	// +optional
	FetchMinBytes *int32 `json:"fetchMinBytes,omitempty"`

	// FetchMaxBytes is the maximum bytes per Fetch response (maps to fetch.max.bytes / Sarama Fetch.Default).
	// +optional
	FetchMaxBytes *int32 `json:"fetchMaxBytes,omitempty"`

	// MaxPartitionFetchBytes is the maximum bytes per partition per Fetch request (Sarama Consumer.Fetch.Max).
	// +optional
	MaxPartitionFetchBytes *int32 `json:"maxPartitionFetchBytes,omitempty"`

	// NetReadTimeout is the network read timeout for Kafka client requests.
	// Should be greater than ConsumerMaxWait when both are set.
	// +optional
	NetReadTimeout *metav1.Duration `json:"netReadTimeout,omitempty"`

	// NetWriteTimeout is the network write timeout for Kafka client requests.
	// +optional
	NetWriteTimeout *metav1.Duration `json:"netWriteTimeout,omitempty"`
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
	// Used in table mode and in query mode when explicitly set (subquery wrapper + composite checkpoint).
	// +optional
	// +kubebuilder:default="updated_at"
	ChangeTrackingColumn string `json:"changeTrackingColumn,omitempty"`

	// OrderByColumn is the secondary sort key for stable pagination (default: id).
	// Used in ORDER BY together with changeTrackingColumn.
	// +optional
	OrderByColumn string `json:"orderByColumn,omitempty"`

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

	// OrderByColumn is the column used for incremental pagination and stable ORDER BY (default: id).
	// +optional
	OrderByColumn string `json:"orderByColumn,omitempty"`

	// ChangeTrackingColumn is the column used to track changes (default: updated_at).
	// Used in table mode and in query mode when explicitly set (subquery wrapper + composite checkpoint).
	// +optional
	// +kubebuilder:default="updated_at"
	ChangeTrackingColumn string `json:"changeTrackingColumn,omitempty"`

	// ReadBatchSize limits rows per poll to reduce load (0 = no limit).
	// +optional
	ReadBatchSize *int32 `json:"readBatchSize,omitempty"`
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

	// OrderByColumn is the column used for incremental pagination and stable ORDER BY (default: id).
	// +optional
	OrderByColumn string `json:"orderByColumn,omitempty"`

	// ChangeTrackingColumn is the column used to track changes (default: created_at).
	// Used in table mode and in query mode when explicitly set (subquery wrapper + composite checkpoint).
	// +optional
	// +kubebuilder:default="created_at"
	ChangeTrackingColumn string `json:"changeTrackingColumn,omitempty"`

	// ReadBatchSize limits rows per poll to reduce load (0 = no limit).
	// +optional
	ReadBatchSize *int32 `json:"readBatchSize,omitempty"`
}

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

	// IncrementalBySnapshot enables incremental reads using the Iceberg snapshot chain
	// instead of a full table scan on every poll. Default: false.
	// +optional
	IncrementalBySnapshot *bool `json:"incrementalBySnapshot,omitempty"`

	// StartSnapshotID is the snapshot ID to start from when no checkpoint exists (unsigned integer string).
	// +optional
	StartSnapshotID string `json:"startSnapshotID,omitempty"`

	// SnapshotCheckpoints persists snapshot progress to the checkpoint store when true.
	// Default: true. Requires spec.checkpointPersistence and incrementalBySnapshot.
	// +optional
	SnapshotCheckpoints *bool `json:"snapshotCheckpoints,omitempty"`
}

// IcebergRESTAuthenticationType selects how Authorization is sent to the Iceberg REST catalog.
type IcebergRESTAuthenticationType = NessieAuthenticationType

const (
	IcebergRESTAuthenticationAuto   = NessieAuthenticationAuto
	IcebergRESTAuthenticationBearer = NessieAuthenticationBearer
	IcebergRESTAuthenticationBasic  = NessieAuthenticationBasic
	IcebergRESTAuthenticationNone   = NessieAuthenticationNone
)

// IcebergSourceSpec defines Apache Iceberg REST catalog source configuration.
type IcebergSourceSpec struct {
	// CatalogURI is the REST catalog base URL (e.g. https://catalog:8181).
	CatalogURI string `json:"catalogURI"`

	// Prefix is the optional REST catalog path prefix (multi-tenant /v1/{prefix}/...).
	// +optional
	Prefix string `json:"prefix,omitempty"`

	// Warehouse is the optional warehouse identifier passed to the catalog.
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

	// BasicAuth for Iceberg REST catalog.
	// +optional
	BasicAuth *BasicAuthConfig `json:"basicAuth,omitempty"`

	// BearerToken for Iceberg REST (optional if TokenSecretRef is set).
	// +optional
	BearerToken string `json:"bearerToken,omitempty"`

	// AuthenticationType controls how Authorization header is sent to the REST catalog.
	// Supported: AUTO (default), BEARER, BASIC, NONE.
	// +optional
	AuthenticationType IcebergRESTAuthenticationType `json:"authenticationType,omitempty"`

	// OAuth2ServerURI overrides the OAuth2 token endpoint (optional).
	// +optional
	OAuth2ServerURI string `json:"oauth2ServerURI,omitempty"`

	// OAuth2ClientID for client credentials flow (optional if OAuth2ClientIDSecretRef is set).
	// +optional
	OAuth2ClientID string `json:"oauth2ClientID,omitempty"`

	// OAuth2ClientSecret for client credentials flow (optional if OAuth2ClientSecretSecretRef is set).
	// +optional
	OAuth2ClientSecret string `json:"oauth2ClientSecret,omitempty"`

	// OAuth2Scope for client credentials flow (default: catalog).
	// +optional
	OAuth2Scope string `json:"oauth2Scope,omitempty"`

	// CatalogURISecretRef references a Kubernetes secret for catalog URI.
	// +optional
	CatalogURISecretRef *SecretRef `json:"catalogURISecretRef,omitempty"`

	// TokenSecretRef references a Kubernetes secret for bearer token.
	// +optional
	TokenSecretRef *SecretRef `json:"tokenSecretRef,omitempty"`

	// OAuth2ServerURISecretRef references a Kubernetes secret for OAuth2 server URI.
	// +optional
	OAuth2ServerURISecretRef *SecretRef `json:"oauth2ServerURISecretRef,omitempty"`

	// OAuth2ClientIDSecretRef references a Kubernetes secret for OAuth2 client ID.
	// +optional
	OAuth2ClientIDSecretRef *SecretRef `json:"oauth2ClientIDSecretRef,omitempty"`

	// OAuth2ClientSecretSecretRef references a Kubernetes secret for OAuth2 client secret.
	// +optional
	OAuth2ClientSecretSecretRef *SecretRef `json:"oauth2ClientSecretSecretRef,omitempty"`

	// NamespaceSecretRef references a Kubernetes secret for namespace.
	// +optional
	NamespaceSecretRef *SecretRef `json:"namespaceSecretRef,omitempty"`

	// TableSecretRef references a Kubernetes secret for table name.
	// +optional
	TableSecretRef *SecretRef `json:"tableSecretRef,omitempty"`

	// IncrementalBySnapshot enables incremental reads using the Iceberg snapshot chain.
	// +optional
	IncrementalBySnapshot *bool `json:"incrementalBySnapshot,omitempty"`

	// StartSnapshotID is the snapshot ID to start from when no checkpoint exists.
	// +optional
	StartSnapshotID string `json:"startSnapshotID,omitempty"`

	// SnapshotCheckpoints persists snapshot progress to the checkpoint store when true.
	// +optional
	SnapshotCheckpoints *bool `json:"snapshotCheckpoints,omitempty"`
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

// GetIcebergConfig returns Iceberg REST catalog sink config.
func (s *SinkSpec) GetIcebergConfig() (*IcebergSinkSpec, error) {
	return getTypedConfig[IcebergSinkSpec](s.Config)
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

	// RawMode when true, creates table with data and _metadata string columns; wraps plain messages using msg.Metadata.
	// +optional
	RawMode *bool `json:"rawMode,omitempty"`

	FlattenMetadataSpec `json:",inline"`

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

	// S3Endpoint is the S3-compatible API endpoint URL for iceberg-go/AWS SDK (e.g. https://storage.yandexcloud.net).
	// When empty on AWS, the SDK uses default endpoints.
	// +optional
	S3Endpoint string `json:"s3Endpoint,omitempty"`

	// S3Region is the region passed as AWS_REGION for object storage operations (e.g. ru-central1).
	// +optional
	S3Region string `json:"s3Region,omitempty"`

	// AccessKeySecretRef references a Kubernetes Secret key for the S3 access key ID (maps to AWS_ACCESS_KEY_ID in the processor pod).
	// Values must not reference Secrets outside the DataFlow namespace — pods can only mount env from same-namespace Secrets.
	// The operator does not resolve this into the spec ConfigMap; credentials are injected only via pod env.
	// +optional
	AccessKeySecretRef *SecretRef `json:"accessKeySecretRef,omitempty"`

	// SecretAccessKeySecretRef references a Kubernetes Secret key for the S3 secret access key (maps to AWS_SECRET_ACCESS_KEY in the processor pod).
	// Same namespace rules as AccessKeySecretRef.
	// +optional
	SecretAccessKeySecretRef *SecretRef `json:"secretAccessKeySecretRef,omitempty"`
}

// IcebergSinkSpec defines Apache Iceberg REST catalog sink configuration.
type IcebergSinkSpec struct {
	// CatalogURI is the REST catalog base URL (e.g. https://catalog:8181).
	CatalogURI string `json:"catalogURI"`

	// Prefix is the optional REST catalog path prefix.
	// +optional
	Prefix string `json:"prefix,omitempty"`

	// Warehouse is the optional warehouse identifier.
	// +optional
	Warehouse string `json:"warehouse,omitempty"`

	// Namespace is the schema/namespace of the Iceberg table.
	Namespace string `json:"namespace"`

	// Table is the Iceberg table name.
	Table string `json:"table"`

	// BatchSize for batch appends.
	// +optional
	BatchSize *int32 `json:"batchSize,omitempty"`

	// BatchFlushIntervalSeconds flushes the batch after this many seconds even if BatchSize is not reached.
	// +optional
	BatchFlushIntervalSeconds *int32 `json:"batchFlushIntervalSeconds,omitempty"`

	// AutoCreateTable creates the table if it does not exist.
	// +optional
	AutoCreateTable *bool `json:"autoCreateTable,omitempty"`

	// RawMode when true, creates table with data and _metadata string columns.
	// +optional
	RawMode *bool `json:"rawMode,omitempty"`

	FlattenMetadataSpec `json:",inline"`

	// BasicAuth for Iceberg REST catalog.
	// +optional
	BasicAuth *BasicAuthConfig `json:"basicAuth,omitempty"`

	// BearerToken for Iceberg REST (optional if TokenSecretRef is set).
	// +optional
	BearerToken string `json:"bearerToken,omitempty"`

	// AuthenticationType controls how Authorization header is sent to the REST catalog.
	// +optional
	AuthenticationType IcebergRESTAuthenticationType `json:"authenticationType,omitempty"`

	// OAuth2ServerURI overrides the OAuth2 token endpoint (optional).
	// +optional
	OAuth2ServerURI string `json:"oauth2ServerURI,omitempty"`

	// OAuth2ClientID for client credentials flow.
	// +optional
	OAuth2ClientID string `json:"oauth2ClientID,omitempty"`

	// OAuth2ClientSecret for client credentials flow.
	// +optional
	OAuth2ClientSecret string `json:"oauth2ClientSecret,omitempty"`

	// OAuth2Scope for client credentials flow (default: catalog).
	// +optional
	OAuth2Scope string `json:"oauth2Scope,omitempty"`

	// CatalogURISecretRef references a Kubernetes secret for catalog URI.
	// +optional
	CatalogURISecretRef *SecretRef `json:"catalogURISecretRef,omitempty"`

	// TokenSecretRef references a Kubernetes secret for bearer token.
	// +optional
	TokenSecretRef *SecretRef `json:"tokenSecretRef,omitempty"`

	// OAuth2ServerURISecretRef references a Kubernetes secret for OAuth2 server URI.
	// +optional
	OAuth2ServerURISecretRef *SecretRef `json:"oauth2ServerURISecretRef,omitempty"`

	// OAuth2ClientIDSecretRef references a Kubernetes secret for OAuth2 client ID.
	// +optional
	OAuth2ClientIDSecretRef *SecretRef `json:"oauth2ClientIDSecretRef,omitempty"`

	// OAuth2ClientSecretSecretRef references a Kubernetes secret for OAuth2 client secret.
	// +optional
	OAuth2ClientSecretSecretRef *SecretRef `json:"oauth2ClientSecretSecretRef,omitempty"`

	// NamespaceSecretRef references a Kubernetes secret for namespace.
	// +optional
	NamespaceSecretRef *SecretRef `json:"namespaceSecretRef,omitempty"`

	// TableSecretRef references a Kubernetes secret for table name.
	// +optional
	TableSecretRef *SecretRef `json:"tableSecretRef,omitempty"`

	// S3Endpoint is the S3-compatible API endpoint URL for iceberg-go/AWS SDK.
	// +optional
	S3Endpoint string `json:"s3Endpoint,omitempty"`

	// S3Region is the region passed as AWS_REGION for object storage operations.
	// +optional
	S3Region string `json:"s3Region,omitempty"`

	// AccessKeySecretRef references a Kubernetes Secret key for the S3 access key ID.
	// +optional
	AccessKeySecretRef *SecretRef `json:"accessKeySecretRef,omitempty"`

	// SecretAccessKeySecretRef references a Kubernetes Secret key for the S3 secret access key.
	// +optional
	SecretAccessKeySecretRef *SecretRef `json:"secretAccessKeySecretRef,omitempty"`
}

// FlattenMetadataSpec configures writing msg.Metadata as separate columns instead of a single _metadata field.
// Requires rawMode on the sink. Supported by PostgreSQL, Trino, ClickHouse, Nessie, and Iceberg sinks.
type FlattenMetadataSpec struct {
	// FlattenMetadataColumns when true, writes each metadata key as a separate column instead of _metadata.
	// +optional
	FlattenMetadataColumns *bool `json:"flattenMetadataColumns,omitempty"`

	// FlattenMetadataColumnsPrefix is prepended to metadata keys for column names (e.g. kafka_).
	// +optional
	FlattenMetadataColumnsPrefix string `json:"flattenMetadataColumnsPrefix,omitempty"`
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

	// SecurityProtocol maps to Kafka client property security.protocol.
	// Supported: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
	// If empty, TLS/SASL enable flags are inferred from tls/sasl sections (legacy behavior).
	// +optional
	SecurityProtocol string `json:"securityProtocol,omitempty"`

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

	// UpsertVersionColumn is the column used to compare row versions when UpsertStrategy is ifNewer.
	// On conflict, the existing row is updated only when EXCLUDED.<column> > target.<column>.
	// +optional
	UpsertVersionColumn *string `json:"upsertVersionColumn,omitempty"`

	// UpsertStrategy controls conflict resolution: always (default) updates on every conflict;
	// ifNewer updates only when the incoming version is newer (requires upsertVersionColumn).
	// +optional
	// +kubebuilder:validation:Enum=always;ifNewer
	UpsertStrategy *string `json:"upsertStrategy,omitempty"`

	// SoftDeleteColumn specifies column for soft delete (e.g. "deleted_at"). If set, DELETE operations will UPDATE this column instead of physical delete.
	// +optional
	SoftDeleteColumn *string `json:"softDeleteColumn,omitempty"`

	// RawMode when true, expects messages in format {"value": <data>, "_metadata": {...}} or plain body with msg.Metadata. Table is created with data JSONB and _metadata JSONB columns.
	// When false, table structure is inferred from the first message (replicates source structure).
	// +optional
	RawMode *bool `json:"rawMode,omitempty"`

	FlattenMetadataSpec `json:",inline"`

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

	// QueryTimeoutSeconds bounds a single batch INSERT end-to-end (POST + all nextUri polling).
	// Use larger values for Iceberg/Nessie-heavy writes; 0 or negative falls back to the processor default timeout.
	// +optional
	// +kubebuilder:default=600
	QueryTimeoutSeconds *int32 `json:"queryTimeoutSeconds,omitempty"`

	// AutoCreateTable automatically creates the table if it doesn't exist
	// +optional
	AutoCreateTable *bool `json:"autoCreateTable,omitempty"`

	// UpsertMode enables idempotent writes via MERGE (Iceberg catalog).
	// Requires conflictKey. Retries and replays update existing rows instead of creating duplicates.
	// +optional
	UpsertMode *bool `json:"upsertMode,omitempty"`

	// ConflictKey specifies the column used to match rows in MERGE ON clause.
	// Required when upsertMode is true.
	// +optional
	ConflictKey *string `json:"conflictKey,omitempty"`

	// RawMode when true, creates table with data VARCHAR column (JSON storage).
	// When false (default), uses columnar format matching message keys to table columns.
	// +optional
	RawMode *bool `json:"rawMode,omitempty"`

	FlattenMetadataSpec `json:",inline"`

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

	// UpsertMode enables idempotent writes via ReplacingMergeTree deduplication.
	// Inserts remain INSERT; duplicates are resolved on merge using conflictKey ORDER BY.
	// +optional
	UpsertMode *bool `json:"upsertMode,omitempty"`

	// ConflictKey specifies the deduplication key column for ORDER BY when upsertMode is true.
	// If not set, the first message column (or created_at in raw mode) is used.
	// +optional
	ConflictKey *string `json:"conflictKey,omitempty"`

	// TableEngine selects the MergeTree engine variant (default: MergeTree).
	// When upsertMode is true and tableEngine is unset, ReplacingMergeTree is used for auto-created tables.
	// +optional
	// +kubebuilder:validation:Enum=MergeTree;ReplacingMergeTree
	TableEngine *string `json:"tableEngine,omitempty"`

	// ReplacingVersionColumn is the version column for ReplacingMergeTree(engine).
	// Rows with the highest version value are kept during background merges.
	// +optional
	ReplacingVersionColumn *string `json:"replacingVersionColumn,omitempty"`

	// RawMode when true, creates table with data String and created_at columns (JSON storage).
	// When false (default), creates table from message structure (columnar, replicates source schema).
	// +optional
	RawMode *bool `json:"rawMode,omitempty"`

	FlattenMetadataSpec `json:",inline"`

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
	// Type of transformation: timestamp, flatten, filter, mask, router, select, remove, snakeCase, camelCase, debeziumUnwrap
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

// GetDebeziumUnwrapConfig returns DebeziumUnwrap transformation config.
func (t *TransformationSpec) GetDebeziumUnwrapConfig() (*DebeziumUnwrapTransformation, error) {
	return getTypedConfig[DebeziumUnwrapTransformation](t.Config)
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

// DebeziumUnwrapTransformation unwraps Debezium envelope messages into row payloads.
type DebeziumUnwrapTransformation struct {
	// InferDeleteFromTombstone converts Kafka tombstone records into operation=delete messages using metadata.key JSON.
	// +optional
	InferDeleteFromTombstone bool `json:"inferDeleteFromTombstone,omitempty"`

	// IncludeSourceInMetadata copies payload.source fields into metadata with source_ prefix.
	// +optional
	IncludeSourceInMetadata bool `json:"includeSourceInMetadata,omitempty"`

	// SnapshotOperation defines operation for Debezium snapshot records (op="r"): insert (default) or update.
	// +optional
	SnapshotOperation string `json:"snapshotOperation,omitempty"`
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
