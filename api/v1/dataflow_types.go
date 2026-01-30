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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
}

// SourceSpec defines the source configuration
type SourceSpec struct {
	// Type of source: kafka, postgresql
	Type string `json:"type"`

	// Kafka source configuration
	// +optional
	Kafka *KafkaSourceSpec `json:"kafka,omitempty"`

	// PostgreSQL source configuration
	// +optional
	PostgreSQL *PostgreSQLSourceSpec `json:"postgresql,omitempty"`

	// Trino source configuration
	// +optional
	Trino *TrinoSourceSpec `json:"trino,omitempty"`
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

// SinkSpec defines the sink configuration
type SinkSpec struct {
	// Type of sink: kafka, postgresql
	Type string `json:"type"`

	// Kafka sink configuration
	// +optional
	Kafka *KafkaSinkSpec `json:"kafka,omitempty"`

	// PostgreSQL sink configuration
	// +optional
	PostgreSQL *PostgreSQLSinkSpec `json:"postgresql,omitempty"`

	// Trino sink configuration
	// +optional
	Trino *TrinoSinkSpec `json:"trino,omitempty"`
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

	// BatchSize for batch inserts
	// +optional
	BatchSize *int32 `json:"batchSize,omitempty"`

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

	// AutoCreateTable automatically creates the table if it doesn't exist
	// +optional
	AutoCreateTable *bool `json:"autoCreateTable,omitempty"`

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

// TransformationSpec defines a transformation to apply
type TransformationSpec struct {
	// Type of transformation: timestamp, flatten, filter, mask, router, select, remove, snakeCase, camelCase
	Type string `json:"type"`

	// Timestamp transformation configuration
	// +optional
	Timestamp *TimestampTransformation `json:"timestamp,omitempty"`

	// Flatten transformation configuration
	// +optional
	Flatten *FlattenTransformation `json:"flatten,omitempty"`

	// Filter transformation configuration
	// +optional
	Filter *FilterTransformation `json:"filter,omitempty"`

	// Mask transformation configuration
	// +optional
	Mask *MaskTransformation `json:"mask,omitempty"`

	// Router transformation configuration
	// +optional
	Router *RouterTransformation `json:"router,omitempty"`

	// Select transformation configuration
	// +optional
	Select *SelectTransformation `json:"select,omitempty"`

	// Remove transformation configuration
	// +optional
	Remove *RemoveTransformation `json:"remove,omitempty"`

	// SnakeCase transformation configuration
	// +optional
	SnakeCase *SnakeCaseTransformation `json:"snakeCase,omitempty"`

	// CamelCase transformation configuration
	// +optional
	CamelCase *CamelCaseTransformation `json:"camelCase,omitempty"`
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
