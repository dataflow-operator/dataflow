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

package controller

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
)

// SecretResolver resolves values from Kubernetes secrets
type SecretResolver struct {
	client      client.Client
	tempFiles   []string // Track temporary files for cleanup
	tempFilesMu sync.Mutex
}

// NewSecretResolver creates a new secret resolver
func NewSecretResolver(client client.Client) *SecretResolver {
	return &SecretResolver{
		client: client,
	}
}

// ResolveSecretValue reads a value from a Kubernetes secret
func (r *SecretResolver) ResolveSecretValue(ctx context.Context, namespace string, ref *dataflowv1.SecretRef) (string, error) {
	if ref == nil {
		return "", fmt.Errorf("secret reference is nil")
	}

	secretNamespace := ref.Namespace
	if secretNamespace == "" {
		secretNamespace = namespace
	}

	var secret corev1.Secret
	secretKey := types.NamespacedName{
		Name:      ref.Name,
		Namespace: secretNamespace,
	}

	if err := r.client.Get(ctx, secretKey, &secret); err != nil {
		return "", fmt.Errorf("failed to get secret %s/%s: %w", secretNamespace, ref.Name, err)
	}

	value, ok := secret.Data[ref.Key]
	if !ok {
		return "", fmt.Errorf("key %s not found in secret %s/%s", ref.Key, secretNamespace, ref.Name)
	}

	return string(value), nil
}

// ResolveDataFlowSpec resolves all secret references in a DataFlow spec
func (r *SecretResolver) ResolveDataFlowSpec(ctx context.Context, namespace string, spec *dataflowv1.DataFlowSpec) (*dataflowv1.DataFlowSpec, error) {
	// Create a deep copy to avoid modifying the original
	resolved := spec.DeepCopy()

	// Resolve source secrets
	if err := r.resolveSourceSpec(ctx, namespace, resolved); err != nil {
		return nil, fmt.Errorf("failed to resolve source secrets: %w", err)
	}

	// Resolve sink secrets
	if err := r.resolveSinkSpec(ctx, namespace, resolved); err != nil {
		return nil, fmt.Errorf("failed to resolve sink secrets: %w", err)
	}

	return resolved, nil
}

func (r *SecretResolver) resolveSourceSpec(ctx context.Context, namespace string, spec *dataflowv1.DataFlowSpec) error {
	source := &spec.Source

	switch source.Type {
	case "kafka":
		if source.Kafka != nil {
			if err := r.resolveKafkaSourceSpec(ctx, namespace, source.Kafka); err != nil {
				return err
			}
		}
	case "postgresql":
		if source.PostgreSQL != nil {
			if err := r.resolvePostgreSQLSourceSpec(ctx, namespace, source.PostgreSQL); err != nil {
				return err
			}
		}
	case "trino":
		if source.Trino != nil {
			if err := r.resolveTrinoSourceSpec(ctx, namespace, source.Trino); err != nil {
				return err
			}
		}
	case "clickhouse":
		if source.ClickHouse != nil {
			if err := r.resolveClickHouseSourceSpec(ctx, namespace, source.ClickHouse); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *SecretResolver) resolveSinkSpec(ctx context.Context, namespace string, spec *dataflowv1.DataFlowSpec) error {
	sink := &spec.Sink

	switch sink.Type {
	case "kafka":
		if sink.Kafka != nil {
			if err := r.resolveKafkaSinkSpec(ctx, namespace, sink.Kafka); err != nil {
				return err
			}
		}
	case "postgresql":
		if sink.PostgreSQL != nil {
			if err := r.resolvePostgreSQLSinkSpec(ctx, namespace, sink.PostgreSQL); err != nil {
				return err
			}
		}
	case "trino":
		if sink.Trino != nil {
			if err := r.resolveTrinoSinkSpec(ctx, namespace, sink.Trino); err != nil {
				return err
			}
		}
	case "clickhouse":
		if sink.ClickHouse != nil {
			if err := r.resolveClickHouseSinkSpec(ctx, namespace, sink.ClickHouse); err != nil {
				return err
			}
		}
	}

	// Resolve router sink secrets
	for i := range spec.Transformations {
		if spec.Transformations[i].Type == "router" && spec.Transformations[i].Router != nil {
			for j := range spec.Transformations[i].Router.Routes {
				routeSink := &spec.Transformations[i].Router.Routes[j].Sink
				switch routeSink.Type {
				case "kafka":
					if routeSink.Kafka != nil {
						if err := r.resolveKafkaSinkSpec(ctx, namespace, routeSink.Kafka); err != nil {
							return err
						}
					}
				case "postgresql":
					if routeSink.PostgreSQL != nil {
						if err := r.resolvePostgreSQLSinkSpec(ctx, namespace, routeSink.PostgreSQL); err != nil {
							return err
						}
					}
				case "trino":
					if routeSink.Trino != nil {
						if err := r.resolveTrinoSinkSpec(ctx, namespace, routeSink.Trino); err != nil {
							return err
						}
					}
				case "clickhouse":
					if routeSink.ClickHouse != nil {
						if err := r.resolveClickHouseSinkSpec(ctx, namespace, routeSink.ClickHouse); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil
}

func (r *SecretResolver) resolveClickHouseSourceSpec(ctx context.Context, namespace string, spec *dataflowv1.ClickHouseSourceSpec) error {
	if spec.ConnectionStringSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.ConnectionStringSecretRef)
		if err != nil {
			return err
		}
		spec.ConnectionString = value
	}

	if spec.TableSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.TableSecretRef)
		if err != nil {
			return err
		}
		spec.Table = value
	}

	return nil
}

func (r *SecretResolver) resolveClickHouseSinkSpec(ctx context.Context, namespace string, spec *dataflowv1.ClickHouseSinkSpec) error {
	if spec.ConnectionStringSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.ConnectionStringSecretRef)
		if err != nil {
			return err
		}
		spec.ConnectionString = value
	}

	if spec.TableSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.TableSecretRef)
		if err != nil {
			return err
		}
		spec.Table = value
	}

	return nil
}

func (r *SecretResolver) resolveKafkaSourceSpec(ctx context.Context, namespace string, spec *dataflowv1.KafkaSourceSpec) error {
	if spec.BrokersSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.BrokersSecretRef)
		if err != nil {
			return err
		}
		// Parse comma-separated brokers
		brokers := []string{}
		for _, broker := range strings.Split(value, ",") {
			broker = strings.TrimSpace(broker)
			if broker != "" {
				brokers = append(brokers, broker)
			}
		}
		spec.Brokers = brokers
	}

	if spec.TopicSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.TopicSecretRef)
		if err != nil {
			return err
		}
		spec.Topic = value
	}

	if spec.ConsumerGroupSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.ConsumerGroupSecretRef)
		if err != nil {
			return err
		}
		spec.ConsumerGroup = value
	}

	if spec.TLS != nil {
		if err := r.resolveTLSConfig(ctx, namespace, spec.TLS); err != nil {
			return err
		}
	}

	if spec.SASL != nil {
		if err := r.resolveSASLConfig(ctx, namespace, spec.SASL); err != nil {
			return err
		}
	}

	// Resolve Schema Registry secrets
	if spec.SchemaRegistry != nil {
		if err := r.resolveSchemaRegistryConfig(ctx, namespace, spec.SchemaRegistry); err != nil {
			return err
		}
	}

	return nil
}

// resolveSchemaRegistryConfig resolves secrets for Schema Registry configuration
func (r *SecretResolver) resolveSchemaRegistryConfig(ctx context.Context, namespace string, config *dataflowv1.SchemaRegistryConfig) error {
	// Resolve URL from secret if provided
	if config.URLSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, config.URLSecretRef)
		if err != nil {
			return err
		}
		config.URL = value
	}

	// Resolve BasicAuth secrets if provided
	if config.BasicAuth != nil {
		if config.BasicAuth.UsernameSecretRef != nil {
			value, err := r.ResolveSecretValue(ctx, namespace, config.BasicAuth.UsernameSecretRef)
			if err != nil {
				return err
			}
			config.BasicAuth.Username = value
		}

		if config.BasicAuth.PasswordSecretRef != nil {
			value, err := r.ResolveSecretValue(ctx, namespace, config.BasicAuth.PasswordSecretRef)
			if err != nil {
				return err
			}
			config.BasicAuth.Password = value
		}
	}

	// Resolve TLS config if provided
	if config.TLS != nil {
		if err := r.resolveTLSConfig(ctx, namespace, config.TLS); err != nil {
			return err
		}
	}

	return nil
}

func (r *SecretResolver) resolveKafkaSinkSpec(ctx context.Context, namespace string, spec *dataflowv1.KafkaSinkSpec) error {
	if spec.BrokersSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.BrokersSecretRef)
		if err != nil {
			return err
		}
		// Parse comma-separated brokers
		brokers := []string{}
		for _, broker := range strings.Split(value, ",") {
			broker = strings.TrimSpace(broker)
			if broker != "" {
				brokers = append(brokers, broker)
			}
		}
		spec.Brokers = brokers
	}

	if spec.TopicSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.TopicSecretRef)
		if err != nil {
			return err
		}
		spec.Topic = value
	}

	if spec.TLS != nil {
		if err := r.resolveTLSConfig(ctx, namespace, spec.TLS); err != nil {
			return err
		}
	}

	if spec.SASL != nil {
		if err := r.resolveSASLConfig(ctx, namespace, spec.SASL); err != nil {
			return err
		}
	}

	return nil
}

func (r *SecretResolver) resolvePostgreSQLSourceSpec(ctx context.Context, namespace string, spec *dataflowv1.PostgreSQLSourceSpec) error {
	if spec.ConnectionStringSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.ConnectionStringSecretRef)
		if err != nil {
			return err
		}
		spec.ConnectionString = value
	}

	if spec.TableSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.TableSecretRef)
		if err != nil {
			return err
		}
		spec.Table = value
	}

	return nil
}

func (r *SecretResolver) resolvePostgreSQLSinkSpec(ctx context.Context, namespace string, spec *dataflowv1.PostgreSQLSinkSpec) error {
	if spec.ConnectionStringSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.ConnectionStringSecretRef)
		if err != nil {
			return err
		}
		spec.ConnectionString = value
	}

	if spec.TableSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.TableSecretRef)
		if err != nil {
			return err
		}
		spec.Table = value
	}

	return nil
}

// isCertificateContent checks if the value is certificate/key content (starts with -----BEGIN)
// rather than a file path
func isCertificateContent(value string) bool {
	trimmed := strings.TrimSpace(value)
	return strings.HasPrefix(trimmed, "-----BEGIN")
}

func (r *SecretResolver) resolveTLSConfig(ctx context.Context, namespace string, config *dataflowv1.TLSConfig) error {
	if config.CertSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, config.CertSecretRef)
		if err != nil {
			return err
		}
		// Check if value is a file path or certificate content
		// If it starts with -----BEGIN, it's certificate content
		if isCertificateContent(value) {
			// Value is certificate content, create temporary file
			tempFile, err := r.createTempFile("cert-", []byte(value))
			if err != nil {
				return fmt.Errorf("failed to create temporary cert file: %w", err)
			}
			config.CertFile = tempFile
		} else {
			// Check if it's a valid file path
			if _, err := os.Stat(value); err == nil {
				// Value is a file path
				config.CertFile = value
			} else {
				// Treat as certificate content if file doesn't exist
				tempFile, err := r.createTempFile("cert-", []byte(value))
				if err != nil {
					return fmt.Errorf("failed to create temporary cert file: %w", err)
				}
				config.CertFile = tempFile
			}
		}
	}

	if config.KeySecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, config.KeySecretRef)
		if err != nil {
			return err
		}
		// Check if value is a file path or key content
		// If it starts with -----BEGIN, it's key content
		if isCertificateContent(value) {
			// Value is key content, create temporary file
			tempFile, err := r.createTempFile("key-", []byte(value))
			if err != nil {
				return fmt.Errorf("failed to create temporary key file: %w", err)
			}
			config.KeyFile = tempFile
		} else {
			// Check if it's a valid file path
			if _, err := os.Stat(value); err == nil {
				// Value is a file path
				config.KeyFile = value
			} else {
				// Treat as key content if file doesn't exist
				tempFile, err := r.createTempFile("key-", []byte(value))
				if err != nil {
					return fmt.Errorf("failed to create temporary key file: %w", err)
				}
				config.KeyFile = tempFile
			}
		}
	}

	if config.CASecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, config.CASecretRef)
		if err != nil {
			return fmt.Errorf("failed to resolve CA secret %s/%s key %s: %w",
				config.CASecretRef.Namespace, config.CASecretRef.Name, config.CASecretRef.Key, err)
		}
		// Check if value is certificate content
		isContent := isCertificateContent(value)
		// Check if value is a file path or CA certificate content
		// If it starts with -----BEGIN, it's CA certificate content
		if isContent {
			// Value is CA certificate content, create temporary file
			tempFile, err := r.createTempFile("ca-", []byte(value))
			if err != nil {
				return fmt.Errorf("failed to create temporary CA file: %w", err)
			}
			config.CAFile = tempFile
			// Log successful creation
			if stat, err := os.Stat(tempFile); err == nil {
				_ = stat // File exists and has size
			}
		} else {
			// Check if it's a valid file path
			if _, err := os.Stat(value); err == nil {
				// Value is a file path
				config.CAFile = value
			} else {
				// Treat as CA certificate content if file doesn't exist
				tempFile, err := r.createTempFile("ca-", []byte(value))
				if err != nil {
					return fmt.Errorf("failed to create temporary CA file: %w", err)
				}
				config.CAFile = tempFile
			}
		}
		// Verify file exists and is readable
		if config.CAFile != "" {
			if stat, err := os.Stat(config.CAFile); err != nil {
				return fmt.Errorf("CA file %s does not exist or is not readable: %w", config.CAFile, err)
			} else if stat.Size() == 0 {
				return fmt.Errorf("CA file %s is empty", config.CAFile)
			}
		}
	}

	return nil
}

// createTempFile creates a temporary file with the given content
func (r *SecretResolver) createTempFile(prefix string, content []byte) (string, error) {
	tempFile, err := os.CreateTemp("", prefix+"*.pem")
	if err != nil {
		return "", err
	}

	// Save the file name before closing
	fileName := tempFile.Name()

	if _, err := tempFile.Write(content); err != nil {
		tempFile.Close()
		os.Remove(fileName)
		return "", err
	}

	if err := tempFile.Close(); err != nil {
		os.Remove(fileName)
		return "", err
	}

	// Track the temporary file for cleanup
	r.tempFilesMu.Lock()
	r.tempFiles = append(r.tempFiles, fileName)
	r.tempFilesMu.Unlock()

	return fileName, nil
}

// CleanupTempFiles removes all temporary files created by the resolver
func (r *SecretResolver) CleanupTempFiles() error {
	r.tempFilesMu.Lock()
	defer r.tempFilesMu.Unlock()

	var errors []string
	for _, file := range r.tempFiles {
		if err := os.Remove(file); err != nil {
			errors = append(errors, fmt.Sprintf("failed to remove %s: %v", file, err))
		}
	}
	r.tempFiles = nil

	if len(errors) > 0 {
		return fmt.Errorf("cleanup errors: %s", strings.Join(errors, "; "))
	}
	return nil
}

func (r *SecretResolver) resolveSASLConfig(ctx context.Context, namespace string, config *dataflowv1.SASLConfig) error {
	// Validate that username is provided either directly or via secret reference
	if config.Username == "" && config.UsernameSecretRef == nil {
		return fmt.Errorf("SASL username is required: either 'username' or 'usernameSecretRef' must be specified")
	}

	// Validate that password is provided either directly or via secret reference
	if config.Password == "" && config.PasswordSecretRef == nil {
		return fmt.Errorf("SASL password is required: either 'password' or 'passwordSecretRef' must be specified")
	}

	// Resolve username from secret if secret reference is provided
	if config.UsernameSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, config.UsernameSecretRef)
		if err != nil {
			return err
		}
		config.Username = value
	}

	// Resolve password from secret if secret reference is provided
	if config.PasswordSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, config.PasswordSecretRef)
		if err != nil {
			return err
		}
		config.Password = value
	}

	return nil
}

func (r *SecretResolver) resolveTrinoSourceSpec(ctx context.Context, namespace string, spec *dataflowv1.TrinoSourceSpec) error {
	if spec.ServerURLSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.ServerURLSecretRef)
		if err != nil {
			return err
		}
		spec.ServerURL = value
	}

	if spec.CatalogSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.CatalogSecretRef)
		if err != nil {
			return err
		}
		spec.Catalog = value
	}

	if spec.SchemaSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.SchemaSecretRef)
		if err != nil {
			return err
		}
		spec.Schema = value
	}

	if spec.TableSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.TableSecretRef)
		if err != nil {
			return err
		}
		spec.Table = value
	}

	// Resolve Keycloak secrets if provided
	if spec.Keycloak != nil {
		if err := r.resolveKeycloakConfig(ctx, namespace, spec.Keycloak); err != nil {
			return err
		}
	}

	return nil
}

func (r *SecretResolver) resolveTrinoSinkSpec(ctx context.Context, namespace string, spec *dataflowv1.TrinoSinkSpec) error {
	if spec.ServerURLSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.ServerURLSecretRef)
		if err != nil {
			return err
		}
		spec.ServerURL = value
	}

	if spec.CatalogSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.CatalogSecretRef)
		if err != nil {
			return err
		}
		spec.Catalog = value
	}

	if spec.SchemaSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.SchemaSecretRef)
		if err != nil {
			return err
		}
		spec.Schema = value
	}

	if spec.TableSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.TableSecretRef)
		if err != nil {
			return err
		}
		spec.Table = value
	}

	// Resolve Keycloak secrets if provided
	if spec.Keycloak != nil {
		if err := r.resolveKeycloakConfig(ctx, namespace, spec.Keycloak); err != nil {
			return err
		}
	}

	return nil
}

func (r *SecretResolver) resolveKeycloakConfig(ctx context.Context, namespace string, config *dataflowv1.KeycloakConfig) error {
	if config.ServerURLSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, config.ServerURLSecretRef)
		if err != nil {
			return err
		}
		config.ServerURL = value
	}

	if config.RealmSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, config.RealmSecretRef)
		if err != nil {
			return err
		}
		config.Realm = value
	}

	if config.ClientIDSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, config.ClientIDSecretRef)
		if err != nil {
			return err
		}
		config.ClientID = value
	}

	if config.ClientSecretSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, config.ClientSecretSecretRef)
		if err != nil {
			return err
		}
		config.ClientSecret = value
	}

	if config.UsernameSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, config.UsernameSecretRef)
		if err != nil {
			return err
		}
		config.Username = value
	}

	if config.PasswordSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, config.PasswordSecretRef)
		if err != nil {
			return err
		}
		config.Password = value
	}

	if config.TokenSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, config.TokenSecretRef)
		if err != nil {
			return err
		}
		config.Token = value
	}

	return nil
}
