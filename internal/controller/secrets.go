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
	case "iceberg":
		if source.Iceberg != nil {
			if err := r.resolveIcebergSourceSpec(ctx, namespace, source.Iceberg); err != nil {
				return err
			}
		}
	case "rabbitmq":
		if source.RabbitMQ != nil {
			if err := r.resolveRabbitMQSourceSpec(ctx, namespace, source.RabbitMQ); err != nil {
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
	case "iceberg":
		if sink.Iceberg != nil {
			if err := r.resolveIcebergSinkSpec(ctx, namespace, sink.Iceberg); err != nil {
				return err
			}
		}
	case "rabbitmq":
		if sink.RabbitMQ != nil {
			if err := r.resolveRabbitMQSinkSpec(ctx, namespace, sink.RabbitMQ); err != nil {
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
				case "iceberg":
					if routeSink.Iceberg != nil {
						if err := r.resolveIcebergSinkSpec(ctx, namespace, routeSink.Iceberg); err != nil {
							return err
						}
					}
				case "rabbitmq":
					if routeSink.RabbitMQ != nil {
						if err := r.resolveRabbitMQSinkSpec(ctx, namespace, routeSink.RabbitMQ); err != nil {
							return err
						}
					}
				}
			}
		}
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

func (r *SecretResolver) resolveIcebergSourceSpec(ctx context.Context, namespace string, spec *dataflowv1.IcebergSourceSpec) error {
	if spec.RESTCatalogURLSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.RESTCatalogURLSecretRef)
		if err != nil {
			return err
		}
		spec.RESTCatalogURL = value
	}

	if spec.NamespaceSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.NamespaceSecretRef)
		if err != nil {
			return err
		}
		spec.Namespace = value
	}

	if spec.TableSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.TableSecretRef)
		if err != nil {
			return err
		}
		spec.Table = value
	}

	if spec.TokenSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.TokenSecretRef)
		if err != nil {
			return err
		}
		spec.Token = value
	}

	return nil
}

func (r *SecretResolver) resolveIcebergSinkSpec(ctx context.Context, namespace string, spec *dataflowv1.IcebergSinkSpec) error {
	if spec.RESTCatalogURLSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.RESTCatalogURLSecretRef)
		if err != nil {
			return err
		}
		spec.RESTCatalogURL = value
	}

	if spec.NamespaceSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.NamespaceSecretRef)
		if err != nil {
			return err
		}
		spec.Namespace = value
	}

	if spec.TableSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.TableSecretRef)
		if err != nil {
			return err
		}
		spec.Table = value
	}

	if spec.TokenSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.TokenSecretRef)
		if err != nil {
			return err
		}
		spec.Token = value
	}

	return nil
}

func (r *SecretResolver) resolveRabbitMQSourceSpec(ctx context.Context, namespace string, spec *dataflowv1.RabbitMQSourceSpec) error {
	if spec.URLSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.URLSecretRef)
		if err != nil {
			return err
		}
		spec.URL = value
	}

	if spec.QueueSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.QueueSecretRef)
		if err != nil {
			return err
		}
		spec.Queue = value
	}

	if spec.ExchangeSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.ExchangeSecretRef)
		if err != nil {
			return err
		}
		spec.Exchange = value
	}

	if spec.RoutingKeySecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.RoutingKeySecretRef)
		if err != nil {
			return err
		}
		spec.RoutingKey = value
	}

	return nil
}

func (r *SecretResolver) resolveRabbitMQSinkSpec(ctx context.Context, namespace string, spec *dataflowv1.RabbitMQSinkSpec) error {
	if spec.URLSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.URLSecretRef)
		if err != nil {
			return err
		}
		spec.URL = value
	}

	if spec.ExchangeSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.ExchangeSecretRef)
		if err != nil {
			return err
		}
		spec.Exchange = value
	}

	if spec.RoutingKeySecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.RoutingKeySecretRef)
		if err != nil {
			return err
		}
		spec.RoutingKey = value
	}

	if spec.QueueSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.QueueSecretRef)
		if err != nil {
			return err
		}
		spec.Queue = value
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
			return err
		}
		// Check if value is a file path or CA certificate content
		// If it starts with -----BEGIN, it's CA certificate content
		if isCertificateContent(value) {
			// Value is CA certificate content, create temporary file
			tempFile, err := r.createTempFile("ca-", []byte(value))
			if err != nil {
				return fmt.Errorf("failed to create temporary CA file: %w", err)
			}
			config.CAFile = tempFile
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
	}

	return nil
}

// createTempFile creates a temporary file with the given content
func (r *SecretResolver) createTempFile(prefix string, content []byte) (string, error) {
	tempFile, err := os.CreateTemp("", prefix+"*.pem")
	if err != nil {
		return "", err
	}

	if _, err := tempFile.Write(content); err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return "", err
	}

	if err := tempFile.Close(); err != nil {
		os.Remove(tempFile.Name())
		return "", err
	}

	// Track the temporary file for cleanup
	r.tempFilesMu.Lock()
	r.tempFiles = append(r.tempFiles, tempFile.Name())
	r.tempFilesMu.Unlock()

	return tempFile.Name(), nil
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
