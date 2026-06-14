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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
)

func unmarshalConfig(cfg *runtime.RawExtension, dst interface{}) error {
	if cfg == nil || len(cfg.Raw) == 0 {
		return nil
	}
	return json.Unmarshal(cfg.Raw, dst)
}

func marshalConfig(cfg *runtime.RawExtension, src interface{}) error {
	if cfg == nil {
		return nil
	}
	b, err := json.Marshal(src)
	if err != nil {
		return err
	}
	cfg.Raw = b
	return nil
}

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

	// Resolve errors sink secrets if specified
	if resolved.Errors != nil {
		if err := r.resolveSinkSpecRecursive(ctx, namespace, &resolved.Errors.SinkSpec); err != nil {
			return nil, fmt.Errorf("failed to resolve errors sink secrets: %w", err)
		}
	}

	return resolved, nil
}

func (r *SecretResolver) resolveSinkSpecRecursive(ctx context.Context, namespace string, sink *dataflowv1.SinkSpec) error {
	// Resolve main sink config
	if sink.Config != nil && len(sink.Config.Raw) > 0 {
		switch sink.Type {
		case "kafka":
			var cfg dataflowv1.KafkaSinkSpec
			if err := unmarshalConfig(sink.Config, &cfg); err != nil {
				return err
			}
			if err := r.resolveKafkaSinkSpec(ctx, namespace, &cfg); err != nil {
				return err
			}
			return marshalConfig(sink.Config, &cfg)
		case "postgresql":
			var cfg dataflowv1.PostgreSQLSinkSpec
			if err := unmarshalConfig(sink.Config, &cfg); err != nil {
				return err
			}
			if err := r.resolvePostgreSQLSinkSpec(ctx, namespace, &cfg); err != nil {
				return err
			}
			return marshalConfig(sink.Config, &cfg)
		case "trino":
			var cfg dataflowv1.TrinoSinkSpec
			if err := unmarshalConfig(sink.Config, &cfg); err != nil {
				return err
			}
			if err := r.resolveTrinoSinkSpec(ctx, namespace, &cfg); err != nil {
				return err
			}
			return marshalConfig(sink.Config, &cfg)
		case "clickhouse":
			var cfg dataflowv1.ClickHouseSinkSpec
			if err := unmarshalConfig(sink.Config, &cfg); err != nil {
				return err
			}
			if err := r.resolveClickHouseSinkSpec(ctx, namespace, &cfg); err != nil {
				return err
			}
			return marshalConfig(sink.Config, &cfg)
		case "nessie":
			var cfg dataflowv1.NessieSinkSpec
			if err := unmarshalConfig(sink.Config, &cfg); err != nil {
				return err
			}
			if err := r.resolveNessieSinkSpec(ctx, namespace, &cfg); err != nil {
				return err
			}
			return marshalConfig(sink.Config, &cfg)
		case "iceberg":
			var cfg dataflowv1.IcebergSinkSpec
			if err := unmarshalConfig(sink.Config, &cfg); err != nil {
				return err
			}
			if err := r.resolveIcebergSinkSpec(ctx, namespace, &cfg); err != nil {
				return err
			}
			return marshalConfig(sink.Config, &cfg)
		}
	}
	return nil
}

func (r *SecretResolver) resolveSourceSpec(ctx context.Context, namespace string, spec *dataflowv1.DataFlowSpec) error {
	source := &spec.Source

	// When Config is set, unmarshal -> resolve -> marshal
	if source.Config != nil && len(source.Config.Raw) > 0 {
		switch source.Type {
		case "kafka":
			var cfg dataflowv1.KafkaSourceSpec
			if err := unmarshalConfig(source.Config, &cfg); err != nil {
				return err
			}
			if err := r.resolveKafkaSourceSpec(ctx, namespace, &cfg); err != nil {
				return err
			}
			return marshalConfig(source.Config, &cfg)
		case "postgresql":
			var cfg dataflowv1.PostgreSQLSourceSpec
			if err := unmarshalConfig(source.Config, &cfg); err != nil {
				return err
			}
			if err := r.resolvePostgreSQLSourceSpec(ctx, namespace, &cfg); err != nil {
				return err
			}
			return marshalConfig(source.Config, &cfg)
		case "postgresql-cdc":
			var cfg dataflowv1.PostgreSQLCDCSourceSpec
			if err := unmarshalConfig(source.Config, &cfg); err != nil {
				return err
			}
			if err := r.resolvePostgreSQLCDCSourceSpec(ctx, namespace, &cfg); err != nil {
				return err
			}
			return marshalConfig(source.Config, &cfg)
		case "trino":
			var cfg dataflowv1.TrinoSourceSpec
			if err := unmarshalConfig(source.Config, &cfg); err != nil {
				return err
			}
			if err := r.resolveTrinoSourceSpec(ctx, namespace, &cfg); err != nil {
				return err
			}
			return marshalConfig(source.Config, &cfg)
		case "clickhouse":
			var cfg dataflowv1.ClickHouseSourceSpec
			if err := unmarshalConfig(source.Config, &cfg); err != nil {
				return err
			}
			if err := r.resolveClickHouseSourceSpec(ctx, namespace, &cfg); err != nil {
				return err
			}
			return marshalConfig(source.Config, &cfg)
		case "nessie":
			var cfg dataflowv1.NessieSourceSpec
			if err := unmarshalConfig(source.Config, &cfg); err != nil {
				return err
			}
			if err := r.resolveNessieSourceSpec(ctx, namespace, &cfg); err != nil {
				return err
			}
			return marshalConfig(source.Config, &cfg)
		case "iceberg":
			var cfg dataflowv1.IcebergSourceSpec
			if err := unmarshalConfig(source.Config, &cfg); err != nil {
				return err
			}
			if err := r.resolveIcebergSourceSpec(ctx, namespace, &cfg); err != nil {
				return err
			}
			return marshalConfig(source.Config, &cfg)
		}
	}
	return nil
}

func (r *SecretResolver) resolveSinkSpec(ctx context.Context, namespace string, spec *dataflowv1.DataFlowSpec) error {
	sink := &spec.Sink

	// When Config is set, unmarshal -> resolve -> marshal
	if sink.Config != nil && len(sink.Config.Raw) > 0 {
		switch sink.Type {
		case "kafka":
			var cfg dataflowv1.KafkaSinkSpec
			if err := unmarshalConfig(sink.Config, &cfg); err != nil {
				return err
			}
			if err := r.resolveKafkaSinkSpec(ctx, namespace, &cfg); err != nil {
				return err
			}
			if err := marshalConfig(sink.Config, &cfg); err != nil {
				return err
			}
		case "postgresql":
			var cfg dataflowv1.PostgreSQLSinkSpec
			if err := unmarshalConfig(sink.Config, &cfg); err != nil {
				return err
			}
			if err := r.resolvePostgreSQLSinkSpec(ctx, namespace, &cfg); err != nil {
				return err
			}
			if err := marshalConfig(sink.Config, &cfg); err != nil {
				return err
			}
		case "trino":
			var cfg dataflowv1.TrinoSinkSpec
			if err := unmarshalConfig(sink.Config, &cfg); err != nil {
				return err
			}
			if err := r.resolveTrinoSinkSpec(ctx, namespace, &cfg); err != nil {
				return err
			}
			if err := marshalConfig(sink.Config, &cfg); err != nil {
				return err
			}
		case "clickhouse":
			var cfg dataflowv1.ClickHouseSinkSpec
			if err := unmarshalConfig(sink.Config, &cfg); err != nil {
				return err
			}
			if err := r.resolveClickHouseSinkSpec(ctx, namespace, &cfg); err != nil {
				return err
			}
			if err := marshalConfig(sink.Config, &cfg); err != nil {
				return err
			}
		case "nessie":
			var cfg dataflowv1.NessieSinkSpec
			if err := unmarshalConfig(sink.Config, &cfg); err != nil {
				return err
			}
			if err := r.resolveNessieSinkSpec(ctx, namespace, &cfg); err != nil {
				return err
			}
			if err := marshalConfig(sink.Config, &cfg); err != nil {
				return err
			}
		case "iceberg":
			var cfg dataflowv1.IcebergSinkSpec
			if err := unmarshalConfig(sink.Config, &cfg); err != nil {
				return err
			}
			if err := r.resolveIcebergSinkSpec(ctx, namespace, &cfg); err != nil {
				return err
			}
			if err := marshalConfig(sink.Config, &cfg); err != nil {
				return err
			}
		}
	}
	return r.resolveRouterSinks(ctx, namespace, spec)
}

func (r *SecretResolver) resolveRouterSinks(ctx context.Context, namespace string, spec *dataflowv1.DataFlowSpec) error {
	for i := range spec.Transformations {
		t := &spec.Transformations[i]
		if t.Type != "router" {
			continue
		}
		routerCfg, err := t.GetRouterConfig()
		if err != nil || routerCfg == nil {
			continue
		}
		for j := range routerCfg.Routes {
			routeSink := &routerCfg.Routes[j].Sink
			if routeSink.Config != nil && len(routeSink.Config.Raw) > 0 {
				switch routeSink.Type {
				case "kafka":
					var cfg dataflowv1.KafkaSinkSpec
					if err := unmarshalConfig(routeSink.Config, &cfg); err != nil {
						return err
					}
					if err := r.resolveKafkaSinkSpec(ctx, namespace, &cfg); err != nil {
						return err
					}
					if err := marshalConfig(routeSink.Config, &cfg); err != nil {
						return err
					}
				case "postgresql":
					var cfg dataflowv1.PostgreSQLSinkSpec
					if err := unmarshalConfig(routeSink.Config, &cfg); err != nil {
						return err
					}
					if err := r.resolvePostgreSQLSinkSpec(ctx, namespace, &cfg); err != nil {
						return err
					}
					if err := marshalConfig(routeSink.Config, &cfg); err != nil {
						return err
					}
				case "trino":
					var cfg dataflowv1.TrinoSinkSpec
					if err := unmarshalConfig(routeSink.Config, &cfg); err != nil {
						return err
					}
					if err := r.resolveTrinoSinkSpec(ctx, namespace, &cfg); err != nil {
						return err
					}
					if err := marshalConfig(routeSink.Config, &cfg); err != nil {
						return err
					}
				case "clickhouse":
					var cfg dataflowv1.ClickHouseSinkSpec
					if err := unmarshalConfig(routeSink.Config, &cfg); err != nil {
						return err
					}
					if err := r.resolveClickHouseSinkSpec(ctx, namespace, &cfg); err != nil {
						return err
					}
					if err := marshalConfig(routeSink.Config, &cfg); err != nil {
						return err
					}
				case "nessie":
					var cfg dataflowv1.NessieSinkSpec
					if err := unmarshalConfig(routeSink.Config, &cfg); err != nil {
						return err
					}
					if err := r.resolveNessieSinkSpec(ctx, namespace, &cfg); err != nil {
						return err
					}
					if err := marshalConfig(routeSink.Config, &cfg); err != nil {
						return err
					}
				case "iceberg":
					var cfg dataflowv1.IcebergSinkSpec
					if err := unmarshalConfig(routeSink.Config, &cfg); err != nil {
						return err
					}
					if err := r.resolveIcebergSinkSpec(ctx, namespace, &cfg); err != nil {
						return err
					}
					if err := marshalConfig(routeSink.Config, &cfg); err != nil {
						return err
					}
				}
			}
		}
		// When Config was the source, marshal routerCfg back (we modified a copy)
		if t.Config != nil && len(t.Config.Raw) > 0 {
			if err := marshalConfig(t.Config, routerCfg); err != nil {
				return err
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

func (r *SecretResolver) resolvePostgreSQLCDCSourceSpec(ctx context.Context, namespace string, spec *dataflowv1.PostgreSQLCDCSourceSpec) error {
	if spec.ConnectionStringSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.ConnectionStringSecretRef)
		if err != nil {
			return err
		}
		spec.ConnectionString = value
	}
	if spec.SlotNameSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.SlotNameSecretRef)
		if err != nil {
			return err
		}
		spec.SlotName = value
	}
	if spec.PublicationNameSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.PublicationNameSecretRef)
		if err != nil {
			return err
		}
		spec.PublicationName = value
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

// resolveTLSField resolves a secret reference for a TLS field (cert, key, or CA) into a file path.
func (r *SecretResolver) resolveTLSField(ctx context.Context, namespace string, secretRef *dataflowv1.SecretRef, fieldName string) (string, error) {
	value, err := r.ResolveSecretValue(ctx, namespace, secretRef)
	if err != nil {
		return "", err
	}
	return r.ensureTLSFile(ctx, value, fieldName)
}

// ensureTLSFile materializes a TLS-related value (cert, key, or CA) into a file path.
// If the value is certificate content or doesn't point to an existing file, a temp file is created.
func (r *SecretResolver) ensureTLSFile(ctx context.Context, value, fieldName string) (string, error) {
	if isCertificateContent(value) {
		tempFile, err := r.createTempFile(fieldName+"-", []byte(value))
		if err != nil {
			return "", fmt.Errorf("failed to create temporary %s file: %w", fieldName, err)
		}
		return tempFile, nil
	}
	if _, err := os.Stat(value); err == nil {
		return value, nil
	}
	tempFile, err := r.createTempFile(fieldName+"-", []byte(value))
	if err != nil {
		return "", fmt.Errorf("failed to create temporary %s file: %w", fieldName, err)
	}
	return tempFile, nil
}

func (r *SecretResolver) resolveTLSConfig(ctx context.Context, namespace string, config *dataflowv1.TLSConfig) error {
	var err error

	if config.CertSecretRef != nil {
		if config.CertFile, err = r.resolveTLSField(ctx, namespace, config.CertSecretRef, "cert"); err != nil {
			return err
		}
	}

	if config.KeySecretRef != nil {
		if config.KeyFile, err = r.resolveTLSField(ctx, namespace, config.KeySecretRef, "key"); err != nil {
			return err
		}
	}

	if config.CASecretRef != nil {
		if config.CAFile, err = r.resolveTLSField(ctx, namespace, config.CASecretRef, "CA"); err != nil {
			return err
		}
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

func (r *SecretResolver) resolveNessieSourceSpec(ctx context.Context, namespace string, spec *dataflowv1.NessieSourceSpec) error {
	if spec.BaseURLSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.BaseURLSecretRef)
		if err != nil {
			return err
		}
		spec.BaseURL = value
	}
	if spec.TokenSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.TokenSecretRef)
		if err != nil {
			return err
		}
		spec.BearerToken = value
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
	if spec.BasicAuth != nil {
		if spec.BasicAuth.UsernameSecretRef != nil {
			value, err := r.ResolveSecretValue(ctx, namespace, spec.BasicAuth.UsernameSecretRef)
			if err != nil {
				return err
			}
			spec.BasicAuth.Username = value
		}
		if spec.BasicAuth.PasswordSecretRef != nil {
			value, err := r.ResolveSecretValue(ctx, namespace, spec.BasicAuth.PasswordSecretRef)
			if err != nil {
				return err
			}
			spec.BasicAuth.Password = value
		}
	}
	return nil
}

func (r *SecretResolver) resolveNessieSinkSpec(ctx context.Context, namespace string, spec *dataflowv1.NessieSinkSpec) error {
	// S3 object storage credentials (AccessKeySecretRef, SecretAccessKeySecretRef) are intentionally not resolved here:
	// resolved values must not be written to the spec ConfigMap; the controller injects them via pod env + SecretKeyRef.
	if spec.BaseURLSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.BaseURLSecretRef)
		if err != nil {
			return err
		}
		spec.BaseURL = value
	}
	if spec.TokenSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.TokenSecretRef)
		if err != nil {
			return err
		}
		spec.BearerToken = value
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
	if spec.BasicAuth != nil {
		if spec.BasicAuth.UsernameSecretRef != nil {
			value, err := r.ResolveSecretValue(ctx, namespace, spec.BasicAuth.UsernameSecretRef)
			if err != nil {
				return err
			}
			spec.BasicAuth.Username = value
		}
		if spec.BasicAuth.PasswordSecretRef != nil {
			value, err := r.ResolveSecretValue(ctx, namespace, spec.BasicAuth.PasswordSecretRef)
			if err != nil {
				return err
			}
			spec.BasicAuth.Password = value
		}
	}
	return nil
}

func (r *SecretResolver) resolveIcebergSourceSpec(ctx context.Context, namespace string, spec *dataflowv1.IcebergSourceSpec) error {
	if spec.CatalogURISecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.CatalogURISecretRef)
		if err != nil {
			return err
		}
		spec.CatalogURI = value
	}
	if spec.TokenSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.TokenSecretRef)
		if err != nil {
			return err
		}
		spec.BearerToken = value
	}
	if spec.OAuth2ServerURISecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.OAuth2ServerURISecretRef)
		if err != nil {
			return err
		}
		spec.OAuth2ServerURI = value
	}
	if spec.OAuth2ClientIDSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.OAuth2ClientIDSecretRef)
		if err != nil {
			return err
		}
		spec.OAuth2ClientID = value
	}
	if spec.OAuth2ClientSecretSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.OAuth2ClientSecretSecretRef)
		if err != nil {
			return err
		}
		spec.OAuth2ClientSecret = value
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
	if spec.BasicAuth != nil {
		if spec.BasicAuth.UsernameSecretRef != nil {
			value, err := r.ResolveSecretValue(ctx, namespace, spec.BasicAuth.UsernameSecretRef)
			if err != nil {
				return err
			}
			spec.BasicAuth.Username = value
		}
		if spec.BasicAuth.PasswordSecretRef != nil {
			value, err := r.ResolveSecretValue(ctx, namespace, spec.BasicAuth.PasswordSecretRef)
			if err != nil {
				return err
			}
			spec.BasicAuth.Password = value
		}
	}
	return nil
}

func (r *SecretResolver) resolveIcebergSinkSpec(ctx context.Context, namespace string, spec *dataflowv1.IcebergSinkSpec) error {
	if spec.CatalogURISecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.CatalogURISecretRef)
		if err != nil {
			return err
		}
		spec.CatalogURI = value
	}
	if spec.TokenSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.TokenSecretRef)
		if err != nil {
			return err
		}
		spec.BearerToken = value
	}
	if spec.OAuth2ServerURISecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.OAuth2ServerURISecretRef)
		if err != nil {
			return err
		}
		spec.OAuth2ServerURI = value
	}
	if spec.OAuth2ClientIDSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.OAuth2ClientIDSecretRef)
		if err != nil {
			return err
		}
		spec.OAuth2ClientID = value
	}
	if spec.OAuth2ClientSecretSecretRef != nil {
		value, err := r.ResolveSecretValue(ctx, namespace, spec.OAuth2ClientSecretSecretRef)
		if err != nil {
			return err
		}
		spec.OAuth2ClientSecret = value
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
	if spec.BasicAuth != nil {
		if spec.BasicAuth.UsernameSecretRef != nil {
			value, err := r.ResolveSecretValue(ctx, namespace, spec.BasicAuth.UsernameSecretRef)
			if err != nil {
				return err
			}
			spec.BasicAuth.Username = value
		}
		if spec.BasicAuth.PasswordSecretRef != nil {
			value, err := r.ResolveSecretValue(ctx, namespace, spec.BasicAuth.PasswordSecretRef)
			if err != nil {
				return err
			}
			spec.BasicAuth.Password = value
		}
	}
	return nil
}
