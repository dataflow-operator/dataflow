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

package connectors

import (
	"encoding/json"
	"fmt"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// sourceConnectorFactory creates a SourceConnector from raw config and options.
type sourceConnectorFactory func(raw *runtime.RawExtension, options *SourceConnectorOptions) (SourceConnector, error)

// sinkConnectorFactory creates a SinkConnector from raw config.
type sinkConnectorFactory func(raw *runtime.RawExtension) (SinkConnector, error)

var sourceConnectorRegistry = map[string]sourceConnectorFactory{
	"kafka": createSourceConnector[v1.KafkaSourceSpec]("kafka source", func(cfg *v1.KafkaSourceSpec) SourceConnector { return NewKafkaSourceConnector(cfg) }),
	"postgresql": createSourceConnectorWithOptions[v1.PostgreSQLSourceSpec]("postgresql source", func(cfg *v1.PostgreSQLSourceSpec, opts *SourceConnectorOptions) SourceConnector {
		return NewPostgreSQLSourceConnectorWithOptions(cfg, opts)
	}),
	"trino": createSourceConnectorWithOptions[v1.TrinoSourceSpec]("trino source", func(cfg *v1.TrinoSourceSpec, opts *SourceConnectorOptions) SourceConnector {
		return NewTrinoSourceConnectorWithOptions(cfg, opts)
	}),
	"clickhouse": createSourceConnectorWithOptions[v1.ClickHouseSourceSpec]("clickhouse source", func(cfg *v1.ClickHouseSourceSpec, opts *SourceConnectorOptions) SourceConnector {
		return NewClickHouseSourceConnectorWithOptions(cfg, opts)
	}),
	"nessie": createSourceConnector[v1.NessieSourceSpec]("nessie source", func(cfg *v1.NessieSourceSpec) SourceConnector { return NewNessieSourceConnector(cfg) }),
}

var sinkConnectorRegistry = map[string]sinkConnectorFactory{
	"kafka":      createSinkConnector[v1.KafkaSinkSpec]("kafka sink", func(cfg *v1.KafkaSinkSpec) SinkConnector { return NewKafkaSinkConnector(cfg) }),
	"postgresql": createSinkConnector[v1.PostgreSQLSinkSpec]("postgresql sink", func(cfg *v1.PostgreSQLSinkSpec) SinkConnector { return NewPostgreSQLSinkConnector(cfg) }),
	"trino":      createSinkConnector[v1.TrinoSinkSpec]("trino sink", func(cfg *v1.TrinoSinkSpec) SinkConnector { return NewTrinoSinkConnector(cfg) }),
	"clickhouse": createSinkConnector[v1.ClickHouseSinkSpec]("clickhouse sink", func(cfg *v1.ClickHouseSinkSpec) SinkConnector { return NewClickHouseSinkConnector(cfg) }),
	"nessie":     createSinkConnector[v1.NessieSinkSpec]("nessie sink", func(cfg *v1.NessieSinkSpec) SinkConnector { return NewNessieSinkConnector(cfg) }),
}

// unmarshalConfig unmarshals raw extension into T. Returns (nil, nil) if raw is nil or empty.
func unmarshalConfig[T any](raw *runtime.RawExtension, errPrefix string) (*T, error) {
	if raw == nil || len(raw.Raw) == 0 {
		return nil, nil
	}
	var cfg T
	if err := json.Unmarshal(raw.Raw, &cfg); err != nil {
		return nil, fmt.Errorf("%s: %w", errPrefix, err)
	}
	return &cfg, nil
}

// createSourceConnector returns a factory for sources that do not need options (Kafka, Nessie).
func createSourceConnector[T any](typeName string, newFn func(*T) SourceConnector) sourceConnectorFactory {
	return func(raw *runtime.RawExtension, _ *SourceConnectorOptions) (SourceConnector, error) {
		cfg, err := unmarshalConfig[T](raw, typeName+" configuration")
		if err != nil {
			return nil, err
		}
		if cfg == nil {
			return nil, fmt.Errorf("%s configuration is required", typeName)
		}
		return newFn(cfg), nil
	}
}

// createSourceConnectorWithOptions returns a factory for sources that need options (PostgreSQL, Trino, ClickHouse).
func createSourceConnectorWithOptions[T any](typeName string, newFn func(*T, *SourceConnectorOptions) SourceConnector) sourceConnectorFactory {
	return func(raw *runtime.RawExtension, opts *SourceConnectorOptions) (SourceConnector, error) {
		cfg, err := unmarshalConfig[T](raw, typeName+" configuration")
		if err != nil {
			return nil, err
		}
		if cfg == nil {
			return nil, fmt.Errorf("%s configuration is required", typeName)
		}
		return newFn(cfg, opts), nil
	}
}

// createSinkConnector returns a factory for sink connectors.
func createSinkConnector[T any](typeName string, newFn func(*T) SinkConnector) sinkConnectorFactory {
	return func(raw *runtime.RawExtension) (SinkConnector, error) {
		cfg, err := unmarshalConfig[T](raw, typeName+" configuration")
		if err != nil {
			return nil, err
		}
		if cfg == nil {
			return nil, fmt.Errorf("%s configuration is required", typeName)
		}
		return newFn(cfg), nil
	}
}

// CreateSourceConnector creates a source connector based on the source spec.
// Options can include WithCheckpointStore for checkpoint persistence.
// Supports both config (type+config) and legacy (type+kafka/postgresql/etc) formats.
func CreateSourceConnector(source *v1.SourceSpec, opts ...SourceConnectorOption) (SourceConnector, error) {
	options := &SourceConnectorOptions{}
	for _, opt := range opts {
		opt(options)
	}

	factory, ok := sourceConnectorRegistry[source.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported source type: %s", source.Type)
	}
	return factory(source.Config, options)
}

// CreateSinkConnector creates a sink connector based on the sink spec.
// Supports both config (type+config) and legacy (type+kafka/postgresql/etc) formats.
func CreateSinkConnector(sink *v1.SinkSpec) (SinkConnector, error) {
	factory, ok := sinkConnectorRegistry[sink.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported sink type: %s", sink.Type)
	}
	return factory(sink.Config)
}
