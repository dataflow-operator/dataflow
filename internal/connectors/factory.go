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
	"fmt"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
)

// CreateSourceConnector creates a source connector based on the source spec.
// Options can include WithCheckpointStore for checkpoint persistence.
// Supports both config (type+config) and legacy (type+kafka/postgresql/etc) formats.
func CreateSourceConnector(source *v1.SourceSpec, opts ...SourceConnectorOption) (SourceConnector, error) {
	options := &SourceConnectorOptions{}
	for _, opt := range opts {
		opt(options)
	}

	switch source.Type {
	case "kafka":
		cfg, err := source.GetKafkaConfig()
		if err != nil {
			return nil, fmt.Errorf("kafka source configuration: %w", err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("kafka source configuration is required")
		}
		return NewKafkaSourceConnector(cfg), nil
	case "postgresql":
		cfg, err := source.GetPostgreSQLConfig()
		if err != nil {
			return nil, fmt.Errorf("postgresql source configuration: %w", err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("postgresql source configuration is required")
		}
		return NewPostgreSQLSourceConnectorWithOptions(cfg, options), nil
	case "trino":
		cfg, err := source.GetTrinoConfig()
		if err != nil {
			return nil, fmt.Errorf("trino source configuration: %w", err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("trino source configuration is required")
		}
		return NewTrinoSourceConnectorWithOptions(cfg, options), nil
	case "clickhouse":
		cfg, err := source.GetClickHouseConfig()
		if err != nil {
			return nil, fmt.Errorf("clickhouse source configuration: %w", err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("clickhouse source configuration is required")
		}
		return NewClickHouseSourceConnectorWithOptions(cfg, options), nil
	case "nessie":
		cfg, err := source.GetNessieConfig()
		if err != nil {
			return nil, fmt.Errorf("nessie source configuration: %w", err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("nessie source configuration is required")
		}
		return NewNessieSourceConnector(cfg), nil
	default:
		return nil, fmt.Errorf("unsupported source type: %s", source.Type)
	}
}

// CreateSinkConnector creates a sink connector based on the sink spec.
// Supports both config (type+config) and legacy (type+kafka/postgresql/etc) formats.
func CreateSinkConnector(sink *v1.SinkSpec) (SinkConnector, error) {
	switch sink.Type {
	case "kafka":
		cfg, err := sink.GetKafkaConfig()
		if err != nil {
			return nil, fmt.Errorf("kafka sink configuration: %w", err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("kafka sink configuration is required")
		}
		return NewKafkaSinkConnector(cfg), nil
	case "postgresql":
		cfg, err := sink.GetPostgreSQLConfig()
		if err != nil {
			return nil, fmt.Errorf("postgresql sink configuration: %w", err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("postgresql sink configuration is required")
		}
		return NewPostgreSQLSinkConnector(cfg), nil
	case "trino":
		cfg, err := sink.GetTrinoConfig()
		if err != nil {
			return nil, fmt.Errorf("trino sink configuration: %w", err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("trino sink configuration is required")
		}
		return NewTrinoSinkConnector(cfg), nil
	case "clickhouse":
		cfg, err := sink.GetClickHouseConfig()
		if err != nil {
			return nil, fmt.Errorf("clickhouse sink configuration: %w", err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("clickhouse sink configuration is required")
		}
		return NewClickHouseSinkConnector(cfg), nil
	case "nessie":
		cfg, err := sink.GetNessieConfig()
		if err != nil {
			return nil, fmt.Errorf("nessie sink configuration: %w", err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("nessie sink configuration is required")
		}
		return NewNessieSinkConnector(cfg), nil
	default:
		return nil, fmt.Errorf("unsupported sink type: %s", sink.Type)
	}
}
