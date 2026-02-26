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

// CreateSourceConnector creates a source connector based on the source spec
func CreateSourceConnector(source *v1.SourceSpec) (SourceConnector, error) {
	switch source.Type {
	case "kafka":
		if source.Kafka == nil {
			return nil, fmt.Errorf("kafka source configuration is required")
		}
		return NewKafkaSourceConnector(source.Kafka), nil
	case "postgresql":
		if source.PostgreSQL == nil {
			return nil, fmt.Errorf("postgresql source configuration is required")
		}
		return NewPostgreSQLSourceConnector(source.PostgreSQL), nil
	case "trino":
		if source.Trino == nil {
			return nil, fmt.Errorf("trino source configuration is required")
		}
		return NewTrinoSourceConnector(source.Trino), nil
	case "clickhouse":
		if source.ClickHouse == nil {
			return nil, fmt.Errorf("clickhouse source configuration is required")
		}
		return NewClickHouseSourceConnector(source.ClickHouse), nil
	case "nessie":
		if source.Nessie == nil {
			return nil, fmt.Errorf("nessie source configuration is required")
		}
		return NewNessieSourceConnector(source.Nessie), nil
	default:
		return nil, fmt.Errorf("unsupported source type: %s", source.Type)
	}
}

// CreateSinkConnector creates a sink connector based on the sink spec
func CreateSinkConnector(sink *v1.SinkSpec) (SinkConnector, error) {
	switch sink.Type {
	case "kafka":
		if sink.Kafka == nil {
			return nil, fmt.Errorf("kafka sink configuration is required")
		}
		return NewKafkaSinkConnector(sink.Kafka), nil
	case "postgresql":
		if sink.PostgreSQL == nil {
			return nil, fmt.Errorf("postgresql sink configuration is required")
		}
		return NewPostgreSQLSinkConnector(sink.PostgreSQL), nil
	case "trino":
		if sink.Trino == nil {
			return nil, fmt.Errorf("trino sink configuration is required")
		}
		return NewTrinoSinkConnector(sink.Trino), nil
	case "clickhouse":
		if sink.ClickHouse == nil {
			return nil, fmt.Errorf("clickhouse sink configuration is required")
		}
		return NewClickHouseSinkConnector(sink.ClickHouse), nil
	case "nessie":
		if sink.Nessie == nil {
			return nil, fmt.Errorf("nessie sink configuration is required")
		}
		return NewNessieSinkConnector(sink.Nessie), nil
	default:
		return nil, fmt.Errorf("unsupported sink type: %s", sink.Type)
	}
}
