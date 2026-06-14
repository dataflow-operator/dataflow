package v1

import (
	"encoding/json"
	"fmt"

	"github.com/dataflow-operator/dataflow/pkg/providers"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func init() {
	providers.RegisterSource(providers.SourceDefinition{
		Type: "kafka",
		ValidateConfig: func(raw []byte, path *field.Path) field.ErrorList {
			return validateSourceConfig(raw, "kafka", path, validateKafkaSource)
		},
	})
	providers.RegisterSource(providers.SourceDefinition{
		Type: "postgresql",
		ValidateConfig: func(raw []byte, path *field.Path) field.ErrorList {
			return validateSourceConfig(raw, "postgresql", path, validatePostgreSQLSource)
		},
	})
	providers.RegisterSource(providers.SourceDefinition{
		Type: "postgresql-cdc",
		ValidateConfig: func(raw []byte, path *field.Path) field.ErrorList {
			return validateSourceConfig(raw, "postgresql-cdc", path, validatePostgreSQLCDCSource)
		},
	})
	providers.RegisterSource(providers.SourceDefinition{
		Type: "trino",
		ValidateConfig: func(raw []byte, path *field.Path) field.ErrorList {
			return validateSourceConfig(raw, "trino", path, validateTrinoSource)
		},
	})
	providers.RegisterSource(providers.SourceDefinition{
		Type: "clickhouse",
		ValidateConfig: func(raw []byte, path *field.Path) field.ErrorList {
			return validateSourceConfig(raw, "clickhouse", path, validateClickHouseSource)
		},
	})
	providers.RegisterSource(providers.SourceDefinition{
		Type: "nessie",
		ValidateConfig: func(raw []byte, path *field.Path) field.ErrorList {
			return validateSourceConfig(raw, "nessie", path, validateNessieSource)
		},
	})
	providers.RegisterSource(providers.SourceDefinition{
		Type: "iceberg",
		ValidateConfig: func(raw []byte, path *field.Path) field.ErrorList {
			return validateSourceConfig(raw, "iceberg", path, validateIcebergSource)
		},
	})

	providers.RegisterSink(providers.SinkDefinition{
		Type: "kafka",
		ValidateConfig: func(raw []byte, path *field.Path) field.ErrorList {
			return validateSinkConfig(raw, "kafka", path, validateKafkaSink)
		},
	})
	providers.RegisterSink(providers.SinkDefinition{
		Type: "postgresql",
		ValidateConfig: func(raw []byte, path *field.Path) field.ErrorList {
			return validateSinkConfig(raw, "postgresql", path, validatePostgreSQLSink)
		},
	})
	providers.RegisterSink(providers.SinkDefinition{
		Type: "trino",
		ValidateConfig: func(raw []byte, path *field.Path) field.ErrorList {
			return validateSinkConfig(raw, "trino", path, validateTrinoSink)
		},
	})
	providers.RegisterSink(providers.SinkDefinition{
		Type: "clickhouse",
		ValidateConfig: func(raw []byte, path *field.Path) field.ErrorList {
			return validateSinkConfig(raw, "clickhouse", path, validateClickHouseSink)
		},
	})
	providers.RegisterSink(providers.SinkDefinition{
		Type: "nessie",
		ValidateConfig: func(raw []byte, path *field.Path) field.ErrorList {
			return validateSinkConfig(raw, "nessie", path, validateNessieSink)
		},
	})
	providers.RegisterSink(providers.SinkDefinition{
		Type: "iceberg",
		ValidateConfig: func(raw []byte, path *field.Path) field.ErrorList {
			return validateSinkConfig(raw, "iceberg", path, validateIcebergSink)
		},
	})
}

func validateSourceConfig[T any](raw []byte, typeName string, path *field.Path, validator func(*T, *field.Path) field.ErrorList) field.ErrorList {
	if len(raw) == 0 {
		return field.ErrorList{field.Required(path, fmt.Sprintf("%s source configuration is required", typeName))}
	}
	var cfg T
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return field.ErrorList{field.Invalid(path, string(raw), fmt.Sprintf("invalid %s config: %s", typeName, err.Error()))}
	}
	return validator(&cfg, path)
}

func validateSinkConfig[T any](raw []byte, typeName string, path *field.Path, validator func(*T, *field.Path) field.ErrorList) field.ErrorList {
	if len(raw) == 0 {
		return field.ErrorList{field.Required(path, fmt.Sprintf("%s sink configuration is required", typeName))}
	}
	var cfg T
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return field.ErrorList{field.Invalid(path, string(raw), fmt.Sprintf("invalid %s config: %s", typeName, err.Error()))}
	}
	return validator(&cfg, path)
}
