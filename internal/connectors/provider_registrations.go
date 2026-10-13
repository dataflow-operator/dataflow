package connectors

import (
	"fmt"
	"slices"
	"sort"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/providers"
)

func init() {
	registerSourceConnector("kafka", createSourceConnectorWithOptions[v1.KafkaSourceSpec]("kafka source", func(cfg *v1.KafkaSourceSpec, opts *SourceConnectorOptions) SourceConnector {
		return NewKafkaSourceConnectorWithOptions(cfg, opts)
	}))
	registerSourceConnector("postgresql", createSourceConnectorWithOptions[v1.PostgreSQLSourceSpec]("postgresql source", func(cfg *v1.PostgreSQLSourceSpec, opts *SourceConnectorOptions) SourceConnector {
		return NewPostgreSQLSourceConnectorWithOptions(cfg, opts)
	}))
	registerSourceConnector("postgresql-cdc", createSourceConnectorWithOptions[v1.PostgreSQLCDCSourceSpec]("postgresql-cdc source", func(cfg *v1.PostgreSQLCDCSourceSpec, opts *SourceConnectorOptions) SourceConnector {
		return NewPostgreSQLCDCSourceConnectorWithOptions(cfg, opts)
	}))
	registerSourceConnector("trino", createSourceConnectorWithOptions[v1.TrinoSourceSpec]("trino source", func(cfg *v1.TrinoSourceSpec, opts *SourceConnectorOptions) SourceConnector {
		return NewTrinoSourceConnectorWithOptions(cfg, opts)
	}))
	registerSourceConnector("clickhouse", createSourceConnectorWithOptions[v1.ClickHouseSourceSpec]("clickhouse source", func(cfg *v1.ClickHouseSourceSpec, opts *SourceConnectorOptions) SourceConnector {
		return NewClickHouseSourceConnectorWithOptions(cfg, opts)
	}))
	registerSourceConnector("nessie", createSourceConnectorWithOptions[v1.NessieSourceSpec]("nessie source", func(cfg *v1.NessieSourceSpec, opts *SourceConnectorOptions) SourceConnector {
		return NewNessieSourceConnectorWithOptions(cfg, opts)
	}))
	registerSourceConnector("iceberg", createSourceConnectorWithOptions[v1.IcebergSourceSpec]("iceberg source", func(cfg *v1.IcebergSourceSpec, opts *SourceConnectorOptions) SourceConnector {
		return NewIcebergSourceConnectorWithOptions(cfg, opts)
	}))
	registerSourceConnector("hive", createSourceConnectorWithOptions[v1.HiveSourceSpec]("hive source", func(cfg *v1.HiveSourceSpec, opts *SourceConnectorOptions) SourceConnector {
		return NewHiveSourceConnectorWithOptions(cfg, opts)
	}))

	registerSinkConnector("kafka", createSinkConnector[v1.KafkaSinkSpec]("kafka sink", func(cfg *v1.KafkaSinkSpec) SinkConnector { return NewKafkaSinkConnector(cfg) }))
	registerSinkConnector("postgresql", createSinkConnector[v1.PostgreSQLSinkSpec]("postgresql sink", func(cfg *v1.PostgreSQLSinkSpec) SinkConnector { return NewPostgreSQLSinkConnector(cfg) }))
	registerSinkConnector("trino", createSinkConnector[v1.TrinoSinkSpec]("trino sink", func(cfg *v1.TrinoSinkSpec) SinkConnector { return NewTrinoSinkConnector(cfg) }))
	registerSinkConnector("clickhouse", createSinkConnector[v1.ClickHouseSinkSpec]("clickhouse sink", func(cfg *v1.ClickHouseSinkSpec) SinkConnector { return NewClickHouseSinkConnector(cfg) }))
	registerSinkConnector("nessie", createSinkConnector[v1.NessieSinkSpec]("nessie sink", func(cfg *v1.NessieSinkSpec) SinkConnector { return NewNessieSinkConnector(cfg) }))
	registerSinkConnector("iceberg", createSinkConnector[v1.IcebergSinkSpec]("iceberg sink", func(cfg *v1.IcebergSinkSpec) SinkConnector { return NewIcebergSinkConnector(cfg) }))
	registerSinkConnector("hive", createSinkConnector[v1.HiveSinkSpec]("hive sink", func(cfg *v1.HiveSinkSpec) SinkConnector { return NewHiveSinkConnector(cfg) }))

	if err := providerFactoryCoverage(); err != nil {
		panic(err)
	}
}

// providerFactoryCoverage checks that every admission catalog entry has a factory and a validator.
// Metadata (validator, checkpoint, exhausts) is registered once in api/v1.
func providerFactoryCoverage() error {
	registryMu.RLock()
	sourceFactories := registryKeys(sourceConnectorRegistry)
	sinkFactories := registryKeys(sinkConnectorRegistry)
	registryMu.RUnlock()

	if err := matchCatalog("source", sourceFactories, providers.ListSourceTypes()); err != nil {
		return err
	}
	if err := matchCatalog("sink", sinkFactories, providers.ListSinkTypes()); err != nil {
		return err
	}
	for _, sourceType := range providers.ListSourceTypes() {
		if providers.SourceValidator(sourceType) == nil {
			return fmt.Errorf("connectors: source %q is missing a validator", sourceType)
		}
	}
	for _, sinkType := range providers.ListSinkTypes() {
		if providers.SinkValidator(sinkType) == nil {
			return fmt.Errorf("connectors: sink %q is missing a validator", sinkType)
		}
	}
	return nil
}

func registryKeys[T any](registry map[string]T) []string {
	keys := make([]string, 0, len(registry))
	for key := range registry {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func matchCatalog(kind string, factories, catalog []string) error {
	if slices.Equal(factories, catalog) {
		return nil
	}
	return fmt.Errorf("connectors: %s factories %v do not match provider catalog %v", kind, factories, catalog)
}
