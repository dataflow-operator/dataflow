package connectors

import v1 "github.com/dataflow-operator/dataflow/api/v1"

func init() {
	registerSourceConnector("kafka", createSourceConnectorWithOptions[v1.KafkaSourceSpec]("kafka source", func(cfg *v1.KafkaSourceSpec, opts *SourceConnectorOptions) SourceConnector {
		return NewKafkaSourceConnectorWithOptions(cfg, opts)
	}), false)
	registerSourceConnector("postgresql", createSourceConnectorWithOptions[v1.PostgreSQLSourceSpec]("postgresql source", func(cfg *v1.PostgreSQLSourceSpec, opts *SourceConnectorOptions) SourceConnector {
		return NewPostgreSQLSourceConnectorWithOptions(cfg, opts)
	}), true)
	registerSourceConnector("trino", createSourceConnectorWithOptions[v1.TrinoSourceSpec]("trino source", func(cfg *v1.TrinoSourceSpec, opts *SourceConnectorOptions) SourceConnector {
		return NewTrinoSourceConnectorWithOptions(cfg, opts)
	}), true)
	registerSourceConnector("clickhouse", createSourceConnectorWithOptions[v1.ClickHouseSourceSpec]("clickhouse source", func(cfg *v1.ClickHouseSourceSpec, opts *SourceConnectorOptions) SourceConnector {
		return NewClickHouseSourceConnectorWithOptions(cfg, opts)
	}), true)
	registerSourceConnector("nessie", createSourceConnectorWithOptions[v1.NessieSourceSpec]("nessie source", func(cfg *v1.NessieSourceSpec, opts *SourceConnectorOptions) SourceConnector {
		return NewNessieSourceConnectorWithOptions(cfg, opts)
	}), true)
	registerSourceConnector("iceberg", createSourceConnectorWithOptions[v1.IcebergSourceSpec]("iceberg source", func(cfg *v1.IcebergSourceSpec, opts *SourceConnectorOptions) SourceConnector {
		return NewIcebergSourceConnectorWithOptions(cfg, opts)
	}), true)

	registerSinkConnector("kafka", createSinkConnector[v1.KafkaSinkSpec]("kafka sink", func(cfg *v1.KafkaSinkSpec) SinkConnector { return NewKafkaSinkConnector(cfg) }))
	registerSinkConnector("postgresql", createSinkConnector[v1.PostgreSQLSinkSpec]("postgresql sink", func(cfg *v1.PostgreSQLSinkSpec) SinkConnector { return NewPostgreSQLSinkConnector(cfg) }))
	registerSinkConnector("trino", createSinkConnector[v1.TrinoSinkSpec]("trino sink", func(cfg *v1.TrinoSinkSpec) SinkConnector { return NewTrinoSinkConnector(cfg) }))
	registerSinkConnector("clickhouse", createSinkConnector[v1.ClickHouseSinkSpec]("clickhouse sink", func(cfg *v1.ClickHouseSinkSpec) SinkConnector { return NewClickHouseSinkConnector(cfg) }))
	registerSinkConnector("nessie", createSinkConnector[v1.NessieSinkSpec]("nessie sink", func(cfg *v1.NessieSinkSpec) SinkConnector { return NewNessieSinkConnector(cfg) }))
	registerSinkConnector("iceberg", createSinkConnector[v1.IcebergSinkSpec]("iceberg sink", func(cfg *v1.IcebergSinkSpec) SinkConnector { return NewIcebergSinkConnector(cfg) }))
}
