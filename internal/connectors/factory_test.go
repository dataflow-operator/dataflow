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
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateSourceConnector_Kafka(t *testing.T) {
	runCreateSourceConnectorTests(t, []sourceConnectorTestCase{
		{
			name: "valid kafka source",
			source: func() *v1.SourceSpec {
				cfg := v1.KafkaSourceSpec{
					Brokers:       []string{"localhost:9092"},
					Topic:         "test-topic",
					ConsumerGroup: "test-group",
				}
				raw, _ := json.Marshal(cfg)
				return &v1.SourceSpec{Type: "kafka", Config: &runtime.RawExtension{Raw: raw}}
			}(),
		},
		{
			name: "valid kafka source with raw config",
			source: func() *v1.SourceSpec {
				cfg := v1.KafkaSourceSpec{
					Brokers:       []string{"localhost:9092"},
					Topic:         "test-topic",
					ConsumerGroup: "test-group",
				}
				raw, _ := json.Marshal(cfg)
				return &v1.SourceSpec{Type: "kafka", Config: &runtime.RawExtension{Raw: raw}}
			}(),
		},
		{
			name: "kafka source without config",
			source: &v1.SourceSpec{
				Type: "kafka",
			},
			wantErr:     true,
			errContains: "kafka source configuration is required",
		},
	})
}

func TestCreateSourceConnector_PostgreSQL(t *testing.T) {
	runCreateSourceConnectorTests(t, []sourceConnectorTestCase{
		{
			name: "valid postgresql source",
			source: func() *v1.SourceSpec {
				cfg := v1.PostgreSQLSourceSpec{
					ConnectionString: "postgres://user:pass@localhost/db",
					Table:            "test_table",
				}
				raw, _ := json.Marshal(cfg)
				return &v1.SourceSpec{Type: "postgresql", Config: &runtime.RawExtension{Raw: raw}}
			}(),
		},
		{
			name: "postgresql source without config",
			source: &v1.SourceSpec{
				Type: "postgresql",
			},
			wantErr:     true,
			errContains: "postgresql source configuration is required",
		},
	})
}

func TestCreateSourceConnector_Iceberg(t *testing.T) {
	runCreateSourceConnectorTests(t, []sourceConnectorTestCase{
		// TODO: Iceberg source is not yet implemented, using Trino instead
		{
			name: "valid trino source",
			source: func() *v1.SourceSpec {
				cfg := v1.TrinoSourceSpec{
					ServerURL: "http://localhost:8080",
					Catalog:   "test_catalog",
					Schema:    "test_schema",
					Table:     "test_table",
				}
				raw, _ := json.Marshal(cfg)
				return &v1.SourceSpec{Type: "trino", Config: &runtime.RawExtension{Raw: raw}}
			}(),
		},
		{
			name: "trino source without config",
			source: &v1.SourceSpec{
				Type: "trino",
			},
			wantErr:     true,
			errContains: "trino source configuration is required",
		},
	})
}

func TestCreateSourceConnector_Nessie(t *testing.T) {
	runCreateSourceConnectorTests(t, []sourceConnectorTestCase{
		{
			name: "valid nessie source",
			source: func() *v1.SourceSpec {
				cfg := v1.NessieSourceSpec{
					BaseURL:   "http://nessie:19120",
					Branch:    "main",
					Namespace: "ns",
					Table:     "t1",
				}
				raw, _ := json.Marshal(cfg)
				return &v1.SourceSpec{Type: "nessie", Config: &runtime.RawExtension{Raw: raw}}
			}(),
		},
		{
			name: "nessie source without config",
			source: &v1.SourceSpec{
				Type: "nessie",
			},
			wantErr:     true,
			errContains: "nessie source configuration is required",
		},
	})
}

func TestCreateSourceConnector_UnsupportedType(t *testing.T) {
	source := &v1.SourceSpec{
		Type: "unsupported",
	}

	connector, err := CreateSourceConnector(source)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported source type")
	assert.Nil(t, connector)
}

func TestCreateSinkConnector_Kafka(t *testing.T) {
	runCreateSinkConnectorTests(t, []sinkConnectorTestCase{
		{
			name: "valid kafka sink",
			sink: func() *v1.SinkSpec {
				cfg := v1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic"}
				raw, _ := json.Marshal(cfg)
				return &v1.SinkSpec{Type: "kafka", Config: &runtime.RawExtension{Raw: raw}}
			}(),
		},
		{
			name: "kafka sink without config",
			sink: &v1.SinkSpec{
				Type: "kafka",
			},
			wantErr:     true,
			errContains: "kafka sink configuration is required",
		},
	})
}

func TestCreateSinkConnector_PostgreSQL(t *testing.T) {
	runCreateSinkConnectorTests(t, []sinkConnectorTestCase{
		{
			name: "valid postgresql sink",
			sink: func() *v1.SinkSpec {
				cfg := v1.PostgreSQLSinkSpec{
					ConnectionString: "postgres://user:pass@localhost/db",
					Table:            "test_table",
				}
				raw, _ := json.Marshal(cfg)
				return &v1.SinkSpec{Type: "postgresql", Config: &runtime.RawExtension{Raw: raw}}
			}(),
		},
		{
			name: "postgresql sink without config",
			sink: &v1.SinkSpec{
				Type: "postgresql",
			},
			wantErr:     true,
			errContains: "postgresql sink configuration is required",
		},
	})
}

func TestCreateSinkConnector_Nessie(t *testing.T) {
	runCreateSinkConnectorTests(t, []sinkConnectorTestCase{
		{
			name: "valid nessie sink",
			sink: func() *v1.SinkSpec {
				cfg := v1.NessieSinkSpec{
					BaseURL:   "http://nessie:19120",
					Branch:    "main",
					Namespace: "ns",
					Table:     "t1",
				}
				raw, _ := json.Marshal(cfg)
				return &v1.SinkSpec{Type: "nessie", Config: &runtime.RawExtension{Raw: raw}}
			}(),
		},
		{
			name: "nessie sink without config",
			sink: &v1.SinkSpec{
				Type: "nessie",
			},
			wantErr:     true,
			errContains: "nessie sink configuration is required",
		},
	})
}

func TestCreateSinkConnector_Trino(t *testing.T) {
	runCreateSinkConnectorTests(t, []sinkConnectorTestCase{
		{
			name: "valid trino sink",
			sink: func() *v1.SinkSpec {
				cfg := v1.TrinoSinkSpec{
					ServerURL: "http://trino:8080",
					Catalog:   "hive",
					Schema:    "default",
					Table:     "test_table",
				}
				raw, _ := json.Marshal(cfg)
				return &v1.SinkSpec{Type: "trino", Config: &runtime.RawExtension{Raw: raw}}
			}(),
		},
		{
			name: "trino sink with keycloak",
			sink: func() *v1.SinkSpec {
				cfg := v1.TrinoSinkSpec{
					ServerURL: "http://trino:8080",
					Catalog:   "hive",
					Schema:    "default",
					Table:     "test_table",
					Keycloak: &v1.KeycloakConfig{
						ServerURL:    "https://keycloak.example.com",
						Realm:        "myrealm",
						ClientID:     "trino-client",
						ClientSecret: "secret",
						Username:     "user",
						Password:     "pass",
					},
				}
				raw, _ := json.Marshal(cfg)
				return &v1.SinkSpec{Type: "trino", Config: &runtime.RawExtension{Raw: raw}}
			}(),
		},
		{
			name: "trino sink without config",
			sink: &v1.SinkSpec{
				Type: "trino",
			},
			wantErr:     true,
			errContains: "trino sink configuration is required",
		},
	})
}

func TestCreateSourceConnector_ClickHouse(t *testing.T) {
	runCreateSourceConnectorTests(t, []sourceConnectorTestCase{
		{
			name: "valid clickhouse source",
			source: func() *v1.SourceSpec {
				cfg := v1.ClickHouseSourceSpec{
					ConnectionString: "clickhouse://localhost:9000?username=default&password=&database=default",
					Table:            "test_table",
				}
				raw, _ := json.Marshal(cfg)
				return &v1.SourceSpec{Type: "clickhouse", Config: &runtime.RawExtension{Raw: raw}}
			}(),
		},
		{
			name: "clickhouse source without config",
			source: &v1.SourceSpec{
				Type: "clickhouse",
			},
			wantErr:     true,
			errContains: "clickhouse source configuration is required",
		},
	})
}

func TestCreateSinkConnector_ClickHouse(t *testing.T) {
	runCreateSinkConnectorTests(t, []sinkConnectorTestCase{
		{
			name: "valid clickhouse sink",
			sink: func() *v1.SinkSpec {
				cfg := v1.ClickHouseSinkSpec{
					ConnectionString: "clickhouse://localhost:9000?username=default&password=&database=default",
					Table:            "test_table",
				}
				raw, _ := json.Marshal(cfg)
				return &v1.SinkSpec{Type: "clickhouse", Config: &runtime.RawExtension{Raw: raw}}
			}(),
		},
		{
			name: "clickhouse sink without config",
			sink: &v1.SinkSpec{
				Type: "clickhouse",
			},
			wantErr:     true,
			errContains: "clickhouse sink configuration is required",
		},
	})
}
