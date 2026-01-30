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
	"testing"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateSourceConnector_Kafka(t *testing.T) {
	tests := []struct {
		name        string
		source      *v1.SourceSpec
		wantErr     bool
		errContains string
	}{
		{
			name: "valid kafka source",
			source: &v1.SourceSpec{
				Type: "kafka",
				Kafka: &v1.KafkaSourceSpec{
					Brokers:       []string{"localhost:9092"},
					Topic:         "test-topic",
					ConsumerGroup: "test-group",
				},
			},
			wantErr: false,
		},
		{
			name: "kafka source without config",
			source: &v1.SourceSpec{
				Type: "kafka",
			},
			wantErr:     true,
			errContains: "kafka source configuration is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connector, err := CreateSourceConnector(tt.source)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				assert.Nil(t, connector)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, connector)
			}
		})
	}
}

func TestCreateSourceConnector_PostgreSQL(t *testing.T) {
	tests := []struct {
		name        string
		source      *v1.SourceSpec
		wantErr     bool
		errContains string
	}{
		{
			name: "valid postgresql source",
			source: &v1.SourceSpec{
				Type: "postgresql",
				PostgreSQL: &v1.PostgreSQLSourceSpec{
					ConnectionString: "postgres://user:pass@localhost/db",
					Table:            "test_table",
				},
			},
			wantErr: false,
		},
		{
			name: "postgresql source without config",
			source: &v1.SourceSpec{
				Type: "postgresql",
			},
			wantErr:     true,
			errContains: "postgresql source configuration is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connector, err := CreateSourceConnector(tt.source)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				assert.Nil(t, connector)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, connector)
			}
		})
	}
}

func TestCreateSourceConnector_Iceberg(t *testing.T) {
	tests := []struct {
		name        string
		source      *v1.SourceSpec
		wantErr     bool
		errContains string
	}{
		// TODO: Iceberg source is not yet implemented, using Trino instead
		// {
		// 	name: "valid iceberg source",
		// 	source: &v1.SourceSpec{
		// 		Type: "iceberg",
		// 		Iceberg: &v1.IcebergSourceSpec{
		// 			RESTCatalogURL: "http://localhost:8181",
		// 			Namespace:      "test_namespace",
		// 			Table:          "test_table",
		// 		},
		// 	},
		{
			name: "valid trino source",
			source: &v1.SourceSpec{
				Type: "trino",
				Trino: &v1.TrinoSourceSpec{
					ServerURL: "http://localhost:8080",
					Catalog:   "test_catalog",
					Schema:    "test_schema",
					Table:     "test_table",
				},
			},
			wantErr: false,
		},
		// TODO: Iceberg source is not yet implemented
		// {
		// 	name: "iceberg source without config",
		// 	source: &v1.SourceSpec{
		// 		Type: "iceberg",
		// 	},
		// 	wantErr:     true,
		// 	errContains: "iceberg source configuration is required",
		// },
		{
			name: "trino source without config",
			source: &v1.SourceSpec{
				Type: "trino",
			},
			wantErr:     true,
			errContains: "trino source configuration is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connector, err := CreateSourceConnector(tt.source)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				assert.Nil(t, connector)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, connector)
			}
		})
	}
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
	tests := []struct {
		name        string
		sink        *v1.SinkSpec
		wantErr     bool
		errContains string
	}{
		{
			name: "valid kafka sink",
			sink: &v1.SinkSpec{
				Type: "kafka",
				Kafka: &v1.KafkaSinkSpec{
					Brokers: []string{"localhost:9092"},
					Topic:   "test-topic",
				},
			},
			wantErr: false,
		},
		{
			name: "kafka sink without config",
			sink: &v1.SinkSpec{
				Type: "kafka",
			},
			wantErr:     true,
			errContains: "kafka sink configuration is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connector, err := CreateSinkConnector(tt.sink)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				assert.Nil(t, connector)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, connector)
			}
		})
	}
}

func TestCreateSinkConnector_PostgreSQL(t *testing.T) {
	tests := []struct {
		name        string
		sink        *v1.SinkSpec
		wantErr     bool
		errContains string
	}{
		{
			name: "valid postgresql sink",
			sink: &v1.SinkSpec{
				Type: "postgresql",
				PostgreSQL: &v1.PostgreSQLSinkSpec{
					ConnectionString: "postgres://user:pass@localhost/db",
					Table:            "test_table",
				},
			},
			wantErr: false,
		},
		{
			name: "postgresql sink without config",
			sink: &v1.SinkSpec{
				Type: "postgresql",
			},
			wantErr:     true,
			errContains: "postgresql sink configuration is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connector, err := CreateSinkConnector(tt.sink)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				assert.Nil(t, connector)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, connector)
			}
		})
	}
}

// TODO: Iceberg sink is not yet implemented, testing Trino sink instead
func TestCreateSinkConnector_Trino(t *testing.T) {
	tests := []struct {
		name        string
		sink        *v1.SinkSpec
		wantErr     bool
		errContains string
	}{
		{
			name: "valid trino sink",
			sink: &v1.SinkSpec{
				Type: "trino",
				Trino: &v1.TrinoSinkSpec{
					ServerURL: "http://trino:8080",
					Catalog:   "hive",
					Schema:    "default",
					Table:     "test_table",
				},
			},
			wantErr: false,
		},
		{
			name: "trino sink with keycloak",
			sink: &v1.SinkSpec{
				Type: "trino",
				Trino: &v1.TrinoSinkSpec{
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
				},
			},
			wantErr: false,
		},
		{
			name: "trino sink without config",
			sink: &v1.SinkSpec{
				Type: "trino",
			},
			wantErr:     true,
			errContains: "trino sink configuration is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connector, err := CreateSinkConnector(tt.sink)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				assert.Nil(t, connector)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, connector)
			}
		})
	}
}
