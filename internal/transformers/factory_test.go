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

package transformers

import (
	"encoding/json"
	"slices"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/pkg/transformtypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransformerRegistryMatchesTransformTypes(t *testing.T) {
	t.Parallel()
	want := transformtypes.All()
	got := make([]string, 0, len(transformerRegistry))
	for k := range transformerRegistry {
		got = append(got, k)
	}
	slices.Sort(want)
	slices.Sort(got)
	require.Equal(t, want, got, "transformer factory keys must match pkg/transformtypes")
}

func mustConfig(v interface{}) *runtime.RawExtension {
	b, _ := json.Marshal(v)
	return &runtime.RawExtension{Raw: b}
}

func TestCreateTransformer_Timestamp(t *testing.T) {
	runCreateTransformerTests(t, []transformerTestCase{
		{
			name: "valid timestamp transformation",
			transformation: &v1.TransformationSpec{
				Type:   "timestamp",
				Config: mustConfig(v1.TimestampTransformation{FieldName: "created_at", Format: "RFC3339"}),
			},
		},
		{
			name: "timestamp without config",
			transformation: &v1.TransformationSpec{
				Type: "timestamp",
			},
			wantErr:     true,
			errContains: "timestamp transformation configuration is required",
		},
	})
}

func TestCreateTransformer_Flatten(t *testing.T) {
	runCreateTransformerTests(t, []transformerTestCase{
		{
			name: "valid flatten transformation",
			transformation: &v1.TransformationSpec{
				Type:   "flatten",
				Config: mustConfig(v1.FlattenTransformation{Field: "$.items"}),
			},
		},
		{
			name: "flatten without config",
			transformation: &v1.TransformationSpec{
				Type: "flatten",
			},
			wantErr:     true,
			errContains: "flatten transformation configuration is required",
		},
	})
}

func TestCreateTransformer_Filter(t *testing.T) {
	runCreateTransformerTests(t, []transformerTestCase{
		{
			name: "valid filter transformation",
			transformation: &v1.TransformationSpec{
				Type:   "filter",
				Config: mustConfig(v1.FilterTransformation{Condition: "$.status == 'active'"}),
			},
		},
		{
			name: "filter without config",
			transformation: &v1.TransformationSpec{
				Type: "filter",
			},
			wantErr:     true,
			errContains: "filter transformation configuration is required",
		},
	})
}

func TestCreateTransformer_Mask(t *testing.T) {
	runCreateTransformerTests(t, []transformerTestCase{
		{
			name: "valid mask transformation",
			transformation: &v1.TransformationSpec{
				Type:   "mask",
				Config: mustConfig(v1.MaskTransformation{Fields: []string{"$.password", "$.email"}, MaskChar: "*", KeepLength: true}),
			},
		},
		{
			name: "mask without config",
			transformation: &v1.TransformationSpec{
				Type: "mask",
			},
			wantErr:     true,
			errContains: "mask transformation configuration is required",
		},
	})
}

func TestCreateTransformer_Router(t *testing.T) {
	runCreateTransformerTests(t, []transformerTestCase{
		{
			name: "valid router transformation",
			transformation: &v1.TransformationSpec{
				Type: "router",
				Config: mustConfig(v1.RouterTransformation{
					Routes: []v1.RouteRule{
						{
							Condition: "$.type == 'error'",
							Sink: v1.SinkSpec{
								Type:   "kafka",
								Config: mustConfig(v1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "errors"}),
							},
						},
					},
				}),
			},
		},
		{
			name: "router without config",
			transformation: &v1.TransformationSpec{
				Type: "router",
			},
			wantErr:     true,
			errContains: "router transformation configuration is required",
		},
	})
}

func TestCreateTransformer_Select(t *testing.T) {
	runCreateTransformerTests(t, []transformerTestCase{
		{
			name: "valid select transformation",
			transformation: &v1.TransformationSpec{
				Type:   "select",
				Config: mustConfig(v1.SelectTransformation{Fields: []string{"$.id", "$.name", "$.status"}}),
			},
		},
		{
			name: "select without config",
			transformation: &v1.TransformationSpec{
				Type: "select",
			},
			wantErr:     true,
			errContains: "select transformation configuration is required",
		},
	})
}

func TestCreateTransformer_Remove(t *testing.T) {
	runCreateTransformerTests(t, []transformerTestCase{
		{
			name: "valid remove transformation",
			transformation: &v1.TransformationSpec{
				Type:   "remove",
				Config: mustConfig(v1.RemoveTransformation{Fields: []string{"$.password", "$.secret"}}),
			},
		},
		{
			name: "remove without config",
			transformation: &v1.TransformationSpec{
				Type: "remove",
			},
			wantErr:     true,
			errContains: "remove transformation configuration is required",
		},
	})
}

func TestCreateTransformer_SnakeCase(t *testing.T) {
	runCreateTransformerTests(t, []transformerTestCase{
		{
			name: "valid snakeCase transformation",
			transformation: &v1.TransformationSpec{
				Type:   "snakeCase",
				Config: mustConfig(v1.SnakeCaseTransformation{Deep: true}),
			},
		},
		{
			name: "snakeCase without config",
			transformation: &v1.TransformationSpec{
				Type: "snakeCase",
			},
			wantErr:     true,
			errContains: "snakeCase transformation configuration is required",
		},
	})
}

func TestCreateTransformer_CamelCase(t *testing.T) {
	runCreateTransformerTests(t, []transformerTestCase{
		{
			name: "valid camelCase transformation",
			transformation: &v1.TransformationSpec{
				Type:   "camelCase",
				Config: mustConfig(v1.CamelCaseTransformation{Deep: true}),
			},
		},
		{
			name: "camelCase without config",
			transformation: &v1.TransformationSpec{
				Type: "camelCase",
			},
			wantErr:     true,
			errContains: "camelCase transformation configuration is required",
		},
	})
}

func TestCreateTransformer_DebeziumUnwrap(t *testing.T) {
	runCreateTransformerTests(t, []transformerTestCase{
		{
			name: "valid debeziumUnwrap transformation",
			transformation: &v1.TransformationSpec{
				Type: transformtypes.DebeziumUnwrap,
				Config: mustConfig(v1.DebeziumUnwrapTransformation{
					InferDeleteFromTombstone: true,
					IncludeSourceInMetadata:  true,
					AddOperationFields:       true,
					AddSourceFields:          []string{"table", "lsn"},
				}),
			},
		},
		{
			name: "debeziumUnwrap without config",
			transformation: &v1.TransformationSpec{
				Type: transformtypes.DebeziumUnwrap,
			},
			wantErr:     true,
			errContains: "debeziumUnwrap transformation configuration is required",
		},
	})
}

func TestCreateTransformer_ReplaceField(t *testing.T) {
	runCreateTransformerTests(t, []transformerTestCase{
		{
			name: "valid replaceField transformation",
			transformation: &v1.TransformationSpec{
				Type: transformtypes.ReplaceField,
				Config: mustConfig(v1.ReplaceFieldTransformation{
					Renames: []string{"oldName:newName"},
					Include: []string{"id", "name"},
				}),
			},
		},
		{
			name: "replaceField without config",
			transformation: &v1.TransformationSpec{
				Type: transformtypes.ReplaceField,
			},
			wantErr:     true,
			errContains: "replaceField transformation configuration is required",
		},
	})
}

func TestCreateTransformer_HeadersToPayload(t *testing.T) {
	runCreateTransformerTests(t, []transformerTestCase{
		{
			name: "valid headersToPayload transformation",
			transformation: &v1.TransformationSpec{
				Type: transformtypes.HeadersToPayload,
				Config: mustConfig(v1.HeadersToPayloadTransformation{
					Mappings: []string{"X-Request-Id:requestId"},
				}),
			},
		},
		{
			name: "headersToPayload without config",
			transformation: &v1.TransformationSpec{
				Type: transformtypes.HeadersToPayload,
			},
			wantErr:     true,
			errContains: "headersToPayload transformation configuration is required",
		},
	})
}

func TestCreateTransformer_StructFlatten(t *testing.T) {
	runCreateTransformerTests(t, []transformerTestCase{
		{
			name: "valid structFlatten transformation",
			transformation: &v1.TransformationSpec{
				Type:   transformtypes.StructFlatten,
				Config: mustConfig(v1.StructFlattenTransformation{Delimiter: "_"}),
			},
		},
		{
			name: "structFlatten with empty config",
			transformation: &v1.TransformationSpec{
				Type:   transformtypes.StructFlatten,
				Config: mustConfig(v1.StructFlattenTransformation{}),
			},
		},
		{
			name: "structFlatten without config",
			transformation: &v1.TransformationSpec{
				Type: transformtypes.StructFlatten,
			},
			wantErr:     true,
			errContains: "structFlatten transformation configuration is required",
		},
	})
}

func TestCreateTransformer_ExtractField(t *testing.T) {
	runCreateTransformerTests(t, []transformerTestCase{
		{
			name: "valid extractField transformation",
			transformation: &v1.TransformationSpec{
				Type:   transformtypes.ExtractField,
				Config: mustConfig(v1.ExtractFieldTransformation{Field: "payload.after"}),
			},
		},
		{
			name: "extractField without config",
			transformation: &v1.TransformationSpec{
				Type: transformtypes.ExtractField,
			},
			wantErr:     true,
			errContains: "extractField transformation configuration is required",
		},
	})
}

func TestCreateTransformer_HoistField(t *testing.T) {
	runCreateTransformerTests(t, []transformerTestCase{
		{
			name: "valid hoistField transformation",
			transformation: &v1.TransformationSpec{
				Type:   transformtypes.HoistField,
				Config: mustConfig(v1.HoistFieldTransformation{Field: "record"}),
			},
		},
		{
			name: "hoistField without config",
			transformation: &v1.TransformationSpec{
				Type: transformtypes.HoistField,
			},
			wantErr:     true,
			errContains: "hoistField transformation configuration is required",
		},
	})
}

func TestCreateTransformer_Cast(t *testing.T) {
	runCreateTransformerTests(t, []transformerTestCase{
		{
			name: "valid cast transformation",
			transformation: &v1.TransformationSpec{
				Type: transformtypes.Cast,
				Config: mustConfig(v1.CastTransformation{Spec: map[string]string{
					"id": "int64",
				}}),
			},
		},
		{
			name: "cast without config",
			transformation: &v1.TransformationSpec{
				Type: transformtypes.Cast,
			},
			wantErr:     true,
			errContains: "cast transformation configuration is required",
		},
	})
}

func TestCreateTransformer_Timezone(t *testing.T) {
	runCreateTransformerTests(t, []transformerTestCase{
		{
			name: "valid timezone transformation",
			transformation: &v1.TransformationSpec{
				Type: transformtypes.Timezone,
				Config: mustConfig(v1.TimezoneTransformation{
					Timezone: "Europe/Moscow",
					Fields:   []string{"created_at", "updated_at"},
					Format:   "RFC3339",
				}),
			},
		},
		{
			name: "timezone without config",
			transformation: &v1.TransformationSpec{
				Type: transformtypes.Timezone,
			},
			wantErr:     true,
			errContains: "timezone transformation configuration is required",
		},
	})
}

func TestCreateTransformer_UnsupportedType(t *testing.T) {
	transformation := &v1.TransformationSpec{
		Type: "unsupported",
	}

	transformer, err := CreateTransformer(transformation)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported transformation type")
	assert.Nil(t, transformer)
}
