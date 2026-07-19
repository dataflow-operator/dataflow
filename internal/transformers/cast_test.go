/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    10|Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package transformers

import (
	"context"
	"testing"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCastTransformer_Transform(t *testing.T) {
	tests := []struct {
		name   string
		config *v1.CastTransformation
		input  string
		want   string
	}{
		{
			name: "cast multiple types",
			config: &v1.CastTransformation{Spec: map[string]string{
				"id":         "int64",
				"amount":     "float64",
				"active":     "bool",
				"note":       "string",
				"deleted_at": "null",
			}},
			input: `{"id":"42","amount":"1.5","active":"true","note":7,"deleted_at":"x"}`,
			want:  `{"id":42,"amount":1.5,"active":true,"note":"7","deleted_at":null}`,
		},
		{
			name: "nested path with JSONPath prefix",
			config: &v1.CastTransformation{Spec: map[string]string{
				"$.row.id": "int64",
			}},
			input: `{"row":{"id":"9"}}`,
			want:  `{"row":{"id":9}}`,
		},
		{
			name: "missing path skipped",
			config: &v1.CastTransformation{Spec: map[string]string{
				"missing": "int64",
				"id":      "string",
			}},
			input: `{"id":1}`,
			want:  `{"id":"1"}`,
		},
		{
			name: "int from whole float",
			config: &v1.CastTransformation{Spec: map[string]string{
				"n": "int64",
			}},
			input: `{"n":10.0}`,
			want:  `{"n":10}`,
		},
		{
			name: "bool from 0/1",
			config: &v1.CastTransformation{Spec: map[string]string{
				"a": "bool",
				"b": "bool",
			}},
			input: `{"a":1,"b":0}`,
			want:  `{"a":true,"b":false}`,
		},
		{
			name: "null overwrites any value",
			config: &v1.CastTransformation{Spec: map[string]string{
				"x": "null",
			}},
			input: `{"x":{"nested":true}}`,
			want:  `{"x":null}`,
		},
		{
			name: "preserves metadata",
			config: &v1.CastTransformation{Spec: map[string]string{
				"id": "int64",
			}},
			input: `{"id":"1"}`,
			want:  `{"id":1}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := types.NewMessage([]byte(tt.input))
			msg.Metadata["operation"] = "insert"

			out, err := NewCastTransformer(tt.config).Transform(context.Background(), msg)
			require.NoError(t, err)
			require.Len(t, out, 1)
			assert.JSONEq(t, tt.want, string(out[0].Data))
			assert.Equal(t, "insert", out[0].Metadata["operation"])
		})
	}
}

func TestCastTransformer_Errors(t *testing.T) {
	tests := []struct {
		name    string
		config  *v1.CastTransformation
		input   string
		wantErr string
	}{
		{
			name: "object to string",
			config: &v1.CastTransformation{Spec: map[string]string{
				"obj": "string",
			}},
			input:   `{"obj":{"a":1}}`,
			wantErr: "cannot cast object/array to string",
		},
		{
			name: "fractional float to int64",
			config: &v1.CastTransformation{Spec: map[string]string{
				"n": "int64",
			}},
			input:   `{"n":1.5}`,
			wantErr: "not an integer",
		},
		{
			name: "invalid bool string",
			config: &v1.CastTransformation{Spec: map[string]string{
				"flag": "bool",
			}},
			input:   `{"flag":"yes"}`,
			wantErr: "cannot cast \"yes\" to bool",
		},
		{
			name: "null to int64",
			config: &v1.CastTransformation{Spec: map[string]string{
				"n": "int64",
			}},
			input:   `{"n":null}`,
			wantErr: "cannot cast Null to int64",
		},
		{
			name: "non-numeric string to float64",
			config: &v1.CastTransformation{Spec: map[string]string{
				"amount": "float64",
			}},
			input:   `{"amount":"abc"}`,
			wantErr: "cannot cast \"abc\" to float64",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := types.NewMessage([]byte(tt.input))
			out, err := NewCastTransformer(tt.config).Transform(context.Background(), msg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Nil(t, out)
		})
	}
}

func TestCastTransformer_Passthrough(t *testing.T) {
	transformer := NewCastTransformer(&v1.CastTransformation{Spec: map[string]string{
		"id": "int64",
	}})

	msg := types.NewMessage([]byte("not-json"))
	out, err := transformer.Transform(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, []byte("not-json"), out[0].Data)
}

func TestNewCastTransformer(t *testing.T) {
	config := &v1.CastTransformation{Spec: map[string]string{"id": "int64"}}
	transformer := NewCastTransformer(config)
	require.NotNil(t, transformer)
	assert.Equal(t, config, transformer.config)
}
