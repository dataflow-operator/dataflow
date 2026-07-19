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
	"context"
	"encoding/json"
	"testing"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractFieldTransformer_Transform(t *testing.T) {
	tests := []struct {
		name   string
		config *v1.ExtractFieldTransformation
		input  string
		want   string
	}{
		{
			name:   "extract nested object",
			config: &v1.ExtractFieldTransformation{Field: "payload.after"},
			input:  `{"payload":{"after":{"id":1}}}`,
			want:   `{"id":1}`,
		},
		{
			name:   "extract with JSONPath prefix",
			config: &v1.ExtractFieldTransformation{Field: "$.payload.after"},
			input:  `{"payload":{"after":{"id":1}}}`,
			want:   `{"id":1}`,
		},
		{
			name:   "extract primitive number",
			config: &v1.ExtractFieldTransformation{Field: "id"},
			input:  `{"id":42,"name":"x"}`,
			want:   `42`,
		},
		{
			name:   "extract array",
			config: &v1.ExtractFieldTransformation{Field: "items"},
			input:  `{"items":[1,2,3]}`,
			want:   `[1,2,3]`,
		},
		{
			name:   "extract null",
			config: &v1.ExtractFieldTransformation{Field: "deleted"},
			input:  `{"deleted":null}`,
			want:   `null`,
		},
		{
			name:   "missing path passthrough",
			config: &v1.ExtractFieldTransformation{Field: "missing"},
			input:  `{"id":1}`,
			want:   `{"id":1}`,
		},
		{
			name:   "preserves metadata",
			config: &v1.ExtractFieldTransformation{Field: "row"},
			input:  `{"row":{"a":1}}`,
			want:   `{"a":1}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := types.NewMessage([]byte(tt.input))
			msg.Metadata["operation"] = "insert"

			out, err := NewExtractFieldTransformer(tt.config).Transform(context.Background(), msg)
			require.NoError(t, err)
			require.Len(t, out, 1)
			assert.JSONEq(t, tt.want, string(out[0].Data))
			assert.Equal(t, "insert", out[0].Metadata["operation"])
		})
	}
}

func TestExtractFieldTransformer_Passthrough(t *testing.T) {
	transformer := NewExtractFieldTransformer(&v1.ExtractFieldTransformation{Field: "payload"})

	t.Run("non-JSON", func(t *testing.T) {
		msg := types.NewMessage([]byte("not-json"))
		out, err := transformer.Transform(context.Background(), msg)
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.Equal(t, []byte("not-json"), out[0].Data)
	})

	t.Run("JSON array root still searchable", func(t *testing.T) {
		msg := types.NewMessage([]byte(`[{"id":1},{"id":2}]`))
		out, err := NewExtractFieldTransformer(&v1.ExtractFieldTransformation{Field: "0"}).Transform(context.Background(), msg)
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.JSONEq(t, `{"id":1}`, string(out[0].Data))
	})
}

func TestNewExtractFieldTransformer(t *testing.T) {
	config := &v1.ExtractFieldTransformation{Field: "payload"}
	transformer := NewExtractFieldTransformer(config)
	require.NotNil(t, transformer)
	assert.Equal(t, config, transformer.config)

	// Ensure result is valid JSON bytes for object extract
	msg := types.NewMessage([]byte(`{"payload":{"x":true}}`))
	out, err := transformer.Transform(context.Background(), msg)
	require.NoError(t, err)
	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(out[0].Data, &decoded))
}
