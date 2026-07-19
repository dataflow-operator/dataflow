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
	"strings"
	"testing"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStructFlattenTransformer_Transform(t *testing.T) {
	tests := []struct {
		name   string
		config *v1.StructFlattenTransformation
		input  string
		want   map[string]interface{}
	}{
		{
			name:   "nested objects with default delimiter",
			config: &v1.StructFlattenTransformation{},
			input: `{
				"content": {
					"id": 42,
					"name": {
						"first": "David",
						"middle": null,
						"last": "Wong"
					},
					"tags": ["a", "b"]
				},
				"active": true
			}`,
			want: map[string]interface{}{
				"content.id":          float64(42),
				"content.name.first":  "David",
				"content.name.middle": nil,
				"content.name.last":   "Wong",
				"content.tags":        []interface{}{"a", "b"},
				"active":              true,
			},
		},
		{
			name:   "underscore delimiter",
			config: &v1.StructFlattenTransformation{Delimiter: "_"},
			input: `{
				"content": {
					"id": 42,
					"name": {"first": "David"}
				},
				"active": true
			}`,
			want: map[string]interface{}{
				"content_id":         float64(42),
				"content_name_first": "David",
				"active":             true,
			},
		},
		{
			name:   "array of objects preserved",
			config: &v1.StructFlattenTransformation{},
			input:  `{"items":[{"x":1}]}`,
			want: map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{"x": float64(1)},
				},
			},
		},
		{
			name:   "empty nested object produces no keys",
			config: &v1.StructFlattenTransformation{},
			input:  `{"keep":1,"empty":{}}`,
			want: map[string]interface{}{
				"keep": float64(1),
			},
		},
		{
			name:   "already flat object",
			config: &v1.StructFlattenTransformation{},
			input:  `{"a":1,"b":"x"}`,
			want: map[string]interface{}{
				"a": float64(1),
				"b": "x",
			},
		},
		{
			name:   "nested overwrites flat collision",
			config: &v1.StructFlattenTransformation{},
			input:  `{"a.b":1,"a":{"b":2}}`,
			want: map[string]interface{}{
				"a.b": float64(2),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer := NewStructFlattenTransformer(tt.config)
			msg := types.NewMessage([]byte(tt.input))
			msg.Metadata = map[string]interface{}{"trace": "keep"}
			out, err := transformer.Transform(context.Background(), msg)
			require.NoError(t, err)
			require.Len(t, out, 1)
			assert.Equal(t, msg.Metadata, out[0].Metadata)

			var got map[string]interface{}
			require.NoError(t, json.Unmarshal(out[0].Data, &got))
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestStructFlattenTransformer_Passthrough(t *testing.T) {
	transformer := NewStructFlattenTransformer(&v1.StructFlattenTransformation{})

	t.Run("non-json", func(t *testing.T) {
		msg := types.NewMessage([]byte("not-json"))
		out, err := transformer.Transform(context.Background(), msg)
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.Equal(t, msg.Data, out[0].Data)
	})

	t.Run("array root", func(t *testing.T) {
		msg := types.NewMessage([]byte(`[1,2]`))
		out, err := transformer.Transform(context.Background(), msg)
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.Equal(t, msg.Data, out[0].Data)
	})

	t.Run("primitive root", func(t *testing.T) {
		msg := types.NewMessage([]byte(`42`))
		out, err := transformer.Transform(context.Background(), msg)
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.Equal(t, msg.Data, out[0].Data)
	})
}

func TestStructFlattenTransformer_MaxDepth(t *testing.T) {
	nested := map[string]interface{}{"v": 1}
	for i := 0; i < structFlattenMaxDepth+1; i++ {
		nested = map[string]interface{}{"n": nested}
	}
	data, err := json.Marshal(nested)
	require.NoError(t, err)

	transformer := NewStructFlattenTransformer(&v1.StructFlattenTransformation{})
	_, err = transformer.Transform(context.Background(), types.NewMessage(data))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nesting depth exceeds")
}

func TestStructFlattenTransformer_DeepNest(t *testing.T) {
	// depth == maxDepth should succeed
	nested := map[string]interface{}{"v": "ok"}
	for i := 0; i < structFlattenMaxDepth; i++ {
		nested = map[string]interface{}{"n": nested}
	}
	data, err := json.Marshal(nested)
	require.NoError(t, err)

	transformer := NewStructFlattenTransformer(&v1.StructFlattenTransformation{})
	out, err := transformer.Transform(context.Background(), types.NewMessage(data))
	require.NoError(t, err)
	require.Len(t, out, 1)

	var got map[string]interface{}
	require.NoError(t, json.Unmarshal(out[0].Data, &got))
	key := strings.Repeat("n.", structFlattenMaxDepth) + "v"
	assert.Equal(t, "ok", got[key])
}

func TestNewStructFlattenTransformer(t *testing.T) {
	config := &v1.StructFlattenTransformation{Delimiter: "_"}
	transformer := NewStructFlattenTransformer(config)
	require.NotNil(t, transformer)
	assert.Equal(t, config, transformer.config)
}
