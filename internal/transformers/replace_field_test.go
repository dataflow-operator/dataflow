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

func TestReplaceFieldTransformer_Transform(t *testing.T) {
	tests := []struct {
		name      string
		config    *v1.ReplaceFieldTransformation
		input     map[string]interface{}
		want      map[string]interface{}
		wantError bool
	}{
		{
			name: "rename simple field",
			config: &v1.ReplaceFieldTransformation{
				Renames: []string{"oldName:newName"},
			},
			input: map[string]interface{}{
				"oldName": "value",
				"other":   "otherValue",
			},
			want: map[string]interface{}{
				"newName": "value",
				"other":   "otherValue",
			},
		},
		{
			name: "rename nested field",
			config: &v1.ReplaceFieldTransformation{
				Renames: []string{"key.sku:sku"},
			},
			input: map[string]interface{}{
				"key": map[string]interface{}{
					"sku": "12345",
				},
				"other": "value",
			},
			want: map[string]interface{}{
				"sku":   "12345",
				"other": "value",
			},
		},
		{
			name: "rename multiple fields",
			config: &v1.ReplaceFieldTransformation{
				Renames: []string{"key.sku:sku", "body.lc:lc"},
			},
			input: map[string]interface{}{
				"key": map[string]interface{}{
					"sku": "12345",
				},
				"body": map[string]interface{}{
					"lc": "en",
				},
			},
			want: map[string]interface{}{
				"sku": "12345",
				"lc":  "en",
			},
		},
		{
			name: "rename to nested path",
			config: &v1.ReplaceFieldTransformation{
				Renames: []string{"oldName:new.path"},
			},
			input: map[string]interface{}{
				"oldName": "value",
			},
			want: map[string]interface{}{
				"new": map[string]interface{}{
					"path": "value",
				},
			},
		},
		{
			name: "rename with JSONPath prefix",
			config: &v1.ReplaceFieldTransformation{
				Renames: []string{"$.key.sku:sku"},
			},
			input: map[string]interface{}{
				"key": map[string]interface{}{
					"sku": "12345",
				},
			},
			want: map[string]interface{}{
				"sku": "12345",
			},
		},
		{
			name: "non-existent field",
			config: &v1.ReplaceFieldTransformation{
				Renames: []string{"nonexistent:newName"},
			},
			input: map[string]interface{}{
				"other": "value",
			},
			want: map[string]interface{}{
				"other": "value",
			},
		},
		{
			name: "rename to same path",
			config: &v1.ReplaceFieldTransformation{
				Renames: []string{"field:field"},
			},
			input: map[string]interface{}{
				"field": "value",
			},
			want: map[string]interface{}{
				"field": "value",
			},
		},
		{
			name: "include preserves nesting",
			config: &v1.ReplaceFieldTransformation{
				Include: []string{"user.id", "user.name", "status"},
			},
			input: map[string]interface{}{
				"user": map[string]interface{}{
					"id":    float64(1),
					"name":  "John",
					"email": "john@example.com",
				},
				"status": "active",
				"extra":  "drop-me",
			},
			want: map[string]interface{}{
				"user": map[string]interface{}{
					"id":   float64(1),
					"name": "John",
				},
				"status": "active",
			},
		},
		{
			name: "exclude removes fields",
			config: &v1.ReplaceFieldTransformation{
				Exclude: []string{"password", "user.secret"},
			},
			input: map[string]interface{}{
				"id":       float64(1),
				"password": "secret",
				"user": map[string]interface{}{
					"name":   "John",
					"secret": "x",
				},
			},
			want: map[string]interface{}{
				"id": float64(1),
				"user": map[string]interface{}{
					"name": "John",
				},
			},
		},
		{
			name: "include then rename",
			config: &v1.ReplaceFieldTransformation{
				Include: []string{"oldName", "keep"},
				Renames: []string{"oldName:newName"},
			},
			input: map[string]interface{}{
				"oldName": "value",
				"keep":    "yes",
				"drop":    "no",
			},
			want: map[string]interface{}{
				"newName": "value",
				"keep":    "yes",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer := NewReplaceFieldTransformer(tt.config)

			jsonData, err := json.Marshal(tt.input)
			require.NoError(t, err)

			message := types.NewMessage(jsonData)
			output, err := transformer.Transform(context.Background(), message)

			if tt.wantError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Len(t, output, 1)

			var outputData map[string]interface{}
			err = json.Unmarshal(output[0].Data, &outputData)
			require.NoError(t, err)

			assert.Equal(t, tt.want, outputData)
		})
	}
}

func TestReplaceFieldTransformer_InvalidFormat(t *testing.T) {
	config := &v1.ReplaceFieldTransformation{
		Renames: []string{"invalid-format"},
	}

	transformer := NewReplaceFieldTransformer(config)

	jsonData, err := json.Marshal(map[string]interface{}{"field": "value"})
	require.NoError(t, err)

	message := types.NewMessage(jsonData)
	_, err = transformer.Transform(context.Background(), message)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid rename format")
}

func TestReplaceFieldTransformer_BinaryPassthrough(t *testing.T) {
	transformer := NewReplaceFieldTransformer(&v1.ReplaceFieldTransformation{
		Renames: []string{"a:b"},
	})
	message := types.NewMessage([]byte("not-json"))
	output, err := transformer.Transform(context.Background(), message)
	require.NoError(t, err)
	require.Len(t, output, 1)
	assert.Equal(t, []byte("not-json"), output[0].Data)
}

func TestNewReplaceFieldTransformer(t *testing.T) {
	config := &v1.ReplaceFieldTransformation{
		Renames: []string{"old:new"},
	}

	transformer := NewReplaceFieldTransformer(config)
	assert.NotNil(t, transformer)
	assert.Equal(t, config, transformer.config)
}
