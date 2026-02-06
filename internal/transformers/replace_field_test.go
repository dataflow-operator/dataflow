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

// TODO: ReplaceFieldTransformation is not yet implemented in API
// All tests are commented out until the type is added to the API

/*
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

			// Compare expected fields
			for key, expectedValue := range tt.want {
				actualValue, exists := outputData[key]
				assert.True(t, exists, "field %s should exist", key)
				assert.Equal(t, expectedValue, actualValue, "field %s should match", key)
			}
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

func TestNewReplaceFieldTransformer(t *testing.T) {
	config := &v1.ReplaceFieldTransformation{
		Renames: []string{"old:new"},
	}

	transformer := NewReplaceFieldTransformer(config)
	assert.NotNil(t, transformer)
	assert.Equal(t, config, transformer.config)
}
*/
