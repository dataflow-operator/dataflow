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

func TestFilterTransformer_Transform(t *testing.T) {
	tests := []struct {
		name      string
		config    *v1.FilterTransformation
		input     map[string]interface{}
		wantCount int
	}{
		{
			name: "filter by boolean field true",
			config: &v1.FilterTransformation{
				Condition: "active",
			},
			input: map[string]interface{}{
				"id":     1,
				"name":   "test",
				"active": true,
			},
			wantCount: 1,
		},
		{
			name: "filter by boolean field false",
			config: &v1.FilterTransformation{
				Condition: "active",
			},
			input: map[string]interface{}{
				"id":     1,
				"name":   "test",
				"active": false,
			},
			wantCount: 0,
		},
		{
			name: "filter by string field",
			config: &v1.FilterTransformation{
				Condition: "status",
			},
			input: map[string]interface{}{
				"id":     1,
				"status": "active",
			},
			wantCount: 1,
		},
		{
			name: "filter by empty string",
			config: &v1.FilterTransformation{
				Condition: "status",
			},
			input: map[string]interface{}{
				"id":     1,
				"status": "",
			},
			wantCount: 0,
		},
		{
			name: "filter by number field",
			config: &v1.FilterTransformation{
				Condition: "count",
			},
			input: map[string]interface{}{
				"id":    1,
				"count": 5,
			},
			wantCount: 1,
		},
		{
			name: "filter by zero number",
			config: &v1.FilterTransformation{
				Condition: "count",
			},
			input: map[string]interface{}{
				"id":    1,
				"count": 0,
			},
			wantCount: 0,
		},
		{
			name: "filter by nested field",
			config: &v1.FilterTransformation{
				Condition: "user.active",
			},
			input: map[string]interface{}{
				"id": 1,
				"user": map[string]interface{}{
					"active": true,
					"name":   "test",
				},
			},
			wantCount: 1,
		},
		{
			name: "filter by non-existent field",
			config: &v1.FilterTransformation{
				Condition: "nonexistent",
			},
			input: map[string]interface{}{
				"id":   1,
				"name": "test",
			},
			wantCount: 0,
		},
		{
			name: "filter by null field",
			config: &v1.FilterTransformation{
				Condition: "value",
			},
			input: map[string]interface{}{
				"id":    1,
				"value": nil,
			},
			wantCount: 0,
		},
		{
			name: "filter eq match",
			config: &v1.FilterTransformation{
				Condition: "$.status == 'active'",
			},
			input: map[string]interface{}{
				"id":     1,
				"status": "active",
			},
			wantCount: 1,
		},
		{
			name: "filter eq mismatch",
			config: &v1.FilterTransformation{
				Condition: "$.status == 'active'",
			},
			input: map[string]interface{}{
				"id":     1,
				"status": "inactive",
			},
			wantCount: 0,
		},
		{
			name: "filter neq match",
			config: &v1.FilterTransformation{
				Condition: "$.status != 'deleted'",
			},
			input: map[string]interface{}{
				"id":     1,
				"status": "active",
			},
			wantCount: 1,
		},
		{
			name: "filter neq mismatch",
			config: &v1.FilterTransformation{
				Condition: "$.status != 'deleted'",
			},
			input: map[string]interface{}{
				"id":     1,
				"status": "deleted",
			},
			wantCount: 0,
		},
		{
			name: "filter eq missing field",
			config: &v1.FilterTransformation{
				Condition: "$.status == 'active'",
			},
			input: map[string]interface{}{
				"id": 1,
			},
			wantCount: 0,
		},
		{
			name: "filter neq missing field",
			config: &v1.FilterTransformation{
				Condition: "$.status != 'deleted'",
			},
			input: map[string]interface{}{
				"id": 1,
			},
			wantCount: 0,
		},
		{
			name: "filter eq unquoted bool",
			config: &v1.FilterTransformation{
				Condition: "$.enabled == true",
			},
			input: map[string]interface{}{
				"id":      1,
				"enabled": true,
			},
			wantCount: 1,
		},
		{
			name: "filter eq with dollar prefix truthy path still works via comparison",
			config: &v1.FilterTransformation{
				Condition: `$.type == "order"`,
			},
			input: map[string]interface{}{
				"type": "order",
			},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer := NewFilterTransformer(tt.config)

			jsonData, err := json.Marshal(tt.input)
			require.NoError(t, err)

			message := types.NewMessage(jsonData)
			output, err := transformer.Transform(context.Background(), message)

			require.NoError(t, err)
			assert.Len(t, output, tt.wantCount)

			if tt.wantCount > 0 && len(output) > 0 {
				// Verify the message is unchanged
				var outputData map[string]interface{}
				err = json.Unmarshal(output[0].Data, &outputData)
				require.NoError(t, err)

				// Compare with original input (JSON numbers become float64)
				// So we need to compare values, not exact types
				for key, expectedValue := range tt.input {
					actualValue, exists := outputData[key]
					assert.True(t, exists, "field %s should exist", key)

					// Handle numeric comparison (int vs float64)
					switch ev := expectedValue.(type) {
					case int:
						if av, ok := actualValue.(float64); ok {
							assert.Equal(t, float64(ev), av, "field %s should match", key)
						} else {
							assert.Equal(t, expectedValue, actualValue, "field %s should match", key)
						}
					default:
						assert.Equal(t, expectedValue, actualValue, "field %s should match", key)
					}
				}
			}
		})
	}
}

func TestNewFilterTransformer(t *testing.T) {
	config := &v1.FilterTransformation{
		Condition: "$.active",
	}

	transformer := NewFilterTransformer(config)
	assert.NotNil(t, transformer)
	assert.Equal(t, config, transformer.config)
}
