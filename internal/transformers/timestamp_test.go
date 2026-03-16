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
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimestampTransformer_Transform(t *testing.T) {
	tests := []struct {
		name      string
		config    *v1.TimestampTransformation
		input     map[string]interface{}
		checkFunc func(t *testing.T, output []*types.Message, err error)
	}{
		{
			name: "add timestamp with default field name",
			config: &v1.TimestampTransformation{
				FieldName: "",
				Format:    "",
			},
			input: map[string]interface{}{
				"id":   1,
				"name": "test",
			},
			checkFunc: func(t *testing.T, output []*types.Message, err error) {
				require.NoError(t, err)
				require.Len(t, output, 1)

				var data map[string]interface{}
				err = json.Unmarshal(output[0].Data, &data)
				require.NoError(t, err)

				assert.Contains(t, data, "created_at")
				assert.NotEmpty(t, data["created_at"])
			},
		},
		{
			name: "add timestamp with custom field name",
			config: &v1.TimestampTransformation{
				FieldName: "timestamp",
				Format:    time.RFC3339,
			},
			input: map[string]interface{}{
				"id":   1,
				"name": "test",
			},
			checkFunc: func(t *testing.T, output []*types.Message, err error) {
				require.NoError(t, err)
				require.Len(t, output, 1)

				var data map[string]interface{}
				err = json.Unmarshal(output[0].Data, &data)
				require.NoError(t, err)

				assert.Contains(t, data, "timestamp")
				assert.NotContains(t, data, "created_at")

				// Verify timestamp format
				timestampStr, ok := data["timestamp"].(string)
				require.True(t, ok)
				_, err = time.Parse(time.RFC3339, timestampStr)
				assert.NoError(t, err)
			},
		},
		{
			name: "add timestamp with custom format",
			config: &v1.TimestampTransformation{
				FieldName: "time",
				Format:    "2006-01-02",
			},
			input: map[string]interface{}{
				"id":   1,
				"name": "test",
			},
			checkFunc: func(t *testing.T, output []*types.Message, err error) {
				require.NoError(t, err)
				require.Len(t, output, 1)

				var data map[string]interface{}
				err = json.Unmarshal(output[0].Data, &data)
				require.NoError(t, err)

				timestampStr, ok := data["time"].(string)
				require.True(t, ok)
				_, err = time.Parse("2006-01-02", timestampStr)
				assert.NoError(t, err)
			},
		},
		{
			name: "preserve existing fields",
			config: &v1.TimestampTransformation{
				FieldName: "created_at",
			},
			input: map[string]interface{}{
				"id":   1,
				"name": "test",
				"data": map[string]interface{}{
					"value": 42,
				},
			},
			checkFunc: func(t *testing.T, output []*types.Message, err error) {
				require.NoError(t, err)
				require.Len(t, output, 1)

				var data map[string]interface{}
				err = json.Unmarshal(output[0].Data, &data)
				require.NoError(t, err)

				assert.Equal(t, float64(1), data["id"])
				assert.Equal(t, "test", data["name"])
				assert.Contains(t, data, "created_at")
				assert.Contains(t, data, "data")
			},
		},
		{
			name: "preserve metadata",
			config: &v1.TimestampTransformation{
				FieldName: "created_at",
			},
			input: map[string]interface{}{
				"id": 1,
			},
			checkFunc: func(t *testing.T, output []*types.Message, err error) {
				require.NoError(t, err)
				require.Len(t, output, 1)

				// Check that metadata is preserved
				assert.NotNil(t, output[0].Metadata)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer := NewTimestampTransformer(tt.config)

			jsonData, err := json.Marshal(tt.input)
			require.NoError(t, err)

			message := types.NewMessage(jsonData)
			message.Metadata = map[string]interface{}{
				"source": "test",
			}

			output, err := transformer.Transform(context.Background(), message)
			tt.checkFunc(t, output, err)
		})
	}
}

func TestTimestampTransformer_InvalidJSON(t *testing.T) {
	transformer := NewTimestampTransformer(&v1.TimestampTransformation{
		FieldName: "created_at",
	})

	message := types.NewMessage([]byte("invalid json"))
	message.Metadata = map[string]interface{}{
		"source": "test",
	}

	output, err := transformer.Transform(context.Background(), message)
	require.NoError(t, err)
	require.Len(t, output, 1)
	// Should return original message unchanged
	assert.Equal(t, message.Data, output[0].Data)
	assert.Equal(t, message.Metadata, output[0].Metadata)
}

func TestNewTimestampTransformer(t *testing.T) {
	config := &v1.TimestampTransformation{
		FieldName: "timestamp",
		Format:    time.RFC3339,
	}

	transformer := NewTimestampTransformer(config)
	assert.NotNil(t, transformer)
	assert.Equal(t, config, transformer.config)
}
