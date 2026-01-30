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

// TODO: HeaderFromTransformation is not yet implemented in API
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

func TestHeaderFromTransformer_Transform(t *testing.T) {
	tests := []struct {
		name      string
		config    *v1.HeaderFromTransformation
		input     map[string]interface{}
		headers   map[string]string
		want      map[string]interface{}
		wantError bool
	}{
		{
			name: "extract single header to simple field",
			config: &v1.HeaderFromTransformation{
				Mappings: []string{"X-Request-Id:requestId"},
			},
			input: map[string]interface{}{
				"data": "value",
			},
			headers: map[string]string{
				"X-Request-Id": "req-123",
			},
			want: map[string]interface{}{
				"data":      "value",
				"requestId": "req-123",
			},
		},
		{
			name: "extract header to nested field",
			config: &v1.HeaderFromTransformation{
				Mappings: []string{"X-Language:metadata.language"},
			},
			input: map[string]interface{}{
				"data": "value",
			},
			headers: map[string]string{
				"X-Language": "en",
			},
			want: map[string]interface{}{
				"data": "value",
				"metadata": map[string]interface{}{
					"language": "en",
				},
			},
		},
		{
			name: "extract multiple headers",
			config: &v1.HeaderFromTransformation{
				Mappings: []string{"X-Request-Id:requestId", "X-User-Id:userId"},
			},
			input: map[string]interface{}{
				"data": "value",
			},
			headers: map[string]string{
				"X-Request-Id": "req-123",
				"X-User-Id":    "user-456",
			},
			want: map[string]interface{}{
				"data":      "value",
				"requestId": "req-123",
				"userId":    "user-456",
			},
		},
		{
			name: "non-existent header",
			config: &v1.HeaderFromTransformation{
				Mappings: []string{"X-Missing:field"},
			},
			input: map[string]interface{}{
				"data": "value",
			},
			headers: map[string]string{
				"X-Other": "other",
			},
			want: map[string]interface{}{
				"data": "value",
			},
		},
		{
			name: "no headers in metadata",
			config: &v1.HeaderFromTransformation{
				Mappings: []string{"X-Request-Id:requestId"},
			},
			input: map[string]interface{}{
				"data": "value",
			},
			headers: nil,
			want: map[string]interface{}{
				"data": "value",
			},
		},
		{
			name: "extract header with JSONPath prefix",
			config: &v1.HeaderFromTransformation{
				Mappings: []string{"X-Request-Id:$.requestId"},
			},
			input: map[string]interface{}{
				"data": "value",
			},
			headers: map[string]string{
				"X-Request-Id": "req-123",
			},
			want: map[string]interface{}{
				"data":      "value",
				"requestId": "req-123",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer := NewHeaderFromTransformer(tt.config)

			jsonData, err := json.Marshal(tt.input)
			require.NoError(t, err)

			message := types.NewMessage(jsonData)
			if tt.headers != nil {
				message.Metadata["headers"] = tt.headers
			}

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

func TestHeaderFromTransformer_InvalidFormat(t *testing.T) {
	config := &v1.HeaderFromTransformation{
		Mappings: []string{"invalid-format"},
	}

	transformer := NewHeaderFromTransformer(config)

	jsonData, err := json.Marshal(map[string]interface{}{"field": "value"})
	require.NoError(t, err)

	message := types.NewMessage(jsonData)
	message.Metadata["headers"] = map[string]string{"X-Header": "value"}

	_, err = transformer.Transform(context.Background(), message)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid header mapping format")
}

func TestHeaderFromTransformer_InvalidHeadersType(t *testing.T) {
	config := &v1.HeaderFromTransformation{
		Mappings: []string{"X-Request-Id:requestId"},
	}

	transformer := NewHeaderFromTransformer(config)

	jsonData, err := json.Marshal(map[string]interface{}{"field": "value"})
	require.NoError(t, err)

	message := types.NewMessage(jsonData)
	// Set headers as wrong type
	message.Metadata["headers"] = "not-a-map"

	output, err := transformer.Transform(context.Background(), message)

	require.NoError(t, err)
	require.Len(t, output, 1)
	// Should return original message unchanged
	assert.Equal(t, message.Data, output[0].Data)
}

func TestNewHeaderFromTransformer(t *testing.T) {
	config := &v1.HeaderFromTransformation{
		Mappings: []string{"X-Header:field"},
	}

	transformer := NewHeaderFromTransformer(config)
	assert.NotNil(t, transformer)
	assert.Equal(t, config, transformer.config)
}
*/