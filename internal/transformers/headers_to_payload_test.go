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

func TestHeadersToPayloadTransformer_Transform(t *testing.T) {
	tests := []struct {
		name      string
		config    *v1.HeadersToPayloadTransformation
		input     map[string]interface{}
		headers   map[string]string
		want      map[string]interface{}
		wantError bool
	}{
		{
			name: "extract single header to simple field",
			config: &v1.HeadersToPayloadTransformation{
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
			config: &v1.HeadersToPayloadTransformation{
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
			config: &v1.HeadersToPayloadTransformation{
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
			config: &v1.HeadersToPayloadTransformation{
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
			config: &v1.HeadersToPayloadTransformation{
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
			config: &v1.HeadersToPayloadTransformation{
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
			transformer := NewHeadersToPayloadTransformer(tt.config)

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

			for key, expectedValue := range tt.want {
				actualValue, exists := outputData[key]
				assert.True(t, exists, "field %s should exist", key)
				assert.Equal(t, expectedValue, actualValue, "field %s should match", key)
			}
		})
	}
}

func TestHeadersToPayloadTransformer_InvalidFormat(t *testing.T) {
	config := &v1.HeadersToPayloadTransformation{
		Mappings: []string{"invalid-format"},
	}

	transformer := NewHeadersToPayloadTransformer(config)

	jsonData, err := json.Marshal(map[string]interface{}{"field": "value"})
	require.NoError(t, err)

	message := types.NewMessage(jsonData)
	message.Metadata["headers"] = map[string]string{"X-Header": "value"}

	_, err = transformer.Transform(context.Background(), message)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid header mapping format")
}

func TestHeadersToPayloadTransformer_InvalidHeadersType(t *testing.T) {
	config := &v1.HeadersToPayloadTransformation{
		Mappings: []string{"X-Request-Id:requestId"},
	}

	transformer := NewHeadersToPayloadTransformer(config)

	jsonData, err := json.Marshal(map[string]interface{}{"field": "value"})
	require.NoError(t, err)

	message := types.NewMessage(jsonData)
	message.Metadata["headers"] = "not-a-map"

	output, err := transformer.Transform(context.Background(), message)

	require.NoError(t, err)
	require.Len(t, output, 1)
	assert.Equal(t, message.Data, output[0].Data)
}

func TestHeadersToPayloadTransformer_HeadersAsInterfaceMap(t *testing.T) {
	transformer := NewHeadersToPayloadTransformer(&v1.HeadersToPayloadTransformation{
		Mappings: []string{"X-Request-Id:requestId"},
	})

	jsonData, err := json.Marshal(map[string]interface{}{"data": "value"})
	require.NoError(t, err)

	message := types.NewMessage(jsonData)
	message.Metadata["headers"] = map[string]interface{}{
		"X-Request-Id": "req-789",
	}

	output, err := transformer.Transform(context.Background(), message)
	require.NoError(t, err)
	require.Len(t, output, 1)

	var outputData map[string]interface{}
	require.NoError(t, json.Unmarshal(output[0].Data, &outputData))
	assert.Equal(t, "req-789", outputData["requestId"])
}

func TestHeadersToPayloadTransformer_BinaryPassthrough(t *testing.T) {
	transformer := NewHeadersToPayloadTransformer(&v1.HeadersToPayloadTransformation{
		Mappings: []string{"X-Request-Id:requestId"},
	})
	message := types.NewMessage([]byte("not-json"))
	message.Metadata["headers"] = map[string]string{"X-Request-Id": "req"}

	output, err := transformer.Transform(context.Background(), message)
	require.NoError(t, err)
	require.Len(t, output, 1)
	assert.Equal(t, []byte("not-json"), output[0].Data)
}

func TestNewHeadersToPayloadTransformer(t *testing.T) {
	config := &v1.HeadersToPayloadTransformation{
		Mappings: []string{"X-Header:field"},
	}

	transformer := NewHeadersToPayloadTransformer(config)
	assert.NotNil(t, transformer)
	assert.Equal(t, config, transformer.config)
}
