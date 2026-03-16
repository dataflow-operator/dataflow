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

var binaryData = []byte{0x00, 0x00, 0x00, 0x00, 0x09, 0x0a, 0x73, 0x74, 0x6f, 0x63, 0x6b, 0xff, 0xfd, 0xfd}

func testBinaryDataPassthrough(t *testing.T, transformer Transformer) {
	t.Helper()
	message := types.NewMessage(binaryData)
	message.Metadata = map[string]interface{}{
		"source": "test",
	}

	output, err := transformer.Transform(context.Background(), message)
	require.NoError(t, err)
	require.Len(t, output, 1)
	assert.Equal(t, message.Data, output[0].Data)
	assert.Equal(t, message.Metadata, output[0].Metadata)
}

func TestFlattenTransformer_BinaryData(t *testing.T) {
	testBinaryDataPassthrough(t, NewFlattenTransformer(&v1.FlattenTransformation{
		Field: "items",
	}))
}

func TestMaskTransformer_BinaryData(t *testing.T) {
	testBinaryDataPassthrough(t, NewMaskTransformer(&v1.MaskTransformation{
		Fields: []string{"password"},
	}))
}

func TestRemoveTransformer_BinaryData(t *testing.T) {
	testBinaryDataPassthrough(t, NewRemoveTransformer(&v1.RemoveTransformation{
		Fields: []string{"password"},
	}))
}

func TestSelectTransformer_BinaryData(t *testing.T) {
	testBinaryDataPassthrough(t, NewSelectTransformer(&v1.SelectTransformation{
		Fields: []string{"id", "name"},
	}))
}

func TestRouterTransformer_BinaryData(t *testing.T) {
	testBinaryDataPassthrough(t, NewRouterTransformer(&v1.RouterTransformation{
		Routes: []v1.RouteRule{
			{
				Condition: "$.type == 'order'",
				Sink: v1.SinkSpec{
					Type: "kafka",
				},
			},
		},
	}))
}

func TestTimestampTransformer_BinaryData(t *testing.T) {
	testBinaryDataPassthrough(t, NewTimestampTransformer(&v1.TimestampTransformation{
		FieldName: "created_at",
	}))
}

func TestIsValidJSON(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{
			name:     "valid JSON object",
			data:     []byte(`{"key": "value"}`),
			expected: true,
		},
		{
			name:     "valid JSON array",
			data:     []byte(`[1, 2, 3]`),
			expected: true,
		},
		{
			name:     "invalid JSON",
			data:     []byte(`invalid json`),
			expected: false,
		},
		{
			name:     "binary data with null bytes",
			data:     binaryData,
			expected: false,
		},
		{
			name:     "empty string",
			data:     []byte(``),
			expected: false,
		},
		{
			name:     "null bytes only",
			data:     []byte{0x00, 0x00, 0x00},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isValidJSON(tt.data)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTransformers_ValidJSON(t *testing.T) {
	validJSON, err := json.Marshal(map[string]interface{}{
		"id":   1,
		"name": "test",
	})
	require.NoError(t, err)

	message := types.NewMessage(validJSON)
	message.Metadata = map[string]interface{}{
		"source": "test",
	}

	timestampTransformer := NewTimestampTransformer(&v1.TimestampTransformation{
		FieldName: "created_at",
	})
	output, err := timestampTransformer.Transform(context.Background(), message)
	require.NoError(t, err)
	require.Len(t, output, 1)
	var data map[string]interface{}
	err = json.Unmarshal(output[0].Data, &data)
	require.NoError(t, err)
	assert.Contains(t, data, "created_at")
}
