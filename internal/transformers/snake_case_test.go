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

func TestSnakeCaseTransformer_Transform(t *testing.T) {
	tests := []struct {
		name      string
		config    *v1.SnakeCaseTransformation
		input     map[string]interface{}
		checkFunc func(t *testing.T, output []*types.Message, err error)
	}{
		{
			name: "convert simple camelCase to snake_case",
			config: &v1.SnakeCaseTransformation{
				Deep: false,
			},
			input: map[string]interface{}{
				"firstName": "John",
				"lastName":  "Doe",
				"userName":  "johndoe",
				"isActive":  true,
				"itemCount": 42,
			},
			checkFunc: func(t *testing.T, output []*types.Message, err error) {
				require.NoError(t, err)
				require.Len(t, output, 1)

				var data map[string]interface{}
				err = json.Unmarshal(output[0].Data, &data)
				require.NoError(t, err)

				assert.Contains(t, data, "first_name")
				assert.Contains(t, data, "last_name")
				assert.Contains(t, data, "user_name")
				assert.Contains(t, data, "is_active")
				assert.Contains(t, data, "item_count")
				assert.Equal(t, "John", data["first_name"])
				assert.Equal(t, "Doe", data["last_name"])
				assert.Equal(t, true, data["is_active"])
				assert.Equal(t, float64(42), data["item_count"])
			},
		},
		{
			name: "convert PascalCase to snake_case",
			config: &v1.SnakeCaseTransformation{
				Deep: false,
			},
			input: map[string]interface{}{
				"FirstName": "John",
				"LastName":  "Doe",
				"UserID":    123,
			},
			checkFunc: func(t *testing.T, output []*types.Message, err error) {
				require.NoError(t, err)
				require.Len(t, output, 1)

				var data map[string]interface{}
				err = json.Unmarshal(output[0].Data, &data)
				require.NoError(t, err)

				assert.Contains(t, data, "first_name")
				assert.Contains(t, data, "last_name")
				assert.Contains(t, data, "user_id")
			},
		},
		{
			name: "convert with deep recursion",
			config: &v1.SnakeCaseTransformation{
				Deep: true,
			},
			input: map[string]interface{}{
				"firstName": "John",
				"address": map[string]interface{}{
					"streetName":  "Main St",
					"houseNumber": 123,
					"zipCode":     "12345",
				},
				"items": []interface{}{
					map[string]interface{}{
						"itemName":  "Product",
						"itemPrice": 99.99,
					},
				},
			},
			checkFunc: func(t *testing.T, output []*types.Message, err error) {
				require.NoError(t, err)
				require.Len(t, output, 1)

				var data map[string]interface{}
				err = json.Unmarshal(output[0].Data, &data)
				require.NoError(t, err)

				assert.Contains(t, data, "first_name")
				assert.Contains(t, data, "address")

				address, ok := data["address"].(map[string]interface{})
				require.True(t, ok)
				assert.Contains(t, address, "street_name")
				assert.Contains(t, address, "house_number")
				assert.Contains(t, address, "zip_code")

				items, ok := data["items"].([]interface{})
				require.True(t, ok)
				require.Len(t, items, 1)

				item, ok := items[0].(map[string]interface{})
				require.True(t, ok)
				assert.Contains(t, item, "item_name")
				assert.Contains(t, item, "item_price")
			},
		},
		{
			name: "preserve already snake_case keys",
			config: &v1.SnakeCaseTransformation{
				Deep: false,
			},
			input: map[string]interface{}{
				"first_name": "John",
				"last_name":  "Doe",
				"user_id":    123,
			},
			checkFunc: func(t *testing.T, output []*types.Message, err error) {
				require.NoError(t, err)
				require.Len(t, output, 1)

				var data map[string]interface{}
				err = json.Unmarshal(output[0].Data, &data)
				require.NoError(t, err)

				assert.Contains(t, data, "first_name")
				assert.Contains(t, data, "last_name")
				assert.Contains(t, data, "user_id")
			},
		},
		{
			name: "preserve metadata",
			config: &v1.SnakeCaseTransformation{
				Deep: false,
			},
			input: map[string]interface{}{
				"firstName": "John",
			},
			checkFunc: func(t *testing.T, output []*types.Message, err error) {
				require.NoError(t, err)
				require.Len(t, output, 1)

				assert.NotNil(t, output[0].Metadata)
				assert.Equal(t, map[string]interface{}{"source": "test"}, output[0].Metadata)
			},
		},
		{
			name: "handle empty object",
			config: &v1.SnakeCaseTransformation{
				Deep: false,
			},
			input: map[string]interface{}{},
			checkFunc: func(t *testing.T, output []*types.Message, err error) {
				require.NoError(t, err)
				require.Len(t, output, 1)

				var data map[string]interface{}
				err = json.Unmarshal(output[0].Data, &data)
				require.NoError(t, err)

				assert.Empty(t, data)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer := NewSnakeCaseTransformer(tt.config)

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

func TestSnakeCaseTransformer_InvalidJSON(t *testing.T) {
	transformer := NewSnakeCaseTransformer(&v1.SnakeCaseTransformation{
		Deep: false,
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

func TestToSnakeCase(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"camelCase", "firstName", "first_name"},
		{"PascalCase", "FirstName", "first_name"},
		{"multiple words", "userName", "user_name"},
		{"already snake_case", "first_name", "first_name"},
		{"with numbers", "userID123", "user_id123"},
		{"single word", "name", "name"},
		{"all caps", "USERNAME", "username"},
		{"mixed case", "XMLHttpRequest", "xml_http_request"},
		{"empty string", "", ""},
		{"with underscores", "user_name", "user_name"},
		{"consecutive capitals", "XMLParser", "xml_parser"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toSnakeCase(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNewSnakeCaseTransformer(t *testing.T) {
	config := &v1.SnakeCaseTransformation{
		Deep: true,
	}

	transformer := NewSnakeCaseTransformer(config)
	assert.NotNil(t, transformer)
	assert.Equal(t, config, transformer.config)
}
