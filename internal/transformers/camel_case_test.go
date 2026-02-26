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

func TestCamelCaseTransformer_Transform(t *testing.T) {
	tests := []struct {
		name      string
		config    *v1.CamelCaseTransformation
		input     map[string]interface{}
		checkFunc func(t *testing.T, output []*types.Message, err error)
	}{
		{
			name: "convert snake_case to CamelCase",
			config: &v1.CamelCaseTransformation{
				Deep: false,
			},
			input: map[string]interface{}{
				"first_name": "John",
				"last_name":  "Doe",
				"user_name":  "johndoe",
				"is_active":  true,
				"item_count": 42,
			},
			checkFunc: func(t *testing.T, output []*types.Message, err error) {
				require.NoError(t, err)
				require.Len(t, output, 1)

				var data map[string]interface{}
				err = json.Unmarshal(output[0].Data, &data)
				require.NoError(t, err)

				assert.Contains(t, data, "FirstName")
				assert.Contains(t, data, "LastName")
				assert.Contains(t, data, "UserName")
				assert.Contains(t, data, "IsActive")
				assert.Contains(t, data, "ItemCount")
				assert.Equal(t, "John", data["FirstName"])
				assert.Equal(t, "Doe", data["LastName"])
				assert.Equal(t, true, data["IsActive"])
				assert.Equal(t, float64(42), data["ItemCount"])
			},
		},
		{
			name: "convert with deep recursion",
			config: &v1.CamelCaseTransformation{
				Deep: true,
			},
			input: map[string]interface{}{
				"first_name": "John",
				"address": map[string]interface{}{
					"street_name":  "Main St",
					"house_number": 123,
					"zip_code":     "12345",
				},
				"items": []interface{}{
					map[string]interface{}{
						"item_name":  "Product",
						"item_price": 99.99,
					},
				},
			},
			checkFunc: func(t *testing.T, output []*types.Message, err error) {
				require.NoError(t, err)
				require.Len(t, output, 1)

				var data map[string]interface{}
				err = json.Unmarshal(output[0].Data, &data)
				require.NoError(t, err)

				assert.Contains(t, data, "FirstName")
				assert.Contains(t, data, "Address")

				address, ok := data["Address"].(map[string]interface{})
				require.True(t, ok)
				assert.Contains(t, address, "StreetName")
				assert.Contains(t, address, "HouseNumber")
				assert.Contains(t, address, "ZipCode")

				items, ok := data["Items"].([]interface{})
				require.True(t, ok)
				require.Len(t, items, 1)

				item, ok := items[0].(map[string]interface{})
				require.True(t, ok)
				assert.Contains(t, item, "ItemName")
				assert.Contains(t, item, "ItemPrice")
			},
		},
		{
			name: "preserve already CamelCase keys",
			config: &v1.CamelCaseTransformation{
				Deep: false,
			},
			input: map[string]interface{}{
				"FirstName": "John",
				"LastName":  "Doe",
				"UserId":    123,
			},
			checkFunc: func(t *testing.T, output []*types.Message, err error) {
				require.NoError(t, err)
				require.Len(t, output, 1)

				var data map[string]interface{}
				err = json.Unmarshal(output[0].Data, &data)
				require.NoError(t, err)

				assert.Contains(t, data, "FirstName")
				assert.Contains(t, data, "LastName")
				assert.Contains(t, data, "UserId")
			},
		},
		{
			name: "preserve metadata",
			config: &v1.CamelCaseTransformation{
				Deep: false,
			},
			input: map[string]interface{}{
				"first_name": "John",
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
			config: &v1.CamelCaseTransformation{
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
		{
			name: "handle single word",
			config: &v1.CamelCaseTransformation{
				Deep: false,
			},
			input: map[string]interface{}{
				"name": "John",
			},
			checkFunc: func(t *testing.T, output []*types.Message, err error) {
				require.NoError(t, err)
				require.Len(t, output, 1)

				var data map[string]interface{}
				err = json.Unmarshal(output[0].Data, &data)
				require.NoError(t, err)

				assert.Contains(t, data, "Name")
				assert.Equal(t, "John", data["Name"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer := NewCamelCaseTransformer(tt.config)

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

func TestCamelCaseTransformer_InvalidJSON(t *testing.T) {
	transformer := NewCamelCaseTransformer(&v1.CamelCaseTransformation{
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

func TestToCamelCase(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"snake_case", "first_name", "FirstName"},
		{"single word", "name", "Name"},
		{"multiple underscores", "user_name_id", "UserNameId"},
		{"already CamelCase", "FirstName", "FirstName"},
		{"with numbers", "user_id_123", "UserId123"},
		{"empty string", "", ""},
		{"single underscore", "name_", "Name"},
		{"leading underscore", "_name", "Name"},
		{"multiple consecutive underscores", "user__name", "UserName"},
		{"all lowercase", "username", "Username"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toCamelCase(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNewCamelCaseTransformer(t *testing.T) {
	config := &v1.CamelCaseTransformation{
		Deep: true,
	}

	transformer := NewCamelCaseTransformer(config)
	assert.NotNil(t, transformer)
	assert.Equal(t, config, transformer.config)
}
