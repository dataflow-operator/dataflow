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

package integration

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/transformers"
	"github.com/dataflow-operator/dataflow/internal/types"
)

// TestTransformersIntegration тестирует все трансформеры с реальными данными
func TestTransformersIntegration(t *testing.T) {
	ctx := context.Background()

	t.Run("Timestamp Transformer", func(t *testing.T) {
		transformer := transformers.NewTimestampTransformer(&v1.TimestampTransformation{
			FieldName: "created_at",
			Format:    "2006-01-02T15:04:05Z07:00",
		})

		testData := map[string]interface{}{
			"id":   1,
			"name": "test",
		}
		jsonData, err := json.Marshal(testData)
		require.NoError(t, err)

		message := types.NewMessage(jsonData)
		result, err := transformer.Transform(ctx, message)
		require.NoError(t, err)
		require.Len(t, result, 1)

		var output map[string]interface{}
		err = json.Unmarshal(result[0].Data, &output)
		require.NoError(t, err)

		assert.Contains(t, output, "created_at")
		assert.Equal(t, float64(1), output["id"])
		assert.Equal(t, "test", output["name"])
	})

	t.Run("Flatten Transformer", func(t *testing.T) {
		transformer := transformers.NewFlattenTransformer(&v1.FlattenTransformation{
			Field: "items",
		})

		testData := map[string]interface{}{
			"id":    1,
			"items": []interface{}{map[string]interface{}{"name": "item1"}, map[string]interface{}{"name": "item2"}},
		}
		jsonData, err := json.Marshal(testData)
		require.NoError(t, err)

		message := types.NewMessage(jsonData)
		result, err := transformer.Transform(ctx, message)
		require.NoError(t, err)
		require.Len(t, result, 2)

		// Проверяем первое сообщение
		var output1 map[string]interface{}
		err = json.Unmarshal(result[0].Data, &output1)
		require.NoError(t, err)
		assert.Equal(t, "item1", output1["name"])

		// Проверяем второе сообщение
		var output2 map[string]interface{}
		err = json.Unmarshal(result[1].Data, &output2)
		require.NoError(t, err)
		assert.Equal(t, "item2", output2["name"])
	})

	t.Run("Filter Transformer", func(t *testing.T) {
		// Filter использует JSONPath, который возвращает значение поля
		// Проверяем, что значение существует и является truthy
		transformer := transformers.NewFilterTransformer(&v1.FilterTransformation{
			Condition: "value", // JSONPath к полю value
		})

		// Тест 1: сообщение проходит фильтр (value существует и не равен 0)
		testData1 := map[string]interface{}{
			"id":    1,
			"value": 20,
		}
		jsonData1, err := json.Marshal(testData1)
		require.NoError(t, err)

		message1 := types.NewMessage(jsonData1)
		result1, err := transformer.Transform(ctx, message1)
		require.NoError(t, err)
		require.Len(t, result1, 1)

		// Тест 2: сообщение не проходит фильтр (value равен 0)
		testData2 := map[string]interface{}{
			"id":    2,
			"value": 0,
		}
		jsonData2, err := json.Marshal(testData2)
		require.NoError(t, err)

		message2 := types.NewMessage(jsonData2)
		result2, err := transformer.Transform(ctx, message2)
		require.NoError(t, err)
		require.Len(t, result2, 0)

		// Тест 3: поле отсутствует - сообщение фильтруется
		testData3 := map[string]interface{}{
			"id": 3,
		}
		jsonData3, err := json.Marshal(testData3)
		require.NoError(t, err)

		message3 := types.NewMessage(jsonData3)
		result3, err := transformer.Transform(ctx, message3)
		require.NoError(t, err)
		require.Len(t, result3, 0)
	})

	t.Run("Mask Transformer", func(t *testing.T) {
		transformer := transformers.NewMaskTransformer(&v1.MaskTransformation{
			Fields:     []string{"password", "email"},
			MaskChar:   "*",
			KeepLength: true,
		})

		testData := map[string]interface{}{
			"id":       1,
			"username": "testuser",
			"password": "secret123",
			"email":    "test@example.com",
		}
		jsonData, err := json.Marshal(testData)
		require.NoError(t, err)

		message := types.NewMessage(jsonData)
		result, err := transformer.Transform(ctx, message)
		require.NoError(t, err)
		require.Len(t, result, 1)

		var output map[string]interface{}
		err = json.Unmarshal(result[0].Data, &output)
		require.NoError(t, err)

		assert.Equal(t, "testuser", output["username"])
		assert.NotEqual(t, "secret123", output["password"])
		assert.NotEqual(t, "test@example.com", output["email"])
		assert.Contains(t, output["password"].(string), "*")
		assert.Contains(t, output["email"].(string), "*")
	})

	t.Run("Select Transformer", func(t *testing.T) {
		transformer := transformers.NewSelectTransformer(&v1.SelectTransformation{
			Fields: []string{"id", "name"},
		})

		testData := map[string]interface{}{
			"id":       1,
			"name":     "test",
			"password": "secret",
			"email":    "test@example.com",
		}
		jsonData, err := json.Marshal(testData)
		require.NoError(t, err)

		message := types.NewMessage(jsonData)
		result, err := transformer.Transform(ctx, message)
		require.NoError(t, err)
		require.Len(t, result, 1)

		var output map[string]interface{}
		err = json.Unmarshal(result[0].Data, &output)
		require.NoError(t, err)

		assert.Contains(t, output, "id")
		assert.Contains(t, output, "name")
		assert.NotContains(t, output, "password")
		assert.NotContains(t, output, "email")
	})

	t.Run("Remove Transformer", func(t *testing.T) {
		transformer := transformers.NewRemoveTransformer(&v1.RemoveTransformation{
			Fields: []string{"password", "email"},
		})

		testData := map[string]interface{}{
			"id":       1,
			"name":     "test",
			"password": "secret",
			"email":    "test@example.com",
		}
		jsonData, err := json.Marshal(testData)
		require.NoError(t, err)

		message := types.NewMessage(jsonData)
		result, err := transformer.Transform(ctx, message)
		require.NoError(t, err)
		require.Len(t, result, 1)

		var output map[string]interface{}
		err = json.Unmarshal(result[0].Data, &output)
		require.NoError(t, err)

		assert.Contains(t, output, "id")
		assert.Contains(t, output, "name")
		assert.NotContains(t, output, "password")
		assert.NotContains(t, output, "email")
	})

	t.Run("Router Transformer", func(t *testing.T) {
		// Router трансформер использует условия с == для сравнения
		transformer := transformers.NewRouterTransformer(&v1.RouterTransformation{
			Routes: []v1.RouteRule{
				{
					Condition: "type == 'user'",
					Sink: v1.SinkSpec{
						Type: "kafka",
						Kafka: &v1.KafkaSinkSpec{
							Brokers: []string{"localhost:9092"},
							Topic:   "user-topic",
						},
					},
				},
				{
					Condition: "type == 'order'",
					Sink: v1.SinkSpec{
						Type: "kafka",
						Kafka: &v1.KafkaSinkSpec{
							Brokers: []string{"localhost:9092"},
							Topic:   "order-topic",
						},
					},
				},
			},
		})

		// Тест 1: сообщение типа user
		testData1 := map[string]interface{}{
			"id":   1,
			"type": "user",
			"name": "test user",
		}
		jsonData1, err := json.Marshal(testData1)
		require.NoError(t, err)

		message1 := types.NewMessage(jsonData1)
		result1, err := transformer.Transform(ctx, message1)
		require.NoError(t, err)
		require.Len(t, result1, 1)

		// Проверяем, что метаданные содержат информацию о роутинге
		assert.NotNil(t, result1[0].Metadata)
		assert.Contains(t, result1[0].Metadata, "routed_condition")
		assert.Equal(t, "type == 'user'", result1[0].Metadata["routed_condition"])

		// Тест 2: сообщение типа order
		testData2 := map[string]interface{}{
			"id":     2,
			"type":   "order",
			"amount": 100,
		}
		jsonData2, err := json.Marshal(testData2)
		require.NoError(t, err)

		message2 := types.NewMessage(jsonData2)
		result2, err := transformer.Transform(ctx, message2)
		require.NoError(t, err)
		require.Len(t, result2, 1)

		assert.NotNil(t, result2[0].Metadata)
		assert.Contains(t, result2[0].Metadata, "routed_condition")
		assert.Equal(t, "type == 'order'", result2[0].Metadata["routed_condition"])

		// Тест 3: сообщение без совпадения условий - возвращается как есть
		testData3 := map[string]interface{}{
			"id":   3,
			"type": "unknown",
		}
		jsonData3, err := json.Marshal(testData3)
		require.NoError(t, err)

		message3 := types.NewMessage(jsonData3)
		result3, err := transformer.Transform(ctx, message3)
		require.NoError(t, err)
		require.Len(t, result3, 1)
		// Не должно быть routed_condition в метаданных
		assert.NotContains(t, result3[0].Metadata, "routed_condition")
	})
}

// TestTransformersChainIntegration тестирует цепочку трансформеров
func TestTransformersChainIntegration(t *testing.T) {
	ctx := context.Background()

	// Создаем цепочку трансформеров: Select -> Mask -> Timestamp
	selectTransformer := transformers.NewSelectTransformer(&v1.SelectTransformation{
		Fields: []string{"id", "name", "email", "password"},
	})

	maskTransformer := transformers.NewMaskTransformer(&v1.MaskTransformation{
		Fields:     []string{"password"},
		MaskChar:   "*",
		KeepLength: true,
	})

	timestampTransformer := transformers.NewTimestampTransformer(&v1.TimestampTransformation{
		FieldName: "created_at",
	})

	testData := map[string]interface{}{
		"id":       1,
		"name":     "test user",
		"email":    "test@example.com",
		"password": "secret123",
		"extra":    "should be removed",
	}
	jsonData, err := json.Marshal(testData)
	require.NoError(t, err)

	message := types.NewMessage(jsonData)

	// Применяем трансформеры по цепочке
	result, err := selectTransformer.Transform(ctx, message)
	require.NoError(t, err)
	require.Len(t, result, 1)

	result, err = maskTransformer.Transform(ctx, result[0])
	require.NoError(t, err)
	require.Len(t, result, 1)

	result, err = timestampTransformer.Transform(ctx, result[0])
	require.NoError(t, err)
	require.Len(t, result, 1)

	// Проверяем результат
	var output map[string]interface{}
	err = json.Unmarshal(result[0].Data, &output)
	require.NoError(t, err)

	// Должны быть только выбранные поля
	assert.Contains(t, output, "id")
	assert.Contains(t, output, "name")
	assert.Contains(t, output, "email")
	assert.Contains(t, output, "password")
	assert.Contains(t, output, "created_at")
	assert.NotContains(t, output, "extra")

	// Пароль должен быть замаскирован
	assert.NotEqual(t, "secret123", output["password"])
	assert.Contains(t, output["password"].(string), "*")
}
