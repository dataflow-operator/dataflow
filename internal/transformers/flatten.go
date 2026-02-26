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
	"fmt"
	"strings"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
	"github.com/tidwall/gjson"
)

// FlattenTransformer flattens an array field into multiple messages
type FlattenTransformer struct {
	config *v1.FlattenTransformation
	logger logr.Logger
}

// NewFlattenTransformer creates a new flatten transformer
func NewFlattenTransformer(config *v1.FlattenTransformation) *FlattenTransformer {
	return &FlattenTransformer{
		config: config,
		logger: logr.Discard(),
	}
}

// SetLogger sets the logger for the transformer
func (f *FlattenTransformer) SetLogger(logger logr.Logger) {
	f.logger = logger
}

// normalizeFieldPath normalizes JSONPath field (removes $. prefix if present)
func (f *FlattenTransformer) normalizeFieldPath(field string) string {
	// Remove $. prefix if present (gjson doesn't need it for root fields)
	if strings.HasPrefix(field, "$.") {
		return field[2:]
	} else if strings.HasPrefix(field, "$") {
		return field[1:]
	}
	return field
}

// Transform flattens the array field into multiple messages
func (f *FlattenTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	// Normalize field path
	fieldPath := f.normalizeFieldPath(f.config.Field)

	// Get the array field using JSONPath
	result := gjson.GetBytes(message.Data, fieldPath)

	f.logger.Info("Flatten transformer processing",
		"field", f.config.Field,
		"normalizedField", fieldPath,
		"exists", result.Exists(),
		"isArray", result.IsArray(),
		"messagePreview", string(message.Data)[:min(200, len(message.Data))])

	if !result.Exists() {
		// Field doesn't exist, return original message
		f.logger.Info("Field does not exist, returning original message",
			"field", fieldPath)
		return []*types.Message{message}, nil
	}

	// Check if field is an array directly
	var arrayResult gjson.Result = result
	if !result.IsArray() {
		// Check if it's an object with "array" field (hamba/avro wraps arrays this way)
		if result.IsObject() {
			arrayField := result.Get("array")
			if arrayField.Exists() && arrayField.IsArray() {
				f.logger.Info("Found array wrapped in object with 'array' field (hamba/avro format)",
					"field", fieldPath)
				arrayResult = arrayField
			} else {
				// Field is not an array and not a wrapped array, return original message
				f.logger.Info("Field is not an array, returning original message",
					"field", fieldPath,
					"type", fmt.Sprintf("%T", result.Value()))
				return []*types.Message{message}, nil
			}
		} else {
			// Field is not an array, return original message
			f.logger.Info("Field is not an array, returning original message",
				"field", fieldPath,
				"type", fmt.Sprintf("%T", result.Value()))
			return []*types.Message{message}, nil
		}
	}

	// Parse the original message
	var originalData map[string]interface{}
	if err := json.Unmarshal(message.Data, &originalData); err != nil {
		// If data is not valid JSON, return original message unchanged
		// This allows handling binary data or other formats
		return []*types.Message{message}, nil
	}

	// Get the array
	array := arrayResult.Array()
	if len(array) == 0 {
		// Empty array, return original message without the array field
		f.logger.Info("Empty array found, removing field and returning original message",
			"field", fieldPath)
		// Try to delete both normalized and original field names
		delete(originalData, fieldPath)
		delete(originalData, f.config.Field)
		newData, _ := json.Marshal(originalData)
		newMsg := types.NewMessage(newData)
		newMsg.Metadata = message.Metadata
		newMsg.Timestamp = message.Timestamp
		return []*types.Message{newMsg}, nil
	}

	f.logger.Info("Flattening array",
		"field", fieldPath,
		"arrayLength", len(array))

	// Create a message for each element in the array
	messages := make([]*types.Message, 0, len(array))
	for _, item := range array {
		// Create a copy of the original data
		newData := make(map[string]interface{})
		for k, v := range originalData {
			// Skip both normalized and original field names
			if k != fieldPath && k != f.config.Field {
				newData[k] = v
			}
		}

		// Add the flattened item fields to the root
		if item.IsObject() {
			itemMap := item.Map()
			for k, v := range itemMap {
				newData[k] = v.Value()
			}
		} else {
			// If it's a primitive, add it with the field name
			newData[f.config.Field] = item.Value()
		}

		jsonData, err := json.Marshal(newData)
		if err != nil {
			continue
		}

		newMsg := types.NewMessage(jsonData)
		newMsg.Metadata = message.Metadata
		newMsg.Timestamp = message.Timestamp
		messages = append(messages, newMsg)
	}

	f.logger.Info("Flatten completed",
		"field", fieldPath,
		"inputMessages", 1,
		"outputMessages", len(messages))

	// Log first flattened message structure for debugging
	if len(messages) > 0 {
		var firstFlattened map[string]interface{}
		if err := json.Unmarshal(messages[0].Data, &firstFlattened); err == nil {
			firstKeys := make([]string, 0, len(firstFlattened))
			for k := range firstFlattened {
				firstKeys = append(firstKeys, k)
			}
			f.logger.Info("First flattened message structure",
				"keys", firstKeys,
				"messagePreview", string(messages[0].Data)[:min(300, len(messages[0].Data))])
		}
	}

	return messages, nil
}
