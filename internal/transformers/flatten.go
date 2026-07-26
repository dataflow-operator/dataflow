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
	"fmt"

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

// Transform flattens the array field into multiple messages
func (f *FlattenTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	// Normalize field path
	fieldPath := normalizeFieldPath(f.config.Field)

	// Get the array field using JSONPath
	result := gjson.GetBytes(message.Data, fieldPath)

	if log := f.logger.V(1); log.Enabled() {
		log.Info("Flatten transformer processing",
			"field", f.config.Field,
			"normalizedField", fieldPath,
			"exists", result.Exists(),
			"isArray", result.IsArray(),
			"messagePreview", payloadPreview(message.Data))
	}

	if !result.Exists() {
		// Field doesn't exist, return original message
		f.logger.V(1).Info("Field does not exist, returning original message",
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
				f.logger.V(1).Info("Found array wrapped in object with 'array' field (hamba/avro format)",
					"field", fieldPath)
				arrayResult = arrayField
			} else {
				// Field is not an array and not a wrapped array, return original message
				f.logger.V(1).Info("Field is not an array, returning original message",
					"field", fieldPath,
					"type", fmt.Sprintf("%T", result.Value()))
				return []*types.Message{message}, nil
			}
		} else {
			// Field is not an array, return original message
			f.logger.V(1).Info("Field is not an array, returning original message",
				"field", fieldPath,
				"type", fmt.Sprintf("%T", result.Value()))
			return []*types.Message{message}, nil
		}
	}

	// Parse the original message (parse-once cache)
	originalData, ok := tryUnmarshalJSON(message)
	if !ok {
		// If data is not a JSON object, return original message unchanged
		return []*types.Message{message}, nil
	}

	// Get the array
	array := arrayResult.Array()
	if len(array) == 0 {
		// Empty array, return original message without the array field
		f.logger.V(1).Info("Empty array found, removing field and returning original message",
			"field", fieldPath)
		delete(originalData, fieldPath)
		delete(originalData, f.config.Field)
		out, err := newMessageFromJSON(message, originalData)
		if err != nil {
			return []*types.Message{message}, nil
		}
		return []*types.Message{out}, nil
	}

	f.logger.V(1).Info("Flattening array",
		"field", fieldPath,
		"arrayLength", len(array))

	// Create a message for each element in the array
	messages := make([]*types.Message, 0, len(array))
	for _, item := range array {
		newData := make(map[string]interface{})
		for k, v := range originalData {
			if k != fieldPath && k != f.config.Field {
				newData[k] = v
			}
		}

		if item.IsObject() {
			itemMap := item.Map()
			for k, v := range itemMap {
				newData[k] = v.Value()
			}
		} else {
			newData[f.config.Field] = item.Value()
		}

		out, err := newMessageFromJSON(message, newData)
		if err != nil {
			continue
		}
		messages = append(messages, out)
	}

	f.logger.V(1).Info("Flatten completed",
		"field", fieldPath,
		"inputMessages", 1,
		"outputMessages", len(messages))

	if len(messages) > 0 {
		if log := f.logger.V(1); log.Enabled() {
			if obj, ok := messages[0].JSONObject(); ok {
				firstKeys := make([]string, 0, len(obj))
				for k := range obj {
					firstKeys = append(firstKeys, k)
				}
				log.Info("First flattened message structure",
					"keys", firstKeys,
					"messagePreview", payloadPreview(messages[0].Data))
			}
		}
	}

	return messages, nil
}
