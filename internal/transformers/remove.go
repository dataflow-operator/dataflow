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
	"strings"

	"github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// RemoveTransformer removes specific fields from messages
type RemoveTransformer struct {
	config *v1.RemoveTransformation
	logger logr.Logger
}

// NewRemoveTransformer creates a new remove transformer
func NewRemoveTransformer(config *v1.RemoveTransformation) *RemoveTransformer {
	return &RemoveTransformer{
		config: config,
		logger: logr.Discard(),
	}
}

// SetLogger sets the logger for the transformer
func (r *RemoveTransformer) SetLogger(logger logr.Logger) {
	r.logger = logger
}

// normalizeFieldPath normalizes JSONPath field (removes $. prefix if present)
func (r *RemoveTransformer) normalizeFieldPath(field string) string {
	// Remove $. prefix if present (sjson supports both formats)
	if strings.HasPrefix(field, "$.") {
		return field[2:]
	} else if strings.HasPrefix(field, "$") {
		return field[1:]
	}
	return field
}

// Transform removes the specified fields
func (r *RemoveTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	jsonStr := string(message.Data)

	r.logger.V(1).Info("Remove transformer processing",
		"fields", r.config.Fields,
		"messagePreview", string(message.Data)[:min(200, len(message.Data))])

	for _, field := range r.config.Fields {
		normalizedField := r.normalizeFieldPath(field)

		// Check if field exists before trying to delete
		result := gjson.GetBytes(message.Data, normalizedField)
		if !result.Exists() {
			r.logger.V(1).Info("Field does not exist, skipping removal",
				"field", field,
				"normalizedField", normalizedField)
			continue
		}

		var err error
		// Try with normalized field first
		jsonStr, err = sjson.Delete(jsonStr, normalizedField)
		if err != nil {
			// Try with original field name
			jsonStr, err = sjson.Delete(jsonStr, field)
			if err != nil {
				// Fallback to manual approach
				var data map[string]interface{}
				if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
					// If data is not valid JSON, return original message unchanged
					// This allows handling binary data or other formats
					r.logger.V(1).Info("Invalid JSON, returning original message",
						"field", field)
					return []*types.Message{message}, nil
				}
				// Simple field removal (doesn't support nested paths)
				delete(data, normalizedField)
				delete(data, field)
				newData, _ := json.Marshal(data)
				jsonStr = string(newData)
			}
		}

		r.logger.V(1).Info("Field removed",
			"field", field,
			"normalizedField", normalizedField)
	}

	newMsg := types.NewMessage([]byte(jsonStr))
	newMsg.Metadata = message.Metadata
	newMsg.Timestamp = message.Timestamp

	r.logger.V(1).Info("Remove transformer completed",
		"fieldsRemoved", len(r.config.Fields))

	return []*types.Message{newMsg}, nil
}
