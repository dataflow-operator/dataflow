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
	"unicode"

	"github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
)

// SnakeCaseTransformer converts field names to snake_case
type SnakeCaseTransformer struct {
	config *v1.SnakeCaseTransformation
}

// NewSnakeCaseTransformer creates a new snake_case transformer
func NewSnakeCaseTransformer(config *v1.SnakeCaseTransformation) *SnakeCaseTransformer {
	return &SnakeCaseTransformer{
		config: config,
	}
}

// Transform converts all field names in the message to snake_case
func (s *SnakeCaseTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	var data interface{}
	if err := json.Unmarshal(message.Data, &data); err != nil {
		// If data is not valid JSON, return original message unchanged
		return []*types.Message{message}, nil
	}

	converted := s.convertKeys(data, s.config.Deep)

	jsonData, err := json.Marshal(converted)
	if err != nil {
		return nil, err
	}

	newMsg := types.NewMessage(jsonData)
	newMsg.Metadata = message.Metadata
	newMsg.Timestamp = message.Timestamp
	return []*types.Message{newMsg}, nil
}

// convertKeys recursively converts keys in the data structure
func (s *SnakeCaseTransformer) convertKeys(data interface{}, deep bool) interface{} {
	switch v := data.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{})
		for key, value := range v {
			newKey := toSnakeCase(key)
			if deep {
				result[newKey] = s.convertKeys(value, deep)
			} else {
				result[newKey] = value
			}
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			if deep {
				result[i] = s.convertKeys(item, deep)
			} else {
				result[i] = item
			}
		}
		return result
	default:
		return data
	}
}

// toSnakeCase converts a string to snake_case
func toSnakeCase(s string) string {
	if s == "" {
		return s
	}

	var result strings.Builder
	runes := []rune(s)
	prevLower := false
	prevUpper := false

	for i, r := range runes {
		if unicode.IsUpper(r) {
			// Add underscore if:
			// 1. Previous letter was lowercase, or
			// 2. Previous was uppercase but next is lowercase (end of uppercase sequence)
			nextIsLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
			if i > 0 && (prevLower || (prevUpper && nextIsLower)) {
				result.WriteRune('_')
			}
			result.WriteRune(unicode.ToLower(r))
			prevLower = false
			prevUpper = true
		} else if unicode.IsLower(r) || unicode.IsDigit(r) {
			result.WriteRune(r)
			prevLower = true
			prevUpper = false
		} else {
			result.WriteRune(r)
			prevLower = false
			prevUpper = false
		}
	}

	return result.String()
}
