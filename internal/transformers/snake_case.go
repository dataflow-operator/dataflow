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
	data, err := message.JSONValue()
	if err != nil {
		// If data is not valid JSON, return original message unchanged
		return []*types.Message{message}, nil
	}

	converted := convertKeysRecursive(data, s.config.Deep, toSnakeCase)

	out, err := newMessageFromJSON(message, converted)
	if err != nil {
		return nil, err
	}
	return []*types.Message{out}, nil
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
		isUpper := unicode.IsUpper(r)
		isLower := unicode.IsLower(r)

		if i > 0 && isUpper {
			// Insert underscore before uppercase when previous was lowercase,
			// or when previous was uppercase and next is lowercase (XMLParser → XML_Parser).
			nextLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
			if prevLower || (prevUpper && nextLower) {
				result.WriteByte('_')
			}
		}

		result.WriteRune(unicode.ToLower(r))
		prevLower = isLower
		prevUpper = isUpper
	}

	return result.String()
}
