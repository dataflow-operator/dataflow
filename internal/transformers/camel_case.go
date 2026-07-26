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

// CamelCaseTransformer converts field names to CamelCase
type CamelCaseTransformer struct {
	config *v1.CamelCaseTransformation
}

// NewCamelCaseTransformer creates a new CamelCase transformer
func NewCamelCaseTransformer(config *v1.CamelCaseTransformation) *CamelCaseTransformer {
	return &CamelCaseTransformer{
		config: config,
	}
}

// Transform converts all field names in the message to CamelCase
func (c *CamelCaseTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	data, err := message.JSONValue()
	if err != nil {
		// If data is not valid JSON, return original message unchanged
		return []*types.Message{message}, nil
	}

	converted := convertKeysRecursive(data, c.config.Deep, toCamelCase)

	out, err := newMessageFromJSON(message, converted)
	if err != nil {
		return nil, err
	}
	return []*types.Message{out}, nil
}

// toCamelCase converts a string to CamelCase
func toCamelCase(s string) string {
	if s == "" {
		return s
	}

	parts := strings.Split(s, "_")
	var result strings.Builder

	for _, part := range parts {
		if part == "" {
			continue
		}
		runes := []rune(part)
		if len(runes) > 0 {
			// All parts start with uppercase (PascalCase)
			result.WriteRune(unicode.ToUpper(runes[0]))
			if len(runes) > 1 {
				result.WriteString(string(runes[1:]))
			}
		}
	}

	// If result is empty, return original string
	if result.Len() == 0 {
		return s
	}

	return result.String()
}
