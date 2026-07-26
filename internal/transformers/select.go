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

	"github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/tidwall/gjson"
)

// SelectTransformer selects specific fields from messages
type SelectTransformer struct {
	config *v1.SelectTransformation
}

// NewSelectTransformer creates a new select transformer
func NewSelectTransformer(config *v1.SelectTransformation) *SelectTransformer {
	return &SelectTransformer{
		config: config,
	}
}

// Transform selects only the specified fields
func (s *SelectTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	newData := make(map[string]interface{})

	for _, field := range s.config.Fields {
		result := gjson.GetBytes(message.Data, field)
		if result.Exists() {
			// Extract field name (last part of path)
			fieldName := field
			if idx := strings.LastIndex(field, "."); idx >= 0 {
				fieldName = field[idx+1:]
			}
			newData[fieldName] = result.Value()
		}
	}

	if len(newData) == 0 {
		// No fields selected, return empty message or original?
		return []*types.Message{message}, nil
	}

	out, err := newMessageFromJSON(message, newData)
	if err != nil {
		return nil, err
	}

	return []*types.Message{out}, nil
}
