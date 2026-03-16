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

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/tidwall/gjson"
)

// FilterTransformer filters messages based on conditions
type FilterTransformer struct {
	config *v1.FilterTransformation
}

// NewFilterTransformer creates a new filter transformer
func NewFilterTransformer(config *v1.FilterTransformation) *FilterTransformer {
	return &FilterTransformer{
		config: config,
	}
}

// Transform filters messages based on the condition
func (f *FilterTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	// Evaluate the condition using JSONPath
	result := gjson.GetBytes(message.Data, f.config.Condition)

	// Check if condition evaluates to true
	// For JSONPath, we check if the result exists and is truthy
	if !result.Exists() {
		// Condition doesn't match, filter out the message
		return []*types.Message{}, nil
	}

	if !isTruthy(result.Value()) {
		return []*types.Message{}, nil
	}

	// Condition matches, return the message
	return []*types.Message{message}, nil
}
