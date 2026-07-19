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

// Transform filters messages based on the condition.
// Condition syntax matches router: truthiness ("$.active"), equality ("$.status == 'active'"),
// or inequality ("$.status != 'deleted'"). Non-matching messages are dropped.
func (f *FilterTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	if !evaluateCondition(message.Data, f.config.Condition) {
		return []*types.Message{}, nil
	}
	return []*types.Message{message}, nil
}
