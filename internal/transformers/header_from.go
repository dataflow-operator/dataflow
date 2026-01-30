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

	"github.com/dataflow-operator/dataflow/internal/types"
)

// TODO: HeaderFromTransformation is not yet defined in API
// HeaderFromTransformer extracts data from Kafka message headers and adds them to the message body
// type HeaderFromTransformer struct {
// 	config *v1.HeaderFromTransformation
// }

// NewHeaderFromTransformer creates a new header from transformer
// func NewHeaderFromTransformer(config *v1.HeaderFromTransformation) *HeaderFromTransformer {
// 	return &HeaderFromTransformer{
// 		config: config,
// 	}
// }

// Temporary stub to allow compilation
type HeaderFromTransformer struct {
	config interface{}
}

func NewHeaderFromTransformer(config interface{}) *HeaderFromTransformer {
	return &HeaderFromTransformer{
		config: config,
	}
}

// Transform is a stub - HeaderFromTransformation is not yet implemented in API
func (h *HeaderFromTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	return nil, fmt.Errorf("headerFrom transformation is not yet implemented in API")
}
