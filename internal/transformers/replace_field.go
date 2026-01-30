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

// TODO: ReplaceFieldTransformation is not yet defined in API
// ReplaceFieldTransformer renames fields in messages
// type ReplaceFieldTransformer struct {
// 	config *v1.ReplaceFieldTransformation
// }

// NewReplaceFieldTransformer creates a new replace field transformer
// func NewReplaceFieldTransformer(config *v1.ReplaceFieldTransformation) *ReplaceFieldTransformer {
// 	return &ReplaceFieldTransformer{
// 		config: config,
// 	}
// }

// Temporary stub to allow compilation
type ReplaceFieldTransformer struct {
	config interface{}
}

func NewReplaceFieldTransformer(config interface{}) *ReplaceFieldTransformer {
	return &ReplaceFieldTransformer{
		config: config,
	}
}

// Transform is a stub - ReplaceFieldTransformation is not yet implemented in API
func (r *ReplaceFieldTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	return nil, fmt.Errorf("replaceField transformation is not yet implemented in API")
}
