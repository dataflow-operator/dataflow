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

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/tidwall/gjson"
)

// ExtractFieldTransformer replaces the message payload with the value of one field.
type ExtractFieldTransformer struct {
	config *v1.ExtractFieldTransformation
}

// NewExtractFieldTransformer creates a new extractField transformer.
func NewExtractFieldTransformer(config *v1.ExtractFieldTransformation) *ExtractFieldTransformer {
	return &ExtractFieldTransformer{config: config}
}

// Transform sets Data to the JSON value at config.Field (1→1).
// Non-JSON payloads and missing paths are passed through unchanged.
func (e *ExtractFieldTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	if !json.Valid(message.Data) {
		return []*types.Message{message}, nil
	}

	path := normalizeFieldPath(e.config.Field)
	if path == "" {
		return []*types.Message{message}, nil
	}

	result := gjson.GetBytes(message.Data, path)
	if !result.Exists() {
		return []*types.Message{message}, nil
	}

	return []*types.Message{newMessageFrom(message, []byte(result.Raw))}, nil
}
