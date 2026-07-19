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
	"fmt"
	"strings"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/tidwall/sjson"
)

// HoistFieldTransformer wraps the entire payload under a single top-level key.
type HoistFieldTransformer struct {
	config *v1.HoistFieldTransformation
}

// NewHoistFieldTransformer creates a new hoistField transformer.
func NewHoistFieldTransformer(config *v1.HoistFieldTransformation) *HoistFieldTransformer {
	return &HoistFieldTransformer{config: config}
}

// Transform wraps Data as {"<field>": <parsed value>} (1→1).
// Non-JSON payloads are passed through unchanged.
func (h *HoistFieldTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	if !json.Valid(message.Data) {
		return []*types.Message{message}, nil
	}

	field := strings.TrimSpace(h.config.Field)
	if field == "" {
		return []*types.Message{message}, nil
	}

	wrapped, err := sjson.SetRaw("{}", field, string(message.Data))
	if err != nil {
		return nil, fmt.Errorf("hoistField wrap %q: %w", field, err)
	}

	return []*types.Message{newMessageFrom(message, []byte(wrapped))}, nil
}
