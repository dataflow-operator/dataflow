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
	"strings"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/tidwall/sjson"
)

// HeadersToPayloadTransformer copies message headers from Metadata into the JSON payload.
type HeadersToPayloadTransformer struct {
	config *v1.HeadersToPayloadTransformation
}

// NewHeadersToPayloadTransformer creates a new headersToPayload transformer.
func NewHeadersToPayloadTransformer(config *v1.HeadersToPayloadTransformation) *HeadersToPayloadTransformer {
	return &HeadersToPayloadTransformer{config: config}
}

// Transform copies mapped headers from Metadata["headers"] into JSON fields.
// Missing headers are skipped. Invalid headers metadata type leaves the message unchanged.
func (h *HeadersToPayloadTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	if !isJSONObjectPayload(message.Data) {
		return []*types.Message{message}, nil
	}

	headers, ok := headersFromMetadata(message.Metadata)
	if !ok {
		return []*types.Message{message}, nil
	}

	jsonStr := string(message.Data)
	for _, mapping := range h.config.Mappings {
		headerName, fieldPath, ok := strings.Cut(mapping, ":")
		if !ok || strings.TrimSpace(headerName) == "" || strings.TrimSpace(fieldPath) == "" {
			return nil, fmt.Errorf("invalid header mapping format: %q (expected headerName:fieldPath)", mapping)
		}
		headerName = strings.TrimSpace(headerName)
		fieldPath = normalizeFieldPath(strings.TrimSpace(fieldPath))

		value, found := headers[headerName]
		if !found {
			continue
		}

		var err error
		jsonStr, err = sjson.Set(jsonStr, fieldPath, value)
		if err != nil {
			return nil, fmt.Errorf("headersToPayload set %q: %w", fieldPath, err)
		}
	}

	return []*types.Message{newMessageFrom(message, []byte(jsonStr))}, nil
}

// headersFromMetadata extracts a string header map from Metadata["headers"].
// Returns ok=false when the key is missing or has an unsupported type.
func headersFromMetadata(meta map[string]interface{}) (map[string]string, bool) {
	if meta == nil {
		return nil, false
	}
	raw, exists := meta["headers"]
	if !exists || raw == nil {
		return nil, false
	}

	switch h := raw.(type) {
	case map[string]string:
		return h, true
	case map[string]interface{}:
		out := make(map[string]string, len(h))
		for k, v := range h {
			switch val := v.(type) {
			case string:
				out[k] = val
			case []byte:
				out[k] = string(val)
			default:
				out[k] = fmt.Sprint(val)
			}
		}
		return out, true
	default:
		return nil, false
	}
}
