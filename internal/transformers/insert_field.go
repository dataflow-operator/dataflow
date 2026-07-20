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
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/tidwall/sjson"
)

const (
	insertFieldNowPlaceholder = "${now}"
	insertFieldMetadataPrefix = "${metadata."
	insertFieldMetadataSuffix = "}"
	insertFieldJSONPrefix     = "json:"
)

// InsertFieldTransformer inserts or overwrites JSON fields with literals and placeholders.
type InsertFieldTransformer struct {
	config *v1.InsertFieldTransformation
}

// NewInsertFieldTransformer creates a new insertField transformer.
func NewInsertFieldTransformer(config *v1.InsertFieldTransformation) *InsertFieldTransformer {
	return &InsertFieldTransformer{config: config}
}

// Transform inserts configured fields into the JSON payload.
// Non-JSON payloads are passed through unchanged. Metadata is preserved.
// Missing metadata keys resolve to an empty string. Invalid json: values return an error.
func (t *InsertFieldTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	if _, ok := tryUnmarshalJSON(message); !ok {
		return []*types.Message{message}, nil
	}

	jsonStr := string(message.Data)
	for path, rawValue := range t.config.Fields {
		normalized := normalizeFieldPath(strings.TrimSpace(path))
		if normalized == "" {
			continue
		}

		value, asRaw, err := resolveInsertFieldValue(rawValue, message.Metadata)
		if err != nil {
			return nil, fmt.Errorf("insertField %q: %w", path, err)
		}

		if asRaw {
			jsonStr, err = sjson.SetRaw(jsonStr, normalized, value.(string))
		} else {
			jsonStr, err = sjson.Set(jsonStr, normalized, value)
		}
		if err != nil {
			return nil, fmt.Errorf("insertField set %q: %w", path, err)
		}
	}

	return []*types.Message{newMessageFrom(message, []byte(jsonStr))}, nil
}

// resolveInsertFieldValue resolves a configured field value.
// Returns asRaw=true when the value should be written via sjson.SetRaw (json: prefix).
func resolveInsertFieldValue(raw string, meta map[string]interface{}) (value interface{}, asRaw bool, err error) {
	switch {
	case raw == insertFieldNowPlaceholder:
		return time.Now().Format(time.RFC3339), false, nil
	case strings.HasPrefix(raw, insertFieldMetadataPrefix) && strings.HasSuffix(raw, insertFieldMetadataSuffix):
		key := raw[len(insertFieldMetadataPrefix) : len(raw)-len(insertFieldMetadataSuffix)]
		return formatInsertFieldMetadata(meta, key), false, nil
	case strings.HasPrefix(raw, insertFieldJSONPrefix):
		jsonRaw := raw[len(insertFieldJSONPrefix):]
		if !json.Valid([]byte(jsonRaw)) {
			return nil, false, fmt.Errorf("invalid json value %q", jsonRaw)
		}
		return jsonRaw, true, nil
	default:
		return raw, false, nil
	}
}

func formatInsertFieldMetadata(meta map[string]interface{}, key string) string {
	if meta == nil || key == "" {
		return ""
	}
	v, ok := meta[key]
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(t)
	}
}
