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
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ReplaceFieldTransformer renames fields and optionally filters via include/exclude.
type ReplaceFieldTransformer struct {
	config *v1.ReplaceFieldTransformation
}

// NewReplaceFieldTransformer creates a new replaceField transformer.
func NewReplaceFieldTransformer(config *v1.ReplaceFieldTransformation) *ReplaceFieldTransformer {
	return &ReplaceFieldTransformer{config: config}
}

// Transform applies include/exclude filtering, then renames fields.
// Include preserves nested structure (unlike select). Include and exclude are mutually exclusive.
func (r *ReplaceFieldTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	if _, ok := tryUnmarshalJSON(message); !ok {
		return []*types.Message{message}, nil
	}

	jsonStr := string(message.Data)

	if len(r.config.Include) > 0 {
		included := "{}"
		for _, field := range r.config.Include {
			path := normalizeFieldPath(field)
			result := gjson.Get(jsonStr, path)
			if !result.Exists() {
				continue
			}
			var err error
			included, err = sjson.Set(included, path, result.Value())
			if err != nil {
				return nil, fmt.Errorf("replaceField include %q: %w", field, err)
			}
		}
		jsonStr = included
	} else if len(r.config.Exclude) > 0 {
		for _, field := range r.config.Exclude {
			path := normalizeFieldPath(field)
			if !gjson.Get(jsonStr, path).Exists() {
				continue
			}
			var err error
			jsonStr, err = sjson.Delete(jsonStr, path)
			if err != nil {
				return nil, fmt.Errorf("replaceField exclude %q: %w", field, err)
			}
			jsonStr = pruneEmptyParents(jsonStr, path)
		}
	}

	for _, rename := range r.config.Renames {
		oldPath, newPath, ok := strings.Cut(rename, ":")
		if !ok || strings.TrimSpace(oldPath) == "" || strings.TrimSpace(newPath) == "" {
			return nil, fmt.Errorf("invalid rename format: %q (expected oldPath:newPath)", rename)
		}
		oldPath = normalizeFieldPath(strings.TrimSpace(oldPath))
		newPath = normalizeFieldPath(strings.TrimSpace(newPath))

		if oldPath == newPath {
			continue
		}

		result := gjson.Get(jsonStr, oldPath)
		if !result.Exists() {
			continue
		}

		var err error
		jsonStr, err = sjson.Set(jsonStr, newPath, result.Value())
		if err != nil {
			return nil, fmt.Errorf("replaceField rename set %q: %w", newPath, err)
		}
		jsonStr, err = sjson.Delete(jsonStr, oldPath)
		if err != nil {
			return nil, fmt.Errorf("replaceField rename delete %q: %w", oldPath, err)
		}
		jsonStr = pruneEmptyParents(jsonStr, oldPath)
	}

	return []*types.Message{newMessageFrom(message, []byte(jsonStr))}, nil
}

// pruneEmptyParents removes empty parent objects left after deleting a nested path.
func pruneEmptyParents(jsonStr, deletedPath string) string {
	parts := strings.Split(deletedPath, ".")
	for i := len(parts) - 1; i >= 1; i-- {
		parentPath := strings.Join(parts[:i], ".")
		parent := gjson.Get(jsonStr, parentPath)
		if !parent.IsObject() || len(parent.Map()) > 0 {
			break
		}
		next, err := sjson.Delete(jsonStr, parentPath)
		if err != nil {
			break
		}
		jsonStr = next
	}
	return jsonStr
}
