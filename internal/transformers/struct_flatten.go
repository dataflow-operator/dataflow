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
	"sort"
	"strings"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
)

// structFlattenMaxDepth limits nesting to avoid stack overflow on pathological input.
const structFlattenMaxDepth = 64

// StructFlattenTransformer flattens nested JSON objects into a single-level map (1→1).
type StructFlattenTransformer struct {
	config *v1.StructFlattenTransformation
}

// NewStructFlattenTransformer creates a new structFlatten transformer.
func NewStructFlattenTransformer(config *v1.StructFlattenTransformation) *StructFlattenTransformer {
	return &StructFlattenTransformer{config: config}
}

// Transform flattens nested objects using the configured delimiter. Non-object / non-JSON payloads pass through.
func (s *StructFlattenTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	root, ok := tryUnmarshalJSON(message)
	if !ok {
		return []*types.Message{message}, nil
	}

	flat, err := flattenStruct(root, s.delimiter())
	if err != nil {
		return nil, err
	}

	out, err := newMessageFromJSON(message, flat)
	if err != nil {
		return nil, fmt.Errorf("structFlatten marshal: %w", err)
	}

	return []*types.Message{out}, nil
}

func (s *StructFlattenTransformer) delimiter() string {
	d := strings.TrimSpace(s.config.Delimiter)
	if d == "" {
		return "."
	}
	return d
}

type structFlattenFrame struct {
	obj    map[string]interface{}
	prefix string
	depth  int
}

func flattenStruct(root map[string]interface{}, delimiter string) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	stack := []structFlattenFrame{{obj: root, prefix: "", depth: 0}}

	for len(stack) > 0 {
		frame := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if frame.depth > structFlattenMaxDepth {
			return nil, fmt.Errorf("structFlatten: nesting depth exceeds %d", structFlattenMaxDepth)
		}

		keys := make([]string, 0, len(frame.obj))
		for k := range frame.obj {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		// Leaves first, then nested objects (lex order within each group) so nested paths overwrite flat collisions.
		var objectKeys []string
		for _, key := range keys {
			value := frame.obj[key]
			nested, isObject := value.(map[string]interface{})
			if isObject {
				if len(nested) == 0 {
					continue
				}
				objectKeys = append(objectKeys, key)
				continue
			}
			result[joinStructFlattenKey(frame.prefix, key, delimiter)] = value
		}

		for i := len(objectKeys) - 1; i >= 0; i-- {
			key := objectKeys[i]
			nested := frame.obj[key].(map[string]interface{})
			stack = append(stack, structFlattenFrame{
				obj:    nested,
				prefix: joinStructFlattenKey(frame.prefix, key, delimiter),
				depth:  frame.depth + 1,
			})
		}
	}

	return result, nil
}

func joinStructFlattenKey(prefix, key, delimiter string) string {
	if prefix == "" {
		return key
	}
	return prefix + delimiter + key
}
