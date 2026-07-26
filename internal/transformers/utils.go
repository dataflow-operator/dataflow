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
	"encoding/json"
	"strings"

	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// normalizeFieldPath removes $. or $ prefix from JSONPath fields.
// Libraries like gjson/sjson don't need the prefix for root fields.
func normalizeFieldPath(field string) string {
	if strings.HasPrefix(field, "$.") {
		return field[2:]
	} else if strings.HasPrefix(field, "$") {
		return field[1:]
	}
	return field
}

// isTruthy evaluates the truthiness of a value (from gjson).
func isTruthy(value interface{}) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return v != "" && v != "false"
	case float64:
		return v != 0
	case nil:
		return false
	default:
		return true
	}
}

// convertKeysRecursive recursively converts map keys using keyFn.
// When deep is true, nested maps and arrays are processed recursively.
func convertKeysRecursive(data interface{}, deep bool, keyFn func(string) string) interface{} {
	switch v := data.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{})
		for key, value := range v {
			newKey := keyFn(key)
			if deep {
				result[newKey] = convertKeysRecursive(value, deep, keyFn)
			} else {
				result[newKey] = value
			}
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			if deep {
				result[i] = convertKeysRecursive(item, deep, keyFn)
			} else {
				result[i] = item
			}
		}
		return result
	default:
		return data
	}
}

// sjsonSetWithFallback sets a value in a JSON string using sjson,
// falling back to manual unmarshal/marshal if sjson fails.
func sjsonSetWithFallback(jsonStr, path string, value interface{}) (string, error) {
	result, err := sjson.Set(jsonStr, path, value)
	if err != nil {
		var data map[string]interface{}
		if uerr := json.Unmarshal([]byte(jsonStr), &data); uerr != nil {
			return jsonStr, uerr
		}
		data[path] = value
		newData, merr := json.Marshal(data)
		if merr != nil {
			return jsonStr, merr
		}
		return string(newData), nil
	}
	return result, nil
}

// sjsonDeleteWithFallback deletes a key from a JSON string using sjson,
// falling back to manual unmarshal/marshal if sjson fails.
func sjsonDeleteWithFallback(jsonStr, path string) (string, error) {
	result, err := sjson.Delete(jsonStr, path)
	if err != nil {
		var data map[string]interface{}
		if uerr := json.Unmarshal([]byte(jsonStr), &data); uerr != nil {
			return jsonStr, uerr
		}
		delete(data, path)
		newData, merr := json.Marshal(data)
		if merr != nil {
			return jsonStr, merr
		}
		return string(newData), nil
	}
	return result, nil
}

// tryUnmarshalJSON returns Data as a JSON object, using Message's parse-once cache.
// Returns (data, true) on success or (nil, false) if the data is not a JSON object.
func tryUnmarshalJSON(message *types.Message) (map[string]interface{}, bool) {
	return message.JSONObject()
}

// isJSONObjectPayload reports whether data is a JSON object without building a full map.
// Prefer this over tryUnmarshalJSON when only validating before sjson/gjson mutations.
func isJSONObjectPayload(data []byte) bool {
	return gjson.ParseBytes(data).IsObject()
}

// newMessageFrom creates a new message with the given data, copying Metadata and Timestamp from src.
func newMessageFrom(src *types.Message, data []byte) *types.Message {
	msg := types.NewMessage(data)
	msg.Metadata = src.Metadata
	msg.Timestamp = src.Timestamp
	return msg
}

// newMessageFromJSON marshals value once, copies Metadata/Timestamp, and primes the parse cache
// so the next transform stage can reuse the object without Unmarshal.
func newMessageFromJSON(src *types.Message, value interface{}) (*types.Message, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	msg := newMessageFrom(src, data)
	msg.PrimeJSONCache(value)
	return msg, nil
}

// payloadPreview returns a short string view of payload for debug logs.
// Call only after checking logger.V(n).Enabled() — Go evaluates Info args eagerly.
func payloadPreview(data []byte) string {
	const maxPreview = 200
	if len(data) <= maxPreview {
		return string(data)
	}
	return string(data[:maxPreview])
}
