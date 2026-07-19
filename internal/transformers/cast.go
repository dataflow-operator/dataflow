/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    10|Unless required by applicable law or agreed to in writing, software
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
	"math"
	"strconv"
	"strings"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// CastTransformer converts field values to declared target types.
type CastTransformer struct {
	config *v1.CastTransformation
}

// NewCastTransformer creates a new cast transformer.
func NewCastTransformer(config *v1.CastTransformation) *CastTransformer {
	return &CastTransformer{config: config}
}

// Transform casts fields listed in config.Spec.
// Missing paths are skipped. Failed conversion of an existing value returns an error.
// Non-JSON payloads are passed through unchanged. Metadata is preserved.
func (c *CastTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	if !json.Valid(message.Data) {
		return []*types.Message{message}, nil
	}

	jsonStr := string(message.Data)
	for path, targetType := range c.config.Spec {
		normalized := normalizeFieldPath(strings.TrimSpace(path))
		if normalized == "" {
			continue
		}

		result := gjson.Get(jsonStr, normalized)
		if !result.Exists() {
			continue
		}

		converted, err := castJSONValue(result, targetType)
		if err != nil {
			return nil, fmt.Errorf("cast %q to %s: %w", path, targetType, err)
		}

		jsonStr, err = sjson.Set(jsonStr, normalized, converted)
		if err != nil {
			return nil, fmt.Errorf("cast set %q: %w", path, err)
		}
	}

	return []*types.Message{newMessageFrom(message, []byte(jsonStr))}, nil
}

func castJSONValue(result gjson.Result, targetType string) (interface{}, error) {
	switch targetType {
	case "null":
		return nil, nil
	case "string":
		return castToString(result)
	case "int64":
		return castToInt64(result)
	case "float64":
		return castToFloat64(result)
	case "bool":
		return castToBool(result)
	default:
		return nil, fmt.Errorf("unsupported type %q", targetType)
	}
}

func castToString(result gjson.Result) (string, error) {
	if result.IsObject() || result.IsArray() {
		return "", fmt.Errorf("cannot cast object/array to string")
	}
	if result.Type == gjson.Null {
		return "", fmt.Errorf("cannot cast null to string")
	}
	return result.String(), nil
}

func castToInt64(result gjson.Result) (int64, error) {
	switch result.Type {
	case gjson.Number:
		return numberToInt64(result)
	case gjson.String:
		s := strings.TrimSpace(result.String())
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i, nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot cast %q to int64", result.String())
		}
		return floatToInt64(f)
	default:
		return 0, fmt.Errorf("cannot cast %s to int64", result.Type.String())
	}
}

func castToFloat64(result gjson.Result) (float64, error) {
	switch result.Type {
	case gjson.Number:
		return result.Float(), nil
	case gjson.String:
		f, err := strconv.ParseFloat(strings.TrimSpace(result.String()), 64)
		if err != nil {
			return 0, fmt.Errorf("cannot cast %q to float64", result.String())
		}
		return f, nil
	default:
		return 0, fmt.Errorf("cannot cast %s to float64", result.Type.String())
	}
}

func castToBool(result gjson.Result) (bool, error) {
	switch result.Type {
	case gjson.True, gjson.False:
		return result.Bool(), nil
	case gjson.String:
		switch strings.ToLower(strings.TrimSpace(result.String())) {
		case "true":
			return true, nil
		case "false":
			return false, nil
		default:
			return false, fmt.Errorf("cannot cast %q to bool", result.String())
		}
	case gjson.Number:
		f := result.Float()
		switch f {
		case 1:
			return true, nil
		case 0:
			return false, nil
		default:
			return false, fmt.Errorf("cannot cast number %v to bool (expected 0 or 1)", f)
		}
	default:
		return false, fmt.Errorf("cannot cast %s to bool", result.Type.String())
	}
}

func numberToInt64(result gjson.Result) (int64, error) {
	if i, err := strconv.ParseInt(result.Raw, 10, 64); err == nil {
		return i, nil
	}
	return floatToInt64(result.Float())
}

func floatToInt64(f float64) (int64, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, fmt.Errorf("cannot cast %v to int64", f)
	}
	if f != math.Trunc(f) {
		return 0, fmt.Errorf("cannot cast %v to int64: not an integer", f)
	}
	if f > float64(math.MaxInt64) || f < float64(math.MinInt64) {
		return 0, fmt.Errorf("cannot cast %v to int64: out of range", f)
	}
	return int64(f), nil
}
