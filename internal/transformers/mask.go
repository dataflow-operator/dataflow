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
	"strings"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/tidwall/gjson"
)

// MaskTransformer masks sensitive data fields
type MaskTransformer struct {
	config *v1.MaskTransformation
}

// NewMaskTransformer creates a new mask transformer
func NewMaskTransformer(config *v1.MaskTransformation) *MaskTransformer {
	return &MaskTransformer{
		config: config,
	}
}

// Transform masks the specified fields
func (m *MaskTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	maskChar := m.config.MaskChar
	if maskChar == "" {
		maskChar = "*"
	}

	jsonStr := string(message.Data)

	for _, field := range m.config.Fields {
		result := gjson.GetBytes(message.Data, field)
		if !result.Exists() {
			continue
		}

		var maskedValue string
		if m.config.KeepLength {
			// Keep original length
			valueStr := result.String()
			maskedValue = strings.Repeat(maskChar, len(valueStr))
		} else {
			// Use fixed length mask
			maskedValue = maskChar + maskChar + maskChar
		}

		var err error
		jsonStr, err = sjsonSetWithFallback(jsonStr, field, maskedValue)
		if err != nil {
			return []*types.Message{message}, nil
		}
	}

	return []*types.Message{newMessageFrom(message, []byte(jsonStr))}, nil
}
