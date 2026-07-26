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
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
)

// TimestampTransformer adds a timestamp field to messages
type TimestampTransformer struct {
	config *v1.TimestampTransformation
}

// NewTimestampTransformer creates a new timestamp transformer
func NewTimestampTransformer(config *v1.TimestampTransformation) *TimestampTransformer {
	return &TimestampTransformer{
		config: config,
	}
}

// Transform adds a timestamp field to the message
func (t *TimestampTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	fieldName := t.config.FieldName
	if fieldName == "" {
		fieldName = "created_at"
	}

	format := t.config.Format
	if format == "" {
		format = time.RFC3339
	}

	timestamp := time.Now().Format(format)

	if !isJSONObjectPayload(message.Data) {
		return []*types.Message{message}, nil
	}

	result, err := sjsonSetWithFallback(string(message.Data), fieldName, timestamp)
	if err != nil {
		return nil, err
	}

	return []*types.Message{newMessageFrom(message, []byte(result))}, nil
}
