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
)

// DebeziumUnwrapTransformer unwraps Debezium envelope into row payload.
type DebeziumUnwrapTransformer struct {
	config *v1.DebeziumUnwrapTransformation
}

// NewDebeziumUnwrapTransformer creates a new Debezium unwrap transformer.
func NewDebeziumUnwrapTransformer(config *v1.DebeziumUnwrapTransformation) *DebeziumUnwrapTransformer {
	return &DebeziumUnwrapTransformer{config: config}
}

func (t *DebeziumUnwrapTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	_ = ctx
	if len(strings.TrimSpace(string(message.Data))) == 0 {
		return t.transformTombstone(message)
	}

	root, ok := message.JSONObject()
	if !ok {
		return nil, fmt.Errorf("debezium unwrap: invalid JSON payload: not a JSON object")
	}

	payload, ok := root["payload"].(map[string]interface{})
	if !ok {
		// Mixed topic support: pass-through when this is not a Debezium envelope.
		return []*types.Message{message}, nil
	}
	op, _ := payload["op"].(string)
	if op == "" {
		return []*types.Message{message}, nil
	}

	record, operation, err := t.extractRecord(payload, op)
	if err != nil {
		return nil, err
	}
	record = t.enrichRecord(record, op, operation, payload)

	out, err := newMessageFromJSON(message, record)
	if err != nil {
		return nil, fmt.Errorf("debezium unwrap: marshal record: %w", err)
	}
	out.Ack = message.Ack
	out.Metadata = cloneMetadata(message.Metadata)
	out.Metadata["operation"] = operation
	if id, ok := record["id"]; ok {
		out.Metadata["id"] = id
	}
	if t.config != nil && t.config.IncludeSourceInMetadata {
		if source, ok := payload["source"].(map[string]interface{}); ok {
			for k, v := range source {
				out.Metadata["source_"+k] = v
			}
		}
	}
	return []*types.Message{out}, nil
}

func (t *DebeziumUnwrapTransformer) extractRecord(payload map[string]interface{}, op string) (map[string]interface{}, string, error) {
	switch op {
	case "c":
		record, err := payloadRecord(payload, "after")
		if err != nil {
			return nil, "", err
		}
		return record, "insert", nil
	case "u":
		record, err := payloadRecord(payload, "after")
		if err != nil {
			return nil, "", err
		}
		return record, "update", nil
	case "r":
		record, err := payloadRecord(payload, "after")
		if err != nil {
			return nil, "", err
		}
		if t != nil && t.config != nil && t.config.SnapshotOperation == "update" {
			return record, "update", nil
		}
		return record, "insert", nil
	case "d":
		record, err := payloadRecord(payload, "before")
		if err != nil {
			return nil, "", err
		}
		return record, "delete", nil
	default:
		return nil, "", fmt.Errorf("debezium unwrap: unsupported op %q", op)
	}
}

func payloadRecord(payload map[string]interface{}, key string) (map[string]interface{}, error) {
	record, ok := payload[key].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("debezium unwrap: payload.%s must be object", key)
	}
	return record, nil
}

// enrichRecord optionally copies NRSE op markers and selected source fields into the row.
// Defaults leave the record unchanged (backwards compatible).
func (t *DebeziumUnwrapTransformer) enrichRecord(
	record map[string]interface{},
	debeziumOp string,
	operation string,
	payload map[string]interface{},
) map[string]interface{} {
	if t == nil || t.config == nil {
		return record
	}
	needClone := t.config.AddOperationFields || len(t.config.AddSourceFields) > 0
	if !needClone {
		return record
	}

	out := cloneRecord(record)
	if t.config.AddOperationFields {
		out["__op"] = debeziumOp
		if operation == "delete" {
			out["__deleted"] = "true"
		} else {
			out["__deleted"] = "false"
		}
	}
	if len(t.config.AddSourceFields) > 0 && payload != nil {
		if source, ok := payload["source"].(map[string]interface{}); ok {
			for _, k := range t.config.AddSourceFields {
				k = strings.TrimSpace(k)
				if k == "" {
					continue
				}
				if v, ok := source[k]; ok {
					out["source_"+k] = v
				}
			}
		}
	}
	return out
}

func (t *DebeziumUnwrapTransformer) transformTombstone(message *types.Message) ([]*types.Message, error) {
	if t == nil || t.config == nil || !t.config.InferDeleteFromTombstone {
		return []*types.Message{}, nil
	}
	keyRaw, ok := message.Metadata["key"].(string)
	if !ok || strings.TrimSpace(keyRaw) == "" {
		return []*types.Message{}, nil
	}

	var key map[string]interface{}
	if err := json.Unmarshal([]byte(keyRaw), &key); err != nil {
		return []*types.Message{}, nil
	}
	record := key
	if payload, ok := key["payload"].(map[string]interface{}); ok {
		record = payload
	}
	// Tombstones have no envelope op; inferred deletes use Debezium "d".
	record = t.enrichRecord(record, "d", "delete", nil)

	data, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("debezium unwrap: marshal tombstone key: %w", err)
	}

	out := types.NewMessage(data)
	out.Timestamp = message.Timestamp
	out.Ack = message.Ack
	out.Metadata = cloneMetadata(message.Metadata)
	out.Metadata["operation"] = "delete"
	if id, ok := record["id"]; ok {
		out.Metadata["id"] = id
	}
	return []*types.Message{out}, nil
}

func cloneMetadata(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return make(map[string]interface{})
	}
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func cloneRecord(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return make(map[string]interface{})
	}
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
