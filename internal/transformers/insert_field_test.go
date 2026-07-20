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
	"testing"
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsertFieldTransformer_Transform(t *testing.T) {
	fixedTS := time.Date(2024, 1, 15, 8, 0, 7, 486000000, time.UTC)

	tests := []struct {
		name     string
		config   *v1.InsertFieldTransformation
		input    string
		metadata map[string]interface{}
		want     map[string]interface{}
		checkNow string // path that should be a valid RFC3339 now
	}{
		{
			name: "literal and nested json",
			config: &v1.InsertFieldTransformation{Fields: map[string]string{
				"pipeline":           "orders-cdc",
				"flags.reprocessed":  "json:false",
			}},
			input: `{"id":1,"Name":"A"}`,
			want: map[string]interface{}{
				"id":       float64(1),
				"Name":     "A",
				"pipeline": "orders-cdc",
				"flags":    map[string]interface{}{"reprocessed": false},
			},
		},
		{
			name: "metadata placeholders",
			config: &v1.InsertFieldTransformation{Fields: map[string]string{
				"source_topic":     "${metadata.topic}",
				"source_partition": "${metadata.partition}",
				"source_offset":    "${metadata.offset}",
				"source_timestamp": "${metadata.timestamp}",
			}},
			input: `{"id":1}`,
			metadata: map[string]interface{}{
				"topic":     "raw.events",
				"partition": int32(2),
				"offset":    int64(100),
				"timestamp": fixedTS,
			},
			want: map[string]interface{}{
				"id":               float64(1),
				"source_topic":     "raw.events",
				"source_partition": "2",
				"source_offset":    "100",
				"source_timestamp": "2024-01-15T08:00:07.486Z",
			},
		},
		{
			name: "missing metadata yields empty string",
			config: &v1.InsertFieldTransformation{Fields: map[string]string{
				"source_topic": "${metadata.topic}",
				"missing":      "${metadata.nope}",
			}},
			input:    `{"id":1}`,
			metadata: map[string]interface{}{},
			want: map[string]interface{}{
				"id":           float64(1),
				"source_topic": "",
				"missing":      "",
			},
		},
		{
			name: "json object and array",
			config: &v1.InsertFieldTransformation{Fields: map[string]string{
				"meta":  `json:{"k":1}`,
				"tags":  `json:["a","b"]`,
				"count": "json:42",
			}},
			input: `{}`,
			want: map[string]interface{}{
				"meta":  map[string]interface{}{"k": float64(1)},
				"tags":  []interface{}{"a", "b"},
				"count": float64(42),
			},
		},
		{
			name: "JSONPath prefix and overwrite",
			config: &v1.InsertFieldTransformation{Fields: map[string]string{
				"$.name": "inserted",
			}},
			input: `{"name":"old","id":1}`,
			want: map[string]interface{}{
				"name": "inserted",
				"id":   float64(1),
			},
		},
		{
			name: "now placeholder",
			config: &v1.InsertFieldTransformation{Fields: map[string]string{
				"ingested_at": "${now}",
			}},
			input:    `{"id":1}`,
			checkNow: "ingested_at",
			want: map[string]interface{}{
				"id": float64(1),
			},
		},
		{
			name: "preserves metadata map",
			config: &v1.InsertFieldTransformation{Fields: map[string]string{
				"x": "y",
			}},
			input:    `{"a":1}`,
			metadata: map[string]interface{}{"topic": "t1"},
			want: map[string]interface{}{
				"a": float64(1),
				"x": "y",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := types.NewMessage([]byte(tt.input))
			if tt.metadata != nil {
				msg.Metadata = tt.metadata
			}

			out, err := NewInsertFieldTransformer(tt.config).Transform(context.Background(), msg)
			require.NoError(t, err)
			require.Len(t, out, 1)

			var got map[string]interface{}
			require.NoError(t, json.Unmarshal(out[0].Data, &got))

			if tt.checkNow != "" {
				ts, ok := got[tt.checkNow].(string)
				require.True(t, ok, "expected %s string", tt.checkNow)
				_, err := time.Parse(time.RFC3339, ts)
				require.NoError(t, err)
				delete(got, tt.checkNow)
			}

			assert.Equal(t, tt.want, got)
			if tt.metadata != nil {
				assert.Equal(t, tt.metadata, out[0].Metadata)
			}
		})
	}
}

func TestInsertFieldTransformer_NonJSONPassthrough(t *testing.T) {
	msg := types.NewMessage([]byte("not-json"))
	msg.Metadata = map[string]interface{}{"topic": "t"}

	out, err := NewInsertFieldTransformer(&v1.InsertFieldTransformation{
		Fields: map[string]string{"x": "y"},
	}).Transform(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, []byte("not-json"), out[0].Data)
	assert.Equal(t, msg.Metadata, out[0].Metadata)
}

func TestInsertFieldTransformer_InvalidJSONValue(t *testing.T) {
	msg := types.NewMessage([]byte(`{"id":1}`))
	_, err := NewInsertFieldTransformer(&v1.InsertFieldTransformation{
		Fields: map[string]string{"bad": "json:{not-json"},
	}).Transform(context.Background(), msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid json")
}

func TestResolveInsertFieldValue(t *testing.T) {
	meta := map[string]interface{}{"topic": "orders"}

	v, raw, err := resolveInsertFieldValue("literal", meta)
	require.NoError(t, err)
	assert.False(t, raw)
	assert.Equal(t, "literal", v)

	v, raw, err = resolveInsertFieldValue("${metadata.topic}", meta)
	require.NoError(t, err)
	assert.False(t, raw)
	assert.Equal(t, "orders", v)

	v, raw, err = resolveInsertFieldValue("json:true", meta)
	require.NoError(t, err)
	assert.True(t, raw)
	assert.Equal(t, "true", v)
}
