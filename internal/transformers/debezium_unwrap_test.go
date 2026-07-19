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
	"testing"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDebeziumUnwrapTransformer_EnvelopeOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string
		wantData  string
		wantOp    string
		wantError string
	}{
		{
			name:     "create maps to insert",
			input:    `{"payload":{"op":"c","after":{"id":1,"name":"alice"}}}`,
			wantData: `{"id":1,"name":"alice"}`,
			wantOp:   "insert",
		},
		{
			name:     "update maps to update",
			input:    `{"payload":{"op":"u","after":{"id":1,"name":"alice2"}}}`,
			wantData: `{"id":1,"name":"alice2"}`,
			wantOp:   "update",
		},
		{
			name:     "delete uses before",
			input:    `{"payload":{"op":"d","before":{"id":1,"name":"alice"}}}`,
			wantData: `{"id":1,"name":"alice"}`,
			wantOp:   "delete",
		},
		{
			name:      "missing required payload field returns error",
			input:     `{"payload":{"op":"u","after":null}}`,
			wantError: "payload.after must be object",
		},
	}

	tr := NewDebeziumUnwrapTransformer(&v1.DebeziumUnwrapTransformation{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := types.NewMessage([]byte(tt.input))
			msg.Metadata["origin"] = "kafka"

			out, err := tr.Transform(context.Background(), msg)
			if tt.wantError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantError)
				return
			}

			require.NoError(t, err)
			require.Len(t, out, 1)
			assert.JSONEq(t, tt.wantData, string(out[0].Data))
			assert.Equal(t, tt.wantOp, out[0].Metadata["operation"])
		})
	}
}

func TestDebeziumUnwrapTransformer_SnapshotOperationAndSourceMetadata(t *testing.T) {
	t.Parallel()

	tr := NewDebeziumUnwrapTransformer(&v1.DebeziumUnwrapTransformation{
		SnapshotOperation:       "update",
		IncludeSourceInMetadata: true,
	})
	msg := types.NewMessage([]byte(`{
		"payload":{
			"op":"r",
			"after":{"id":10,"name":"snapshot"},
			"source":{"table":"users","schema":"public","lsn":12345}
		}
	}`))

	out, err := tr.Transform(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, "update", out[0].Metadata["operation"])
	assert.Equal(t, "users", out[0].Metadata["source_table"])
	assert.Equal(t, "public", out[0].Metadata["source_schema"])
	assert.Equal(t, float64(12345), out[0].Metadata["source_lsn"])
	assert.JSONEq(t, `{"id":10,"name":"snapshot"}`, string(out[0].Data))
}

func TestDebeziumUnwrapTransformer_AddOperationFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		wantData string
		wantOp   string
	}{
		{
			name:     "create sets __op c and __deleted false",
			input:    `{"payload":{"op":"c","after":{"id":1,"name":"alice"}}}`,
			wantData: `{"id":1,"name":"alice","__op":"c","__deleted":"false"}`,
			wantOp:   "insert",
		},
		{
			name:     "update sets __op u and __deleted false",
			input:    `{"payload":{"op":"u","after":{"id":1,"name":"bob"}}}`,
			wantData: `{"id":1,"name":"bob","__op":"u","__deleted":"false"}`,
			wantOp:   "update",
		},
		{
			name:     "delete sets __op d and __deleted true",
			input:    `{"payload":{"op":"d","before":{"id":1,"name":"alice"}}}`,
			wantData: `{"id":1,"name":"alice","__op":"d","__deleted":"true"}`,
			wantOp:   "delete",
		},
		{
			name:     "snapshot sets __op r and __deleted false",
			input:    `{"payload":{"op":"r","after":{"id":2}}}`,
			wantData: `{"id":2,"__op":"r","__deleted":"false"}`,
			wantOp:   "insert",
		},
	}

	tr := NewDebeziumUnwrapTransformer(&v1.DebeziumUnwrapTransformation{AddOperationFields: true})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := types.NewMessage([]byte(tt.input))
			out, err := tr.Transform(context.Background(), msg)
			require.NoError(t, err)
			require.Len(t, out, 1)
			assert.JSONEq(t, tt.wantData, string(out[0].Data))
			assert.Equal(t, tt.wantOp, out[0].Metadata["operation"])
		})
	}
}

func TestDebeziumUnwrapTransformer_AddSourceFields(t *testing.T) {
	t.Parallel()

	tr := NewDebeziumUnwrapTransformer(&v1.DebeziumUnwrapTransformation{
		AddSourceFields: []string{"table", "lsn", "missing"},
	})
	msg := types.NewMessage([]byte(`{
		"payload":{
			"op":"c",
			"after":{"id":1,"name":"alice"},
			"source":{"table":"users","schema":"public","lsn":99}
		}
	}`))

	out, err := tr.Transform(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.JSONEq(t, `{"id":1,"name":"alice","source_table":"users","source_lsn":99}`, string(out[0].Data))
	assert.Equal(t, "insert", out[0].Metadata["operation"])
	_, hasMeta := out[0].Metadata["source_table"]
	assert.False(t, hasMeta, "addSourceFields must not imply includeSourceInMetadata")
}

func TestDebeziumUnwrapTransformer_AddOperationAndSourceFieldsTogether(t *testing.T) {
	t.Parallel()

	tr := NewDebeziumUnwrapTransformer(&v1.DebeziumUnwrapTransformation{
		AddOperationFields:      true,
		AddSourceFields:         []string{"table", "ts_ms"},
		IncludeSourceInMetadata: true,
	})
	msg := types.NewMessage([]byte(`{
		"payload":{
			"op":"u",
			"after":{"id":7,"status":"ok"},
			"source":{"table":"orders","ts_ms":1700000000000,"db":"app"}
		}
	}`))

	out, err := tr.Transform(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.JSONEq(t, `{
		"id":7,
		"status":"ok",
		"__op":"u",
		"__deleted":"false",
		"source_table":"orders",
		"source_ts_ms":1700000000000
	}`, string(out[0].Data))
	assert.Equal(t, "update", out[0].Metadata["operation"])
	assert.Equal(t, "orders", out[0].Metadata["source_table"])
	assert.Equal(t, float64(1700000000000), out[0].Metadata["source_ts_ms"])
	assert.Equal(t, "app", out[0].Metadata["source_db"])
}

func TestDebeziumUnwrapTransformer_DefaultsDoNotAddPayloadMarkers(t *testing.T) {
	t.Parallel()

	tr := NewDebeziumUnwrapTransformer(&v1.DebeziumUnwrapTransformation{})
	msg := types.NewMessage([]byte(`{
		"payload":{
			"op":"c",
			"after":{"id":1},
			"source":{"table":"users"}
		}
	}`))

	out, err := tr.Transform(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.JSONEq(t, `{"id":1}`, string(out[0].Data))
}

func TestDebeziumUnwrapTransformer_Tombstone(t *testing.T) {
	t.Parallel()

	msg := types.NewMessage([]byte{})
	msg.Metadata["key"] = `{"payload":{"id":42}}`

	t.Run("without infer drops message", func(t *testing.T) {
		tr := NewDebeziumUnwrapTransformer(&v1.DebeziumUnwrapTransformation{})
		out, err := tr.Transform(context.Background(), msg)
		require.NoError(t, err)
		assert.Empty(t, out)
	})

	t.Run("with infer builds delete message", func(t *testing.T) {
		tr := NewDebeziumUnwrapTransformer(&v1.DebeziumUnwrapTransformation{InferDeleteFromTombstone: true})
		out, err := tr.Transform(context.Background(), msg)
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.JSONEq(t, `{"id":42}`, string(out[0].Data))
		assert.Equal(t, "delete", out[0].Metadata["operation"])
		assert.Equal(t, float64(42), out[0].Metadata["id"])
	})

	t.Run("with infer and addOperationFields", func(t *testing.T) {
		tr := NewDebeziumUnwrapTransformer(&v1.DebeziumUnwrapTransformation{
			InferDeleteFromTombstone: true,
			AddOperationFields:       true,
		})
		out, err := tr.Transform(context.Background(), msg)
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.JSONEq(t, `{"id":42,"__op":"d","__deleted":"true"}`, string(out[0].Data))
		assert.Equal(t, "delete", out[0].Metadata["operation"])
	})
}

func TestDebeziumUnwrapTransformer_PassThroughWhenNotDebezium(t *testing.T) {
	t.Parallel()

	tr := NewDebeziumUnwrapTransformer(&v1.DebeziumUnwrapTransformation{
		AddOperationFields: true,
		AddSourceFields:    []string{"table"},
	})
	msg := types.NewMessage([]byte(`{"event":"plain-json"}`))
	msg.Metadata["type"] = "non-debezium"

	out, err := tr.Transform(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, msg, out[0])
}
