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
}

func TestDebeziumUnwrapTransformer_PassThroughWhenNotDebezium(t *testing.T) {
	t.Parallel()

	tr := NewDebeziumUnwrapTransformer(&v1.DebeziumUnwrapTransformation{})
	msg := types.NewMessage([]byte(`{"event":"plain-json"}`))
	msg.Metadata["type"] = "non-debezium"

	out, err := tr.Transform(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, msg, out[0])
}
