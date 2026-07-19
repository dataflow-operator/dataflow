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

func TestHoistFieldTransformer_Transform(t *testing.T) {
	tests := []struct {
		name   string
		config *v1.HoistFieldTransformation
		input  string
		want   string
	}{
		{
			name:   "wrap object",
			config: &v1.HoistFieldTransformation{Field: "record"},
			input:  `{"id":1}`,
			want:   `{"record":{"id":1}}`,
		},
		{
			name:   "wrap array",
			config: &v1.HoistFieldTransformation{Field: "items"},
			input:  `[1,2,3]`,
			want:   `{"items":[1,2,3]}`,
		},
		{
			name:   "wrap primitive",
			config: &v1.HoistFieldTransformation{Field: "value"},
			input:  `"hello"`,
			want:   `{"value":"hello"}`,
		},
		{
			name:   "wrap null",
			config: &v1.HoistFieldTransformation{Field: "payload"},
			input:  `null`,
			want:   `{"payload":null}`,
		},
		{
			name:   "preserves metadata",
			config: &v1.HoistFieldTransformation{Field: "row"},
			input:  `{"a":1}`,
			want:   `{"row":{"a":1}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := types.NewMessage([]byte(tt.input))
			msg.Metadata["operation"] = "update"

			out, err := NewHoistFieldTransformer(tt.config).Transform(context.Background(), msg)
			require.NoError(t, err)
			require.Len(t, out, 1)
			assert.JSONEq(t, tt.want, string(out[0].Data))
			assert.Equal(t, "update", out[0].Metadata["operation"])
		})
	}
}

func TestHoistFieldTransformer_Passthrough(t *testing.T) {
	transformer := NewHoistFieldTransformer(&v1.HoistFieldTransformation{Field: "record"})

	msg := types.NewMessage([]byte("not-json"))
	out, err := transformer.Transform(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, []byte("not-json"), out[0].Data)
}

func TestHoistFieldTransformer_RoundTripWithExtract(t *testing.T) {
	original := `{"id":1,"name":"x"}`
	hoisted, err := NewHoistFieldTransformer(&v1.HoistFieldTransformation{Field: "record"}).
		Transform(context.Background(), types.NewMessage([]byte(original)))
	require.NoError(t, err)
	require.Len(t, hoisted, 1)

	extracted, err := NewExtractFieldTransformer(&v1.ExtractFieldTransformation{Field: "record"}).
		Transform(context.Background(), hoisted[0])
	require.NoError(t, err)
	require.Len(t, extracted, 1)
	assert.JSONEq(t, original, string(extracted[0].Data))
}

func TestNewHoistFieldTransformer(t *testing.T) {
	config := &v1.HoistFieldTransformation{Field: "record"}
	transformer := NewHoistFieldTransformer(config)
	require.NotNil(t, transformer)
	assert.Equal(t, config, transformer.config)
}
