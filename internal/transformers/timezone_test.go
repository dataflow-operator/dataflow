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
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimezoneTransformer_Transform(t *testing.T) {
	moscow, err := time.LoadLocation("Europe/Moscow")
	require.NoError(t, err)

	utcNoon := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	moscowNoonAsRFC3339 := utcNoon.In(moscow).Format(time.RFC3339)
	moscowNoonAsRFC3339Nano := utcNoon.In(moscow).Format(time.RFC3339Nano)

	tests := []struct {
		name   string
		config *v1.TimezoneTransformation
		input  string
		want   string
	}{
		{
			name: "RFC3339 with offset to Europe/Moscow",
			config: &v1.TimezoneTransformation{
				Timezone: "Europe/Moscow",
				Fields:   []string{"created_at"},
				Format:   "RFC3339",
			},
			input: `{"created_at":"2024-01-15T12:00:00Z"}`,
			want:  `{"created_at":"` + moscowNoonAsRFC3339 + `"}`,
		},
		{
			name: "default format RFC3339Nano",
			config: &v1.TimezoneTransformation{
				Timezone: "Europe/Moscow",
				Fields:   []string{"created_at"},
			},
			input: `{"created_at":"2024-01-15T12:00:00Z"}`,
			want:  `{"created_at":"` + moscowNoonAsRFC3339Nano + `"}`,
		},
		{
			name: "naive string uses sourceTimezone",
			config: &v1.TimezoneTransformation{
				Timezone:       "+03:00",
				SourceTimezone: "UTC",
				Fields:         []string{"ts"},
				Format:         "RFC3339",
			},
			input: `{"ts":"2024-01-15T12:00:00"}`,
			want:  `{"ts":"2024-01-15T15:00:00+03:00"}`,
		},
		{
			name: "epoch seconds",
			config: &v1.TimezoneTransformation{
				Timezone: "UTC",
				Fields:   []string{"ts"},
				Format:   "RFC3339",
			},
			input: `{"ts":1705320000}`,
			want:  `{"ts":"2024-01-15T12:00:00Z"}`,
		},
		{
			name: "epoch milliseconds",
			config: &v1.TimezoneTransformation{
				Timezone: "UTC",
				Fields:   []string{"ts"},
				Format:   "RFC3339",
			},
			input: `{"ts":1705320000000}`,
			want:  `{"ts":"2024-01-15T12:00:00Z"}`,
		},
		{
			name: "epoch numeric string milliseconds",
			config: &v1.TimezoneTransformation{
				Timezone: "UTC",
				Fields:   []string{"ts"},
				Format:   "UnixMilli",
			},
			input: `{"ts":"1705320000000"}`,
			want:  `{"ts":1705320000000}`,
		},
		{
			name: "UnixMilli output",
			config: &v1.TimezoneTransformation{
				Timezone: "Europe/Moscow",
				Fields:   []string{"ts"},
				Format:   "UnixMilli",
			},
			input: `{"ts":"2024-01-15T12:00:00Z"}`,
			want:  `{"ts":1705320000000}`,
		},
		{
			name: "missing field and null skipped",
			config: &v1.TimezoneTransformation{
				Timezone: "UTC",
				Fields:   []string{"missing", "gone", "keep"},
				Format:   "RFC3339",
			},
			input: `{"gone":null,"keep":"2024-01-15T12:00:00Z","other":1}`,
			want:  `{"gone":null,"keep":"2024-01-15T12:00:00Z","other":1}`,
		},
		{
			name: "nested path with JSONPath prefix",
			config: &v1.TimezoneTransformation{
				Timezone: "UTC",
				Fields:   []string{"$.row.ts"},
				Format:   "RFC3339",
			},
			input: `{"row":{"ts":1705320000}}`,
			want:  `{"row":{"ts":"2024-01-15T12:00:00Z"}}`,
		},
		{
			name: "preserves metadata",
			config: &v1.TimezoneTransformation{
				Timezone: "UTC",
				Fields:   []string{"ts"},
				Format:   "RFC3339",
			},
			input: `{"ts":"2024-01-15T12:00:00+00:00"}`,
			want:  `{"ts":"2024-01-15T12:00:00Z"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := types.NewMessage([]byte(tt.input))
			msg.Metadata["operation"] = "insert"

			out, err := NewTimezoneTransformer(tt.config).Transform(context.Background(), msg)
			require.NoError(t, err)
			require.Len(t, out, 1)
			assert.JSONEq(t, tt.want, string(out[0].Data))
			assert.Equal(t, "insert", out[0].Metadata["operation"])
		})
	}
}

func TestTimezoneTransformer_Errors(t *testing.T) {
	tests := []struct {
		name    string
		config  *v1.TimezoneTransformation
		input   string
		wantErr string
	}{
		{
			name: "unparseable string",
			config: &v1.TimezoneTransformation{
				Timezone: "UTC",
				Fields:   []string{"ts"},
			},
			input:   `{"ts":"not-a-time"}`,
			wantErr: "cannot parse",
		},
		{
			name: "object value",
			config: &v1.TimezoneTransformation{
				Timezone: "UTC",
				Fields:   []string{"ts"},
			},
			input:   `{"ts":{"a":1}}`,
			wantErr: "cannot parse",
		},
		{
			name: "invalid timezone",
			config: &v1.TimezoneTransformation{
				Timezone: "Not/AZone",
				Fields:   []string{"ts"},
			},
			input:   `{"ts":"2024-01-15T12:00:00Z"}`,
			wantErr: "timezone",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := types.NewMessage([]byte(tt.input))
			out, err := NewTimezoneTransformer(tt.config).Transform(context.Background(), msg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Nil(t, out)
		})
	}
}

func TestTimezoneTransformer_Passthrough(t *testing.T) {
	transformer := NewTimezoneTransformer(&v1.TimezoneTransformation{
		Timezone: "UTC",
		Fields:   []string{"ts"},
	})

	msg := types.NewMessage([]byte("not-json"))
	out, err := transformer.Transform(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, []byte("not-json"), out[0].Data)
}

func TestNewTimezoneTransformer(t *testing.T) {
	config := &v1.TimezoneTransformation{Timezone: "UTC", Fields: []string{"ts"}}
	transformer := NewTimezoneTransformer(config)
	require.NotNil(t, transformer)
	assert.Equal(t, config, transformer.config)
}
