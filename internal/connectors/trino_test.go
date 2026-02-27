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

package connectors

import (
	"strings"
	"testing"
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTrinoSinkConnector_formatValueForType_Timestamp(t *testing.T) {
	connector := NewTrinoSinkConnector(&v1.TrinoSinkSpec{
		ServerURL: "http://localhost:8080",
		Catalog:   "test",
		Schema:    "test",
		Table:     "test",
	})
	connector.SetLogger(logr.Discard())

	tests := []struct {
		name       string
		val        interface{}
		columnType string
		want       string
		checkFunc  func(t *testing.T, result string)
	}{
		{
			name:       "RFC3339 timestamp with timezone",
			val:        "2026-01-16T13:55:03+08:00",
			columnType: "timestamp",
			checkFunc: func(t *testing.T, result string) {
				// Should be formatted as TIMESTAMP 'YYYY-MM-DD HH:MM:SS' without timezone
				// Time is converted to UTC: 13:55:03+08:00 = 05:55:03 UTC
				assert.Contains(t, result, "TIMESTAMP '")
				assert.Contains(t, result, "2026-01-16")
				assert.Contains(t, result, "05:55:03") // UTC time
				// Should not contain timezone offset
				assert.NotContains(t, result, "+08:00")
				assert.NotContains(t, result, "-08:00")
			},
		},
		{
			name:       "RFC3339 timestamp with Z",
			val:        "2026-01-16T13:55:03Z",
			columnType: "timestamp",
			checkFunc: func(t *testing.T, result string) {
				assert.Contains(t, result, "TIMESTAMP '")
				assert.Contains(t, result, "2026-01-16")
				assert.Contains(t, result, "13:55:03")
				assert.NotContains(t, result, "Z")
			},
		},
		{
			name:       "Unix timestamp seconds",
			val:        int64(1705390503), // 2024-01-16 13:55:03 UTC
			columnType: "timestamp",
			checkFunc: func(t *testing.T, result string) {
				assert.Contains(t, result, "TIMESTAMP '")
				assert.Contains(t, result, "2024-01-16")
			},
		},
		{
			name:       "Unix timestamp milliseconds",
			val:        int64(1705390503000), // 2024-01-16 13:55:03 UTC
			columnType: "timestamp",
			checkFunc: func(t *testing.T, result string) {
				assert.Contains(t, result, "TIMESTAMP '")
				assert.Contains(t, result, "2024-01-16")
			},
		},
		{
			name:       "Timestamp without timezone",
			val:        "2026-01-16 13:55:03",
			columnType: "timestamp",
			checkFunc: func(t *testing.T, result string) {
				assert.Contains(t, result, "TIMESTAMP '")
				assert.Contains(t, result, "2026-01-16")
				assert.Contains(t, result, "13:55:03")
			},
		},
		{
			name:       "Timestamp with timezone type",
			val:        "2026-01-16T13:55:03+08:00",
			columnType: "timestamp with time zone",
			checkFunc: func(t *testing.T, result string) {
				assert.Contains(t, result, "TIMESTAMP '")
				assert.Contains(t, result, "2026-01-16")
				assert.NotContains(t, result, "+08:00")
			},
		},
		{
			name:       "Integer value",
			val:        42,
			columnType: "integer",
			want:       "42",
		},
		{
			name:       "String value",
			val:        "test",
			columnType: "varchar",
			want:       "'test'",
		},
		{
			name:       "Boolean true",
			val:        true,
			columnType: "boolean",
			want:       "true",
		},
		{
			name:       "Boolean false",
			val:        false,
			columnType: "boolean",
			want:       "false",
		},
		{
			name:       "NULL value",
			val:        nil,
			columnType: "varchar",
			want:       "NULL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := connector.formatValueForType(tt.val, tt.columnType)
			if tt.want != "" {
				assert.Equal(t, tt.want, result)
			}
			if tt.checkFunc != nil {
				tt.checkFunc(t, result)
			}
		})
	}
}

func TestTrinoSinkConnector_formatValueForType_TimestampEdgeCases(t *testing.T) {
	connector := NewTrinoSinkConnector(&v1.TrinoSinkSpec{
		ServerURL: "http://localhost:8080",
		Catalog:   "test",
		Schema:    "test",
		Table:     "test",
	})
	connector.SetLogger(logr.Discard())

	tests := []struct {
		name       string
		val        interface{}
		columnType string
		validate   func(t *testing.T, result string)
	}{
		{
			name:       "RFC3339 with nanoseconds",
			val:        "2026-01-16T13:55:03.123456789+08:00",
			columnType: "timestamp",
			validate: func(t *testing.T, result string) {
				// Should parse and format correctly
				// Time is converted to UTC: 13:55:03+08:00 = 05:55:03 UTC
				assert.Contains(t, result, "TIMESTAMP '")
				assert.Contains(t, result, "2026-01-16")
				assert.Contains(t, result, "05:55:03") // UTC time
				// Should not contain timezone
				assert.NotContains(t, result, "+08:00")
			},
		},
		{
			name:       "Timestamp with negative timezone",
			val:        "2026-01-16T13:55:03-05:00",
			columnType: "timestamp",
			validate: func(t *testing.T, result string) {
				assert.Contains(t, result, "TIMESTAMP '")
				assert.NotContains(t, result, "-05:00")
			},
		},
		{
			name:       "Float64 Unix timestamp",
			val:        float64(1705390503.0),
			columnType: "timestamp",
			validate: func(t *testing.T, result string) {
				assert.Contains(t, result, "TIMESTAMP '")
				assert.Contains(t, result, "2024-01-16")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := connector.formatValueForType(tt.val, tt.columnType)
			require.NotEmpty(t, result)
			if tt.validate != nil {
				tt.validate(t, result)
			}
		})
	}
}

func TestTrinoSinkConnector_formatValueForType_TimestampFormat(t *testing.T) {
	connector := NewTrinoSinkConnector(&v1.TrinoSinkSpec{
		ServerURL: "http://localhost:8080",
		Catalog:   "test",
		Schema:    "test",
		Table:     "test",
	})
	connector.SetLogger(logr.Discard())

	// Test that the formatted timestamp is valid for Trino
	// Trino accepts: TIMESTAMP 'YYYY-MM-DD HH:MM:SS' or TIMESTAMP 'YYYY-MM-DDTHH:MM:SS'
	testTime := time.Date(2026, 1, 16, 13, 55, 3, 0, time.FixedZone("UTC+8", 8*3600))
	rfc3339Str := testTime.Format(time.RFC3339)

	result := connector.formatValueForType(rfc3339Str, "timestamp")

	// Should be in format: TIMESTAMP 'YYYY-MM-DD HH:MM:SS'
	// Time is converted to UTC: 13:55:03+08:00 = 05:55:03 UTC
	assert.Contains(t, result, "TIMESTAMP '")
	assert.Contains(t, result, "2026-01-16")
	assert.Contains(t, result, "05:55:03") // UTC time

	// Extract the timestamp part and verify it's parseable
	// Format should be: TIMESTAMP '2026-01-16 05:55:03' (converted to UTC)
	// The time should be converted to UTC (13:55:03 +08:00 = 05:55:03 UTC)
	assert.Contains(t, result, "05:55:03") // UTC time
}

func TestTrinoSinkConnector_nullLiteralForTrinoType(t *testing.T) {
	connector := NewTrinoSinkConnector(&v1.TrinoSinkSpec{
		ServerURL: "http://localhost:8080",
		Catalog:   "test",
		Schema:    "test",
		Table:     "test",
	})

	tests := []struct {
		columnType string
		want       string
	}{
		{"bigint", "CAST(NULL AS bigint)"},
		{"varchar", "CAST(NULL AS varchar)"},
		{"row(a int)", "CAST(NULL AS row(a int))"},
		{"  row(agentDelivery boolean, systemId varchar)  ", "CAST(NULL AS row(agentDelivery boolean, systemId varchar))"},
	}
	for _, tt := range tests {
		t.Run(tt.columnType, func(t *testing.T) {
			got := connector.nullLiteralForTrinoType(tt.columnType)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTrinoSinkConnector_formatValueForType_ROW_ARRAY(t *testing.T) {
	connector := NewTrinoSinkConnector(&v1.TrinoSinkSpec{
		ServerURL: "http://localhost:8080",
		Catalog:   "test",
		Schema:    "test",
		Table:     "test",
	})
	connector.SetLogger(logr.Discard())

	tests := []struct {
		name         string
		val          interface{}
		columnType   string
		wantPrefix   string
		wantSuffix   string
		wantContains string
	}{
		{
			name:         "ROW from map with string and number",
			val:          map[string]interface{}{"v1": float64(123), "v2": "abc", "v3": true},
			columnType:   "row(v1 bigint, v2 varchar, v3 boolean)",
			wantPrefix:   "CAST(JSON '",
			wantSuffix:   "' AS row(v1 bigint, v2 varchar, v3 boolean))",
			wantContains: `"v1":123,"v2":"abc","v3":true`,
		},
		{
			name:         "ROW nested object",
			val:          map[string]interface{}{"a": map[string]interface{}{"b": "nested"}},
			columnType:   "row(a row(b varchar))",
			wantPrefix:   "CAST(JSON '",
			wantSuffix:   "' AS row(a row(b varchar)))",
			wantContains: `"a":{"b":"nested"}`,
		},
		{
			name:         "ARRAY of integers",
			val:          []interface{}{float64(1), float64(2), float64(3)},
			columnType:   "array(integer)",
			wantPrefix:   "CAST(JSON '",
			wantSuffix:   "' AS array(integer))",
			wantContains: "[1,2,3]",
		},
		{
			name: "ARRAY of rows",
			val: []interface{}{
				map[string]interface{}{"x": "a"},
				map[string]interface{}{"x": "b"},
			},
			columnType:   "array(row(x varchar))",
			wantPrefix:   "CAST(JSON '",
			wantSuffix:   "' AS array(row(x varchar)))",
			wantContains: `"x":"a"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := connector.formatValueForType(tt.val, tt.columnType)
			assert.True(t, len(result) > 0, "result should not be empty")
			assert.True(t, len(tt.wantPrefix) > 0)
			assert.True(t, len(tt.wantSuffix) > 0)
			assert.True(t, strings.HasPrefix(result, tt.wantPrefix), "result should have prefix %q, got %q", tt.wantPrefix, result)
			assert.True(t, strings.HasSuffix(result, tt.wantSuffix), "result should have suffix %q, got %q", tt.wantSuffix, result)
			if tt.wantContains != "" {
				assert.Contains(t, result, tt.wantContains)
			}
		})
	}
}

func TestTrinoSinkConnector_formatValueForType_ROW_ARRAY_invalidValReturnsNULL(t *testing.T) {
	connector := NewTrinoSinkConnector(&v1.TrinoSinkSpec{
		ServerURL: "http://localhost:8080",
		Catalog:   "test",
		Schema:    "test",
		Table:     "test",
	})
	connector.SetLogger(logr.Discard())

	// ROW column but value is a plain string (not map) - still JSON-marshalable, so we get CAST(JSON '"...' AS row(...)).
	// Only unmarshalable values would return NULL. So test with something that fails json.Marshal - e.g. channel.
	result := connector.formatValueForType(make(chan int), "row(a int)")
	assert.Equal(t, "NULL", result)
}
