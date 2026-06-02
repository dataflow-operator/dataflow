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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
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

func TestExtractDataAndMetadata(t *testing.T) {
	tests := []struct {
		name     string
		msg      *types.Message
		wantData string
		wantMeta string
	}{
		{
			name: "raw_format",
			msg: &types.Message{
				Data: []byte(`{"value":{"id":1,"name":"foo"},"_metadata":{"offset":10,"topic":"t1"}}`),
			},
			wantData: `{"id":1,"name":"foo"}`,
			wantMeta: `{"offset":10,"topic":"t1"}`,
		},
		{
			name: "plain_format",
			msg: &types.Message{
				Data:     []byte(`{"id":2,"name":"bar"}`),
				Metadata: map[string]interface{}{"table": "products", "id": float64(2)},
			},
			wantData: `{"id":2,"name":"bar"}`,
			wantMeta: `{"id":2,"table":"products"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotData, gotMeta := extractDataAndMetadata(tt.msg)
			assert.JSONEq(t, tt.wantData, gotData)
			assert.JSONEq(t, tt.wantMeta, gotMeta)
		})
	}
}

func TestUnwrapMessageDataForColumns(t *testing.T) {
	tests := []struct {
		name     string
		msgData  string
		wantKeys []string
	}{
		{
			name:     "wrapped_value_format",
			msgData:  `{"value":{"id":1,"name":"foo"},"_metadata":{"offset":10}}`,
			wantKeys: []string{"id", "name"},
		},
		{
			name:     "plain_columnar_format",
			msgData:  `{"id":2,"name":"bar","amount":100}`,
			wantKeys: []string{"id", "name", "amount"},
		},
		{
			name:     "value_not_map_returns_as_is",
			msgData:  `{"value":"string","_metadata":{}}`,
			wantKeys: []string{"value", "_metadata"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m map[string]interface{}
			require.NoError(t, json.Unmarshal([]byte(tt.msgData), &m))
			got := unwrapMessageDataForColumns(m)
			gotKeys := make([]string, 0, len(got))
			for k := range got {
				gotKeys = append(gotKeys, k)
			}
			assert.ElementsMatch(t, tt.wantKeys, gotKeys)
		})
	}
}

func TestTrinoSinkConnector_hasRawModeColumns(t *testing.T) {
	connector := NewTrinoSinkConnector(&v1.TrinoSinkSpec{
		ServerURL: "http://localhost:8080",
		Catalog:   "test",
		Schema:    "test",
		Table:     "test",
	})
	tests := []struct {
		name        string
		columns     []TableColumnInfo
		wantOk      bool
		wantDataCol string
	}{
		{
			name: "data_and_metadata",
			columns: []TableColumnInfo{
				{Name: "data", Type: "varchar"},
				{Name: "_metadata", Type: "varchar"},
			},
			wantOk:      true,
			wantDataCol: "data",
		},
		{
			name: "value_and_metadata",
			columns: []TableColumnInfo{
				{Name: "value", Type: "varchar"},
				{Name: "_metadata", Type: "varchar"},
			},
			wantOk:      true,
			wantDataCol: "value",
		},
		{
			name: "data_value_and_metadata_prefers_data",
			columns: []TableColumnInfo{
				{Name: "data", Type: "varchar"},
				{Name: "value", Type: "varchar"},
				{Name: "_metadata", Type: "varchar"},
			},
			wantOk:      true,
			wantDataCol: "data",
		},
		{
			name: "missing_data",
			columns: []TableColumnInfo{
				{Name: "_metadata", Type: "varchar"},
			},
			wantOk:      false,
			wantDataCol: "",
		},
		{
			name: "missing_metadata",
			columns: []TableColumnInfo{
				{Name: "data", Type: "varchar"},
			},
			wantOk:      false,
			wantDataCol: "",
		},
		{
			name: "missing_metadata_value_only",
			columns: []TableColumnInfo{
				{Name: "value", Type: "varchar"},
			},
			wantOk:      false,
			wantDataCol: "",
		},
		{
			name: "columnar_schema",
			columns: []TableColumnInfo{
				{Name: "id", Type: "bigint"},
				{Name: "name", Type: "varchar"},
				{Name: "amount", Type: "double"},
			},
			wantOk:      false,
			wantDataCol: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOk, gotDataCol := connector.hasRawModeColumns(tt.columns)
			assert.Equal(t, tt.wantOk, gotOk)
			assert.Equal(t, tt.wantDataCol, gotDataCol)
		})
	}
}

func TestTrinoSinkConnector_executeBatchRaw_valueColumn(t *testing.T) {
	var capturedQuery string
	var captureMu sync.Mutex
	trinoResponse := map[string]interface{}{
		"id":      "test-query-id",
		"nextUri": "",
		"stats":   map[string]interface{}{"state": "FINISHED"},
		"columns": []interface{}{},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/v1/statement") {
			body, _ := io.ReadAll(r.Body)
			captureMu.Lock()
			capturedQuery = string(body)
			captureMu.Unlock()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(trinoResponse)
	}))
	defer server.Close()

	connector := NewTrinoSinkConnector(&v1.TrinoSinkSpec{
		ServerURL: server.URL,
		Catalog:   "test",
		Schema:    "test",
		Table:     "tbl",
	})
	connector.SetLogger(logr.Discard())
	connector.client = newTrinoClientForTest(server.URL, "test", "test", server.Client())

	msg := &types.Message{
		Data:     []byte(`{"requestBody":{"id":1},"requestHeader":{"type":"EVENT"}}`),
		Metadata: map[string]interface{}{"topic": "t1", "partition": 0},
	}
	err := connector.executeBatchRaw(context.Background(), []*types.Message{msg}, "value")
	require.NoError(t, err)

	captureMu.Lock()
	query := capturedQuery
	captureMu.Unlock()
	assert.Contains(t, query, `"value"`, "INSERT should use value column")
	assert.Contains(t, query, `"_metadata"`, "INSERT should use _metadata column")
	assert.NotContains(t, query, `"data"`, "INSERT should not use data column when value is specified")
}

func TestQuoteTrinoIdentifier(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"simple", "default", "default"},
		{"catalog", "hive", "hive"},
		{"with_dash", "my-schema", `"my-schema"`},
		{"with_dot", "my.schema", `"my.schema"`},
		{"with_space", "my schema", `"my schema"`},
		{"with_quotes", `say"hello`, `"say""hello"`},
		{"empty", "", `""`},
		{"underscore_ok", "my_table", "my_table"},
		{"mixed_alnum", "table123", "table123"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := quoteTrinoIdentifier(tt.in)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTrinoSourceConnector_buildTableReadQuery(t *testing.T) {
	cfg := &v1.TrinoSourceSpec{
		Catalog: "cat",
		Schema:  "sch",
		Table:   "tbl",
	}
	t.Run("default order by id", func(t *testing.T) {
		c := NewTrinoSourceConnector(cfg)
		got := c.buildTableReadQuery(nil)
		assert.Contains(t, got, "ORDER BY id")
		assert.NotContains(t, got, "WHERE")
	})
	t.Run("incremental", func(t *testing.T) {
		c := NewTrinoSourceConnector(cfg)
		got := c.buildTableReadQuery(int64(10))
		assert.Contains(t, got, "WHERE id > 10")
		assert.Contains(t, got, "ORDER BY id")
	})
	t.Run("custom orderByColumn", func(t *testing.T) {
		cfg := *cfg
		cfg.OrderByColumn = "price_id"
		c := NewTrinoSourceConnector(&cfg)
		got := c.buildTableReadQuery(int64(5))
		assert.Contains(t, got, "WHERE price_id > 5")
		assert.Contains(t, got, "ORDER BY price_id")
	})
}

func TestTrinoSourceConnector_wrapQueryWithStableOrder(t *testing.T) {
	c := NewTrinoSourceConnector(&v1.TrinoSourceSpec{
		Catalog:       "c",
		Schema:        "s",
		Table:         "t",
		OrderByColumn: "price_id",
	})
	got := c.wrapQueryWithStableOrder("SELECT * FROM prices")
	assert.Contains(t, got, "__dataflow_src")
	assert.Contains(t, got, "ORDER BY price_id")
}

// newTrinoClientForTest creates a trinoClient with custom HTTP client for tests.
func newTrinoClientForTest(serverURL, catalog, schema string, hc *http.Client) *trinoClient {
	return &trinoClient{
		serverURL:  serverURL,
		catalog:    catalog,
		schema:     schema,
		httpClient: hc,
		logger:     logr.Discard(),
	}
}
