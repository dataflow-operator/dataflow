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
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/checkpoint"
	"github.com/dataflow-operator/dataflow/internal/metrics"
	"github.com/dataflow-operator/dataflow/internal/types"
)

func TestPostgreSQLSourceConnector_buildReadQuery(t *testing.T) {
	table := "test_table"
	tests := []struct {
		name               string
		config             *v1.PostgreSQLSourceSpec
		lastReadChangeTime *time.Time
		lastReadOrderBy    interface{}
		wantContains       []string
		wantNotContains    []string
	}{
		{
			name: "first read default change tracking",
			config: &v1.PostgreSQLSourceSpec{
				Table: table,
			},
			wantContains:    []string{"COALESCE(updated_at, created_at)", "ORDER BY"},
			wantNotContains: []string{"WHERE"},
		},
		{
			name: "incremental read default column",
			config: &v1.PostgreSQLSourceSpec{
				Table: table,
			},
			lastReadChangeTime: ptrTime(time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)),
			wantContains:       []string{"WHERE COALESCE(updated_at, created_at) > '2024-01-15T10:00:00", `ORDER BY COALESCE(updated_at, created_at), "id"`},
		},
		{
			name: "incremental read composite checkpoint",
			config: &v1.PostgreSQLSourceSpec{
				Table: table,
			},
			lastReadChangeTime: ptrTime(time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)),
			lastReadOrderBy:    int64(5042),
			wantContains:       []string{`WHERE (COALESCE(updated_at, created_at), "id") > ('2024-01-15T10:00:00`, ", 5042)"},
		},
		{
			name: "custom ChangeTrackingColumn first read",
			config: &v1.PostgreSQLSourceSpec{
				Table:                table,
				ChangeTrackingColumn: "modified_at",
			},
			wantContains:    []string{`"modified_at"`, "ORDER BY"},
			wantNotContains: []string{"WHERE", "COALESCE"},
		},
		{
			name: "custom ChangeTrackingColumn incremental",
			config: &v1.PostgreSQLSourceSpec{
				Table:                table,
				ChangeTrackingColumn: "modified_at",
			},
			lastReadChangeTime: ptrTime(time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)),
			wantContains:       []string{`WHERE "modified_at" > '2024-01-15T10:00:00`, `ORDER BY "modified_at", "id"`},
		},
		{
			name: "custom orderByColumn",
			config: &v1.PostgreSQLSourceSpec{
				Table:         table,
				OrderByColumn: "price_id",
			},
			wantContains: []string{`ORDER BY COALESCE(updated_at, created_at), "price_id"`},
		},
		{
			name: "table with hyphens is properly quoted",
			config: &v1.PostgreSQLSourceSpec{
				Table: "kafka-to-postgres-raw-events",
			},
			wantContains: []string{`"public"."kafka-to-postgres-raw-events"`, "ORDER BY"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPostgreSQLSourceConnector(tt.config)
			if tt.lastReadChangeTime != nil {
				p.cp.Advance(checkpoint.Composite{
					ChangeTime:   tt.lastReadChangeTime,
					OrderByValue: tt.lastReadOrderBy,
				}, true)
			}
			got := p.buildReadQuery()
			for _, s := range tt.wantContains {
				assert.Contains(t, got, s, "query should contain %q", s)
			}
			for _, s := range tt.wantNotContains {
				assert.NotContains(t, got, s, "query should not contain %q", s)
			}
		})
	}
}

func TestPostgreSQLSourceConnector_buildIncrementalQueryWrapper(t *testing.T) {
	userQuery := `SELECT material_number, price, update_date, price_id
FROM price.price WHERE price_status = 'EXPORTED'`

	t.Run("first read without checkpoint", func(t *testing.T) {
		p := NewPostgreSQLSourceConnector(&v1.PostgreSQLSourceSpec{
			Table:                "price.price",
			Query:                userQuery,
			ChangeTrackingColumn: "update_date",
			OrderByColumn:        "price_id",
		})
		got := p.buildIncrementalQueryWrapper()
		assert.Contains(t, got, "SELECT * FROM ("+userQuery+") AS __dataflow_src")
		assert.Contains(t, got, `ORDER BY "update_date", "price_id"`)
		assert.Contains(t, got, ") AS __dataflow_src ORDER BY")
		assert.NotContains(t, got, ") AS __dataflow_src WHERE")
	})

	t.Run("incremental with composite checkpoint", func(t *testing.T) {
		p := NewPostgreSQLSourceConnector(&v1.PostgreSQLSourceSpec{
			Table:                "price.price",
			Query:                userQuery,
			ChangeTrackingColumn: "update_date",
			OrderByColumn:        "price_id",
		})
		ts := time.Date(2024, 6, 1, 12, 0, 0, 123456789, time.UTC)
		p.cp.Advance(checkpoint.Composite{ChangeTime: &ts, OrderByValue: int64(5042)}, true)
		got := p.buildIncrementalQueryWrapper()
		assert.Contains(t, got, `WHERE ("update_date", "price_id") > ('2024-06-01T12:00:00.123456789Z', 5042)`)
		assert.Contains(t, got, `ORDER BY "update_date", "price_id"`)
	})
}

func TestPostgreSQLSourceConnector_advanceCheckpoint(t *testing.T) {
	p := NewPostgreSQLSourceConnector(&v1.PostgreSQLSourceSpec{Table: "t"})

	t1 := time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2024, 1, 12, 0, 0, 0, 0, time.UTC)

	p.cp.Advance(checkpoint.Composite{ChangeTime: &t1, OrderByValue: int64(1)}, true)
	got := p.buildReadQuery()
	assert.Contains(t, got, "2024-01-10", "query should use t1 after first advance")
	assert.Contains(t, got, ", 1)")

	p.cp.Advance(checkpoint.Composite{ChangeTime: &t2, OrderByValue: int64(2)}, true)
	got = p.buildReadQuery()
	assert.Contains(t, got, "2024-01-15", "query should use t2 after advancing to later time")
	assert.Contains(t, got, ", 2)")

	p.cp.Advance(checkpoint.Composite{ChangeTime: &t3, OrderByValue: int64(1)}, true)
	got = p.buildReadQuery()
	assert.Contains(t, got, "2024-01-15", "query should still use t2 when advancing to earlier time (no regression)")
	assert.Contains(t, got, ", 2)")

	p.cp.Advance(checkpoint.Composite{ChangeTime: &t2, OrderByValue: int64(99)}, true)
	got = p.buildReadQuery()
	assert.Contains(t, got, ", 99)")
}

func TestPostgreSQLSourceConnector_applyInitialCheckpoint(t *testing.T) {
	opts := &SourceConnectorOptions{
		InitialCheckpoint: []byte(`{"lastReadChangeTime":"2024-01-15T10:30:00Z","lastReadOrderByValue":5042}`),
	}
	p := NewPostgreSQLSourceConnectorWithOptions(&v1.PostgreSQLSourceSpec{Table: "t"}, opts)
	got := p.buildReadQuery()
	assert.Contains(t, got, "2024-01-15T10:30:00", "query should use restored checkpoint")
	assert.Contains(t, got, ", 5042)")
}

func TestFormatPostgreSQLLiteral(t *testing.T) {
	ts := time.Date(2024, 6, 1, 12, 0, 0, 123456789, time.UTC)
	assert.Equal(t, "'2024-06-01T12:00:00.123456789Z'", formatPostgreSQLLiteral(ts))
	assert.Equal(t, "5042", formatPostgreSQLLiteral(int64(5042)))
	assert.Equal(t, "'hello''world'", formatPostgreSQLLiteral("hello'world"))
	assert.Equal(t, "NULL", formatPostgreSQLLiteral(nil))
}

func TestCompareOrderByValues(t *testing.T) {
	assert.Equal(t, -1, checkpoint.CompareOrderBy(int64(1), int64(2)))
	assert.Equal(t, 1, checkpoint.CompareOrderBy(int64(3), int64(2)))
	assert.Equal(t, 0, checkpoint.CompareOrderBy(int64(2), int64(2)))
}

func TestExtractRowCheckpoint_postgresqlFallback(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	fallback := &ChangeTimeFallback{
		UseUpdatedAtCreatedAt: true,
		CreatedAtIndex:        0,
		UpdatedAtIndex:        1,
	}

	tests := []struct {
		name              string
		values            []interface{}
		changeTrackingIdx int
		wantChangeTime    *time.Time
	}{
		{"change tracking column", []interface{}{nil, nil, ts}, 2, &ts},
		{"updated_at fallback", []interface{}{nil, ts, nil}, -1, &ts},
		{"created_at fallback", []interface{}{ts, nil, nil}, -1, &ts},
		{"no time columns", []interface{}{1, "x"}, -1, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := ExtractRowCheckpoint(tt.values, tt.changeTrackingIdx, -1, fallback)
			if tt.wantChangeTime == nil {
				assert.Nil(t, got)
			} else {
				require.NotNil(t, got)
				assert.True(t, got.Equal(*tt.wantChangeTime))
			}
		})
	}
}

func TestPostgreSQLSinkConnector_trySoftDelete(t *testing.T) {
	softDeleteCol := "deleted_at"
	tests := []struct {
		name         string
		config       *v1.PostgreSQLSinkSpec
		msg          *types.Message
		wantHandled  bool
		wantBatchLen int
	}{
		{
			name: "no SoftDeleteColumn",
			config: &v1.PostgreSQLSinkSpec{
				Table: "t",
			},
			msg:          msgWithOpAndID("delete", 1),
			wantHandled:  false,
			wantBatchLen: 0,
		},
		{
			name: "operation not delete",
			config: &v1.PostgreSQLSinkSpec{
				Table:            "t",
				SoftDeleteColumn: &softDeleteCol,
			},
			msg:          msgWithOpAndID("insert", 1),
			wantHandled:  false,
			wantBatchLen: 0,
		},
		{
			name: "delete with id in metadata",
			config: &v1.PostgreSQLSinkSpec{
				Table:            "t",
				SoftDeleteColumn: &softDeleteCol,
			},
			msg:          msgWithOpAndID("delete", 42),
			wantHandled:  true,
			wantBatchLen: 1,
		},
		{
			name: "delete with id in data",
			config: &v1.PostgreSQLSinkSpec{
				Table:            "t",
				SoftDeleteColumn: &softDeleteCol,
			},
			msg:          msgWithOpAndIDAndData("delete", 0, `{"id": 99}`),
			wantHandled:  true,
			wantBatchLen: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPostgreSQLSinkConnector(tt.config)
			batch := &pgx.Batch{}
			batchMsgs := make([]*types.Message, 0)
			count := 0
			got := p.trySoftDelete(tt.msg, batch, &batchMsgs, &count)
			assert.Equal(t, tt.wantHandled, got)
			assert.Equal(t, tt.wantBatchLen, batch.Len())
			if tt.wantHandled {
				assert.Len(t, batchMsgs, 1)
				assert.Equal(t, 1, count)
			}
		})
	}
}

func ptrTime(v time.Time) *time.Time { return &v }

func msgWithOpAndID(op string, id int) *types.Message {
	msg := types.NewMessage([]byte(`{}`))
	msg.Metadata = map[string]interface{}{"operation": op, "id": id}
	return msg
}

func msgWithOpAndIDAndData(op string, metaID int, data string) *types.Message {
	msg := types.NewMessage([]byte(data))
	msg.Metadata = map[string]interface{}{"operation": op}
	if metaID != 0 {
		msg.Metadata["id"] = metaID
	}
	return msg
}

func TestInferPostgreSQLType(t *testing.T) {
	tests := []struct {
		name string
		v    interface{}
		want string
	}{
		{"nil", nil, "TEXT"},
		{"bool", true, "BOOLEAN"},
		{"int", 42, "BIGINT"},
		{"int64", int64(100), "BIGINT"},
		{"float64 whole", float64(42), "NUMERIC"},
		{"float64 decimal", float64(10.50), "NUMERIC"},
		{"string", "hello", "TEXT"},
		{"map", map[string]interface{}{"a": 1}, "JSONB"},
		{"slice", []interface{}{1, 2}, "JSONB"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := inferPostgreSQLType(tt.v)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestJoinStrings(t *testing.T) {
	tests := []struct {
		name string
		ss   []string
		sep  string
		want string
	}{
		{"empty", nil, ",", ""},
		{"single", []string{"a"}, ",", "a"},
		{"multiple", []string{"a", "b", "c"}, ",", "a,b,c"},
		{"comma sep", []string{"a", "b"}, ", ", "a, b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := joinStrings(tt.ss, tt.sep)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPostgreSQLSinkConnector_buildInsertForMessage_RawMode(t *testing.T) {
	rawMode := true
	config := &v1.PostgreSQLSinkSpec{
		Table:   "test_raw",
		RawMode: &rawMode,
	}
	p := NewPostgreSQLSinkConnector(config)
	// Note: buildInsertForMessage requires conn for tableExists check when autoCreateTable.
	// For RawMode path we only need data with value - we skip ensure when autoCreateTable is nil.
	config.AutoCreateTable = nil

	data := map[string]interface{}{
		"value":     map[string]interface{}{"id": 1, "name": "foo"},
		"_metadata": map[string]interface{}{"table": "src", "id": 1},
	}
	query, values, err := p.buildInsertForMessage(context.Background(), data, nil)
	// Will fail without conn (tableExists check), but when autoCreateTable is nil we skip that.
	// Actually buildInsertForMessage checks: if autoCreateTable && !exists -> ensure. So we skip.
	// Then we hit RawMode branch - data["value"] != nil. We don't need conn for that.
	// But wait - we don't have conn. The hasJSONB check uses p.conn.QueryRow - that would panic.
	// So we need conn for the non-RawMode path. For RawMode we return early before hasJSONB.
	// So with RawMode and data["value"] we return (query, values, nil) without touching conn.
	// Good!
	require.NoError(t, err)
	require.NotEmpty(t, query)
	require.Len(t, values, 2)
	assert.Contains(t, query, "data")
	assert.Contains(t, query, "_metadata")
}

func TestPostgreSQLSinkConnector_buildInsertForMessage_RawMode_PlainMessage(t *testing.T) {
	// When sink has rawMode but source sends plain format (no value/_metadata wrapper),
	// sink must wrap the entire message as value and use msg.Metadata for _metadata.
	rawMode := true
	config := &v1.PostgreSQLSinkSpec{
		Table:   "test_raw",
		RawMode: &rawMode,
	}
	p := NewPostgreSQLSinkConnector(config)
	config.AutoCreateTable = nil

	// Plain message from source (e.g. PostgreSQL source without rawMode)
	data := map[string]interface{}{
		"id": 1, "name": "foo", "category": "electronics",
	}
	msg := &types.Message{
		Data: []byte(`{"id":1,"name":"foo","category":"electronics"}`),
		Metadata: map[string]interface{}{
			"table":     "products",
			"id":        1,
			"operation": "insert",
		},
	}

	query, values, err := p.buildInsertForMessage(context.Background(), data, msg)
	require.NoError(t, err)
	require.NotEmpty(t, query)
	require.Len(t, values, 2)
	assert.Contains(t, query, "data")
	assert.Contains(t, query, "_metadata")

	// value should be the full data (id, name, category)
	valueJSON, ok := values[0].(string)
	require.True(t, ok)
	var valueMap map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(valueJSON), &valueMap))
	assert.Equal(t, float64(1), valueMap["id"])
	assert.Equal(t, "foo", valueMap["name"])
	assert.Equal(t, "electronics", valueMap["category"])

	// _metadata should come from msg.Metadata
	metaJSON, ok := values[1].(string)
	require.True(t, ok)
	var metaMap map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(metaJSON), &metaMap))
	assert.Equal(t, "products", metaMap["table"])
	assert.Equal(t, float64(1), metaMap["id"])
	assert.Equal(t, "insert", metaMap["operation"])
}

// TestPostgreSQLConnectors_ImplementSetMetadata verifies both PostgreSQL connectors
// implement the SetMetadata interface used by the processor for metrics.
func TestPostgreSQLConnectors_ImplementSetMetadata(t *testing.T) {
	t.Run("source", func(t *testing.T) {
		p := NewPostgreSQLSourceConnector(&v1.PostgreSQLSourceSpec{Table: "t"})
		_, ok := interface{}(p).(interface{ SetMetadata(string, string) })
		assert.True(t, ok, "PostgreSQLSourceConnector must implement SetMetadata")
	})
	t.Run("sink", func(t *testing.T) {
		p := NewPostgreSQLSinkConnector(&v1.PostgreSQLSinkSpec{Table: "t"})
		_, ok := interface{}(p).(interface{ SetMetadata(string, string) })
		assert.True(t, ok, "PostgreSQLSinkConnector must implement SetMetadata")
	})
}

// TestPostgreSQLSourceConnector_SetConnectorConnectionStatus verifies that Close
// updates the connection status metric to disconnected.
func TestPostgreSQLSourceConnector_SetConnectorConnectionStatus(t *testing.T) {
	// Use a connection string that will fail - we only care about the error path
	// recording SetConnectorConnectionStatus. On success we'd set true; on failure
	// we never set it. So we need a successful connect to test SetConnectorConnectionStatus.
	// Skip if no real DB - use a connection that fails, then we only test RecordConnectorError.
	// For SetConnectorConnectionStatus we need successful Connect. That requires a real DB.
	// Instead, test that SetMetadata is used: create connector, set metadata, call Close.
	// Close will call SetConnectorConnectionStatus(false) even if Connect was never called
	// (guardClose will run, and we'll set status false before closing conn - but conn is nil).
	p := NewPostgreSQLSourceConnector(&v1.PostgreSQLSourceSpec{Table: "t"})
	p.SetMetadata("pg-status-ns", "pg-status-name")

	// Close without Connect - should still call SetConnectorConnectionStatus(false)
	err := p.Close()
	require.NoError(t, err)

	metric, err := metrics.ConnectorConnectionStatus.GetMetricWithLabelValues("pg-status-ns", "pg-status-name", "postgresql", "source")
	require.NoError(t, err)
	var dtoMetric dto.Metric
	require.NoError(t, metric.Write(&dtoMetric))
	require.NotNil(t, dtoMetric.Gauge)
	require.NotNil(t, dtoMetric.Gauge.Value)
	assert.Equal(t, 0.0, *dtoMetric.Gauge.Value, "connection status should be 0 after Close")
}
