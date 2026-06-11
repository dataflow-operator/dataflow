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
	"fmt"
	"testing"
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/checkpoint"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClickHouseSourceConnector(t *testing.T) {
	spec := &v1.ClickHouseSourceSpec{
		ConnectionString: "clickhouse://localhost:9000",
		Table:            "test_table",
	}
	conn := NewClickHouseSourceConnector(spec)
	require.NotNil(t, conn)
	assert.Equal(t, spec, conn.config)
	assert.Nil(t, conn.conn)
	assert.False(t, conn.closed)
}

func TestClickHouseSourceConnector_Read_WithoutConnect(t *testing.T) {
	spec := &v1.ClickHouseSourceSpec{
		ConnectionString: "clickhouse://localhost:9000",
		Table:            "test_table",
	}
	conn := NewClickHouseSourceConnector(spec)
	conn.SetLogger(logr.Discard())

	ctx := context.Background()
	_, err := conn.Read(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")
}

func TestClickHouseSourceConnector_Close_WhenAlreadyClosed(t *testing.T) {
	spec := &v1.ClickHouseSourceSpec{
		ConnectionString: "clickhouse://localhost:9000",
		Table:            "test_table",
	}
	conn := NewClickHouseSourceConnector(spec)
	conn.SetLogger(logr.Discard())

	err := conn.Close()
	require.NoError(t, err)

	err = conn.Close()
	require.NoError(t, err)
}

func TestClickHouseSourceConnector_Connect_WhenClosed(t *testing.T) {
	spec := &v1.ClickHouseSourceSpec{
		ConnectionString: "clickhouse://localhost:9000",
		Table:            "test_table",
	}
	conn := NewClickHouseSourceConnector(spec)
	conn.SetLogger(logr.Discard())
	conn.closed = true

	ctx := context.Background()
	err := conn.Connect(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestClickHouseSourceConnector_advanceCheckpoint(t *testing.T) {
	c := NewClickHouseSourceConnector(&v1.ClickHouseSourceSpec{Table: "t"})
	ts1 := time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)

	c.cp.Advance(checkpoint.Composite{ChangeTime: &ts1, OrderByValue: int64(100)}, true)
	snap := c.cp.Snapshot()
	require.NotNil(t, snap.ChangeTime)
	assert.True(t, snap.ChangeTime.Equal(ts1))
	assert.Equal(t, int64(100), snap.OrderByValue)

	c.cp.Advance(checkpoint.Composite{ChangeTime: &ts2, OrderByValue: int64(150)}, true)
	snap = c.cp.Snapshot()
	assert.True(t, snap.ChangeTime.Equal(ts2))
	assert.Equal(t, int64(150), snap.OrderByValue)

	c.cp.Advance(checkpoint.Composite{ChangeTime: &ts1, OrderByValue: int64(120)}, true)
	snap = c.cp.Snapshot()
	assert.True(t, snap.ChangeTime.Equal(ts2))
}

func TestClickHouseSourceConnector_extractRowCheckpoint(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

	changeTime, orderBy := ExtractRowCheckpoint([]interface{}{uint64(42), ts}, 1, 0, nil)
	assert.Equal(t, int64(42), int64(orderBy.(uint64)))
	require.NotNil(t, changeTime)
	assert.True(t, changeTime.Equal(ts))

	changeTime, orderBy = ExtractRowCheckpoint([]interface{}{int64(100)}, -1, 0, nil)
	assert.Equal(t, int64(100), orderBy)
	assert.Nil(t, changeTime)
}

func TestNewClickHouseSinkConnector_WithBatchFlushInterval(t *testing.T) {
	spec := &v1.ClickHouseSinkSpec{
		ConnectionString: "clickhouse://localhost:9000",
		Table:            "out",
	}
	conn := NewClickHouseSinkConnector(spec)
	require.NotNil(t, conn)
	assert.Equal(t, spec, conn.config)

	spec0 := &v1.ClickHouseSinkSpec{
		ConnectionString:          "clickhouse://localhost:9000",
		Table:                     "out",
		BatchSize:                 ptr(int32(0)),
		BatchFlushIntervalSeconds: ptr(int32(5)),
	}
	conn0 := NewClickHouseSinkConnector(spec0)
	require.NotNil(t, conn0)
	assert.Equal(t, int32(0), *conn0.config.BatchSize)
	assert.Equal(t, int32(5), *conn0.config.BatchFlushIntervalSeconds)

	specSize := &v1.ClickHouseSinkSpec{
		ConnectionString:          "clickhouse://localhost:9000",
		Table:                     "out",
		BatchSize:                 ptr(int32(100)),
		BatchFlushIntervalSeconds: ptr(int32(0)),
	}
	connSize := NewClickHouseSinkConnector(specSize)
	require.NotNil(t, connSize)
	assert.Equal(t, int32(0), *connSize.config.BatchFlushIntervalSeconds)
}

func ptr(i int32) *int32 { return &i }

func TestInferClickHouseType(t *testing.T) {
	tests := []struct {
		v    interface{}
		want string
	}{
		{nil, "Nullable(String)"},
		{true, "UInt8"},
		{float64(0), "UInt8"},
		{float64(1), "UInt8"},
		{float64(255), "UInt8"},
		{float64(256), "UInt16"},
		{float64(65535), "UInt16"},
		{float64(65536), "UInt32"},
		{float64(100), "UInt8"},
		{float64(1000), "UInt16"},
		{float64(-1), "Int8"},
		{float64(-128), "Int8"},
		{float64(-129), "Int16"},
		{float64(32767), "UInt16"},
		{float64(-32768), "Int16"},
		{float64(-32769), "Int32"},
		{float64(99.99), "Decimal(10, 2)"},
		{float64(1.5), "Decimal(10, 2)"},
		{float64(99.999), "Float64"},
		{"2026-03-05T12:27:38Z", "DateTime"},
		{"2026-01-02 15:04:05", "DateTime"},
		{"hello", "String"},
		{int(1), "UInt8"},
		{int64(1000), "Int32"},
		{uint64(1), "UInt8"},
		{uint64(100000), "UInt32"},
		{map[string]interface{}{}, "String"},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%T_%v", tt.v, tt.v), func(t *testing.T) {
			got := inferClickHouseType(tt.v)
			assert.Equal(t, tt.want, got, "inferClickHouseType(%v)", tt.v)
		})
	}
}

func TestContains(t *testing.T) {
	assert.True(t, contains([]string{"a", "b"}, "b"))
	assert.False(t, contains([]string{"a", "b"}, "c"))
}

func TestBuildInsertValues_preservesCreatedAtFromSource(t *testing.T) {
	sourceCreatedAt := "2026-03-05T12:27:38Z"
	fixedNow := time.Date(2026, 3, 5, 12, 52, 30, 0, time.UTC)
	nowFn := func() time.Time { return fixedNow }

	tests := []struct {
		name     string
		columns  []string
		rowData  map[string]interface{}
		nowFn    func() time.Time
		wantVal  interface{}
		colIndex int
	}{
		{
			name:     "created_at from source is preserved",
			columns:  []string{"id", "created_at", "name"},
			rowData:  map[string]interface{}{"id": float64(1), "created_at": sourceCreatedAt, "name": "test"},
			nowFn:    nowFn,
			wantVal:  sourceCreatedAt,
			colIndex: 1,
		},
		{
			name:     "created_at missing uses nowFn",
			columns:  []string{"id", "created_at"},
			rowData:  map[string]interface{}{"id": float64(1)},
			nowFn:    nowFn,
			wantVal:  fixedNow,
			colIndex: 1,
		},
		{
			name:     "created_at nil uses nowFn",
			columns:  []string{"id", "created_at"},
			rowData:  map[string]interface{}{"id": float64(1), "created_at": nil},
			nowFn:    nowFn,
			wantVal:  fixedNow,
			colIndex: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildInsertValues(tt.columns, tt.rowData, tt.nowFn)
			require.Len(t, got, len(tt.columns))
			assert.Equal(t, tt.wantVal, got[tt.colIndex], "created_at value mismatch")
		})
	}
}

func TestClickHouseSourceConnector_buildReadQuery(t *testing.T) {
	spec := &v1.ClickHouseSourceSpec{
		ConnectionString: "clickhouse://localhost:9000",
		Table:            "events",
	}
	c := NewClickHouseSourceConnector(spec)

	t.Run("first read", func(t *testing.T) {
		got := c.buildReadQuery()
		assert.Contains(t, got, "ORDER BY `created_at`, `id`")
		assert.NotContains(t, got, "WHERE")
	})
	t.Run("composite checkpoint", func(t *testing.T) {
		ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
		c.cp.Advance(checkpoint.Composite{ChangeTime: &ts, OrderByValue: int64(100)}, true)
		got := c.buildReadQuery()
		assert.Contains(t, got, "WHERE (`created_at`, `id`) > ('2024-06-01 12:00:00', 100)")
		assert.Contains(t, got, "ORDER BY `created_at`, `id`")
	})
	t.Run("order only legacy", func(t *testing.T) {
		c2 := NewClickHouseSourceConnector(spec)
		c2.cp.ApplyInitial([]byte(`{"lastReadOrderByValue":50}`))
		got := c2.buildReadQuery()
		assert.Contains(t, got, "WHERE `id` > 50")
		assert.Contains(t, got, "ORDER BY `id`")
	})
	t.Run("custom orderByColumn", func(t *testing.T) {
		spec := &v1.ClickHouseSourceSpec{
			ConnectionString: "clickhouse://localhost:9000",
			Table:            "prices",
			OrderByColumn:    "price_id",
		}
		c := NewClickHouseSourceConnector(spec)
		c.cp.ApplyInitial([]byte(`{"lastReadOrderByValue":50}`))
		got := c.buildReadQuery()
		assert.Contains(t, got, "WHERE `price_id` > 50")
		assert.Contains(t, got, "ORDER BY `price_id`")
	})
}

func TestClickHouseSourceConnector_orderByOnlyCheckpointAck(t *testing.T) {
	c := NewClickHouseSourceConnector(&v1.ClickHouseSourceSpec{
		ConnectionString:     "clickhouse://localhost:9000",
		Table:                "mv_one_p_prices_migration",
		ChangeTrackingColumn: "material_id",
		OrderByColumn:        "material_id",
	})
	msg := types.NewMessage([]byte(`{"material_id":200019}`))
	AssignCompositeSourceAck(msg, &c.cp, nil, int64(200019))
	require.NotNil(t, msg.Ack)
	msg.Ack()
	snap := c.cp.Snapshot()
	require.Equal(t, int64(200019), snap.OrderByValue)
	assert.Nil(t, snap.ChangeTime)

	got := c.buildReadQuery()
	assert.Contains(t, got, "WHERE `material_id` > 200019")
}

func TestClickHouseSourceConnector_applyInitialCheckpoint_legacy(t *testing.T) {
	opts := &SourceConnectorOptions{
		InitialCheckpoint: []byte(`{"lastReadID":100,"lastReadTime":"2024-06-01 12:00:00"}`),
	}
	c := NewClickHouseSourceConnectorWithOptions(&v1.ClickHouseSourceSpec{Table: "events"}, opts)
	got := c.buildReadQuery()
	assert.Contains(t, got, "WHERE (`created_at`, `id`) > ('2024-06-01 12:00:00', 100)")
}

func TestClickHouseSourceConnector_wrapQueryWithStableOrder(t *testing.T) {
	c := NewClickHouseSourceConnector(&v1.ClickHouseSourceSpec{
		ConnectionString: "clickhouse://localhost:9000",
		Table:            "t",
		OrderByColumn:    "price_id",
	})
	got := c.wrapQueryWithStableOrder("SELECT 1")
	assert.Contains(t, got, "ORDER BY created_at, price_id")
}
