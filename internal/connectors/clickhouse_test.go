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

func TestNewClickHouseSinkConnector_WithBatchFlushInterval(t *testing.T) {
	// Default: both batch size and timer
	spec := &v1.ClickHouseSinkSpec{
		ConnectionString: "clickhouse://localhost:9000",
		Table:            "out",
	}
	conn := NewClickHouseSinkConnector(spec)
	require.NotNil(t, conn)
	assert.Equal(t, spec, conn.config)

	// Timer only (batchSize 0)
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

	// Size only (timer 0)
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

func TestClickHouseSinkConnector_rawMode(t *testing.T) {
	falseVal, trueVal := false, true
	tests := []struct {
		name   string
		spec   *v1.ClickHouseSinkSpec
		expect bool
	}{
		{"nil", &v1.ClickHouseSinkSpec{}, false},
		{"false", &v1.ClickHouseSinkSpec{RawMode: &falseVal}, false},
		{"true", &v1.ClickHouseSinkSpec{RawMode: &trueVal}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewClickHouseSinkConnector(tt.spec)
			assert.Equal(t, tt.expect, c.rawMode())
		})
	}
}

func TestInferClickHouseType(t *testing.T) {
	tests := []struct {
		v    interface{}
		want string
	}{
		{nil, "Nullable(String)"},
		{true, "UInt8"},
		// float64 value-based: whole numbers by range
		{float64(0), "UInt8"},
		{float64(1), "UInt8"},
		{float64(255), "UInt8"},
		{float64(256), "UInt16"},
		{float64(65535), "UInt16"},
		{float64(65536), "UInt32"},
		{float64(100), "UInt8"}, // 100 <= 255
		{float64(1000), "UInt16"},
		{float64(-1), "Int8"},
		{float64(-128), "Int8"},
		{float64(-129), "Int16"},
		{float64(32767), "UInt16"}, // positive whole: 32767 <= 65535
		{float64(-32768), "Int16"},
		{float64(-32769), "Int32"},
		// float64 with decimals
		{float64(99.99), "Decimal(10, 2)"},
		{float64(1.5), "Decimal(10, 2)"}, // has at most 2 decimal places
		{float64(99.999), "Float64"},
		// RFC3339 string -> DateTime
		{"2026-03-05T12:27:38Z", "DateTime"},
		{"2026-01-02 15:04:05", "DateTime"},
		{"hello", "String"},
		// Native types
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
