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
	"testing"

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
