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
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/dataflow-operator/dataflow/pkg/sinkbatch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stub SQL driver: successful Exec path, Commit can fail to prove Ack is not called from flush*.
type chAckFailDriver struct {
	failCommit bool
}

func (d *chAckFailDriver) Open(string) (driver.Conn, error) {
	return &chAckFailConn{failCommit: d.failCommit}, nil
}

type chAckFailConn struct {
	failCommit bool
}

func (c *chAckFailConn) Prepare(string) (driver.Stmt, error) { return &chAckFailStmt{}, nil }
func (c *chAckFailConn) Close() error                        { return nil }
func (c *chAckFailConn) Begin() (driver.Tx, error) {
	return &chAckFailTx{failCommit: c.failCommit}, nil
}

type chAckFailTx struct {
	failCommit bool
}

func (t *chAckFailTx) Commit() error {
	if t.failCommit {
		return errors.New("commit failed")
	}
	return nil
}
func (t *chAckFailTx) Rollback() error { return nil }

type chAckFailStmt struct{}

func (s *chAckFailStmt) Close() error  { return nil }
func (s *chAckFailStmt) NumInput() int { return -1 }
func (s *chAckFailStmt) Exec(args []driver.Value) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}
func (s *chAckFailStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("query not supported")
}

// Satisfy optional interfaces used by database/sql with context.
func (c *chAckFailConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return c.Begin()
}
func (c *chAckFailConn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	return c.Prepare(query)
}
func (s *chAckFailStmt) ExecContext(_ context.Context, args []driver.NamedValue) (driver.Result, error) {
	vals := make([]driver.Value, len(args))
	for i, a := range args {
		vals[i] = a.Value
	}
	return s.Exec(vals)
}
func (s *chAckFailStmt) QueryContext(context.Context, []driver.NamedValue) (driver.Rows, error) {
	return nil, errors.New("query not supported")
}

var _ driver.Rows = (*emptyRows)(nil)

type emptyRows struct{}

func (*emptyRows) Columns() []string          { return nil }
func (*emptyRows) Close() error               { return nil }
func (*emptyRows) Next([]driver.Value) error  { return io.EOF }
func (*emptyRows) ColumnTypeScanType(int) any { return nil }
func (*emptyRows) ColumnTypeDatabaseTypeName(int) string {
	return ""
}

func openClickHouseAckTestDB(t *testing.T, failCommit bool) *sql.DB {
	t.Helper()
	name := "ch_ack_test_" + t.Name()
	sql.Register(name, &chAckFailDriver{failCommit: failCommit})
	db, err := sql.Open(name, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestClickHouseFlushBatchRaw_DoesNotAckBeforeCommit(t *testing.T) {
	c := NewClickHouseSinkConnector(&v1.ClickHouseSinkSpec{Table: "t"})
	c.conn = openClickHouseAckTestDB(t, true)

	var acked atomic.Int32
	msg := types.NewMessage([]byte(`{"id":1}`))
	msg.Ack = func() { acked.Add(1) }

	err := c.flushBatchRaw(context.Background(), []*types.Message{msg})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit failed")
	assert.Equal(t, int32(0), acked.Load(), "Ack must not run before successful Commit")
}

func TestClickHouseFlushBatchRaw_DoesNotAckInlineOnSuccess(t *testing.T) {
	c := NewClickHouseSinkConnector(&v1.ClickHouseSinkSpec{Table: "t"})
	c.conn = openClickHouseAckTestDB(t, false)

	var acked atomic.Int32
	msg := types.NewMessage([]byte(`{"id":1}`))
	msg.Ack = func() { acked.Add(1) }

	err := c.flushBatchRaw(context.Background(), []*types.Message{msg})
	require.NoError(t, err)
	assert.Equal(t, int32(0), acked.Load(), "Ack belongs to OnAck after flush, not flushBatchRaw")
}

func TestClickHouseFlushBatchColumnar_DoesNotAckInline(t *testing.T) {
	c := NewClickHouseSinkConnector(&v1.ClickHouseSinkSpec{Table: "t"})
	c.conn = openClickHouseAckTestDB(t, false)

	var acked atomic.Int32
	msg := types.NewMessage([]byte(`{"id":1,"name":"a"}`))
	msg.Ack = func() { acked.Add(1) }

	err := c.flushBatchColumnar(context.Background(), []*types.Message{msg})
	require.NoError(t, err)
	assert.Equal(t, int32(0), acked.Load())
}

func TestNewBatchWriteConfig_ClickHouseDefault500(t *testing.T) {
	cfg := NewBatchWriteConfig(nil, nil, int(sinkbatch.DefaultClickHouseBatchSize))
	assert.Equal(t, int(sinkbatch.DefaultClickHouseBatchSize), cfg.MaxBatchSize)
}

func TestNewBatchWriteConfig_TrinoDefault10(t *testing.T) {
	cfg := NewBatchWriteConfig(nil, nil, int(sinkbatch.DefaultTrinoBatchSize))
	assert.Equal(t, int(sinkbatch.DefaultTrinoBatchSize), cfg.MaxBatchSize)
}
