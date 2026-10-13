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
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type memCheckpoint struct {
	mu   sync.Mutex
	data []byte
}

func (m *memCheckpoint) Load(context.Context, string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.data...), nil
}

func (m *memCheckpoint) Save(_ context.Context, _ string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = append([]byte(nil), data...)
	return nil
}

func (m *memCheckpoint) Flush(context.Context) error { return nil }
func (m *memCheckpoint) Clear(context.Context, string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = nil
	return nil
}

type fakeHiveMetastore struct {
	mu             sync.Mutex
	table          hiveTableMeta
	missing        bool
	parts          map[string]hivePartitionMeta
	addCalls       int
	dupAdds        int
	hidePartitions bool
}

func (f *fakeHiveMetastore) GetTable(context.Context, string, string) (hiveTableMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.missing {
		return hiveTableMeta{}, &hiveMetastoreError{kind: "not_found", msg: "table not found"}
	}
	return f.table, nil
}

func (f *fakeHiveMetastore) GetPartitions(context.Context, string, string) ([]hivePartitionMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.hidePartitions {
		return nil, nil
	}
	out := make([]hivePartitionMeta, 0, len(f.parts))
	for _, part := range f.parts {
		out = append(out, part)
	}
	return out, nil
}

func (f *fakeHiveMetastore) AddPartition(_ context.Context, part hivePartitionMeta) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addCalls++
	key, err := hivePartitionPath([]hiveField{{Name: "dt", Type: "string"}}, part.Values)
	if err != nil {
		return err
	}
	if f.parts == nil {
		f.parts = map[string]hivePartitionMeta{}
	}
	if _, ok := f.parts[key]; ok {
		f.dupAdds++
		return &hiveMetastoreError{kind: "already_exists", msg: "partition exists"}
	}
	f.parts[key] = part
	f.missing = false
	return nil
}

func (f *fakeHiveMetastore) CreateTable(_ context.Context, table hiveTableMeta) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.missing {
		return &hiveMetastoreError{kind: "already_exists", msg: "table exists"}
	}
	f.table = table
	f.missing = false
	return nil
}

func (f *fakeHiveMetastore) Close() error { return nil }

func useFakeMetastore(t *testing.T, hms hiveMetastore) {
	t.Helper()
	prev := openHiveMetastore
	openHiveMetastore = func(context.Context, string) (hiveMetastore, error) { return hms, nil }
	t.Cleanup(func() { openHiveMetastore = prev })
}

func TestParseHiveLocation_nameservice(t *testing.T) {
	_, err := parseHiveLocation("hdfs://nameservice/warehouse/t")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nameservice")
}

func TestHiveSource_unpartitionedParquet(t *testing.T) {
	dir := t.TempDir()
	payload, err := encodeParquet([]hiveField{{Name: "id", Type: "bigint"}, {Name: "name", Type: "string"}}, []map[string]interface{}{
		{"id": int64(1), "name": "a"},
		{"id": int64(2), "name": "b"},
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "part.parquet"), payload, 0o644))

	hms := &fakeHiveMetastore{table: hiveTableMeta{
		DB: "default", Name: "events", Location: "file://" + dir,
		InputFormat: hiveParquetInputFormat, SerDe: hiveParquetSerDe,
		Columns: []hiveField{{Name: "id", Type: "bigint"}, {Name: "name", Type: "string"}},
	}}
	useFakeMetastore(t, hms)
	src := NewHiveSourceConnector(&v1.HiveSourceSpec{HiveEndpointSpec: v1.HiveEndpointSpec{
		MetastoreURI: "thrift://hive:9083", Database: "default", Table: "events",
	}})
	require.NoError(t, src.Connect(context.Background()))
	t.Cleanup(func() { _ = src.Close() })

	ch := make(chan *types.Message, 4)
	require.NoError(t, src.readOnce(context.Background(), ch))
	close(ch)
	var got []string
	for msg := range ch {
		obj, ok := msg.JSONObject()
		require.True(t, ok)
		got = append(got, obj["name"].(string))
	}
	assert.Equal(t, []string{"a", "b"}, got)
	require.ErrorIs(t, src.readOnce(context.Background(), make(chan *types.Message, 1)), ErrSourceExhausted)
}

func TestHiveSource_partitionCheckpointAndLimit(t *testing.T) {
	dir := t.TempDir()
	partDir := filepath.Join(dir, "dt=2024-01-01")
	require.NoError(t, os.MkdirAll(partDir, 0o755))
	payload, err := encodeParquet([]hiveField{{Name: "id", Type: "bigint"}}, []map[string]interface{}{
		{"id": int64(1)},
		{"id": int64(2)},
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(partDir, "part.parquet"), payload, 0o644))

	hms := &fakeHiveMetastore{
		table: hiveTableMeta{
			DB: "default", Name: "events", Location: "file://" + dir,
			InputFormat: hiveParquetInputFormat, SerDe: hiveParquetSerDe,
			Columns:       []hiveField{{Name: "id", Type: "bigint"}},
			PartitionKeys: []hiveField{{Name: "dt", Type: "string"}},
		},
		parts: map[string]hivePartitionMeta{
			"dt=2024-01-01": {DB: "default", Table: "events", Values: []string{"2024-01-01"}, Location: "file://" + partDir},
		},
	}
	useFakeMetastore(t, hms)
	limit := int32(1)
	store := &memCheckpoint{}
	src := NewHiveSourceConnectorWithOptions(&v1.HiveSourceSpec{
		HiveEndpointSpec: v1.HiveEndpointSpec{MetastoreURI: "thrift://hive:9083", Database: "default", Table: "events"},
		MaxRowsPerPoll:   &limit,
	}, &SourceConnectorOptions{CheckpointStore: store, SourceType: "hive"})
	require.NoError(t, src.Connect(context.Background()))
	t.Cleanup(func() { _ = src.Close() })

	readOne := func() *types.Message {
		t.Helper()
		ch := make(chan *types.Message, 2)
		require.NoError(t, src.readOnce(context.Background(), ch))
		select {
		case msg := <-ch:
			return msg
		default:
			t.Fatal("expected a row")
			return nil
		}
	}
	first := readOne()
	obj, ok := first.JSONObject()
	require.True(t, ok)
	assert.Equal(t, "2024-01-01", obj["dt"])
	require.NotNil(t, first.Ack)
	first.Ack()
	require.NotEmpty(t, store.data)

	second := readOne()
	obj, ok = second.JSONObject()
	require.True(t, ok)
	assert.Equal(t, int64(2), obj["id"])
	second.Ack()
	require.ErrorIs(t, src.readOnce(context.Background(), make(chan *types.Message, 1)), ErrSourceExhausted)
}

func TestHiveSource_orcRejected(t *testing.T) {
	hms := &fakeHiveMetastore{table: hiveTableMeta{
		DB: "default", Name: "events", Location: "file:///tmp",
		InputFormat: "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
		Columns:     []hiveField{{Name: "id", Type: "bigint"}},
	}}
	useFakeMetastore(t, hms)
	src := NewHiveSourceConnector(&v1.HiveSourceSpec{HiveEndpointSpec: v1.HiveEndpointSpec{
		MetastoreURI: "thrift://hive:9083", Database: "default", Table: "events",
	}})
	err := src.Connect(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ORC")
}

func TestHiveSink_partitionWriteIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	hms := &fakeHiveMetastore{table: hiveTableMeta{
		DB: "default", Name: "events", Location: "file://" + dir,
		InputFormat: hiveParquetInputFormat, SerDe: hiveParquetSerDe,
		Columns:       []hiveField{{Name: "id", Type: "bigint"}},
		PartitionKeys: []hiveField{{Name: "dt", Type: "string"}},
	}}
	useFakeMetastore(t, hms)
	raw := false
	sink := NewHiveSinkConnector(&v1.HiveSinkSpec{HiveEndpointSpec: v1.HiveEndpointSpec{
		MetastoreURI: "thrift://hive:9083", Database: "default", Table: "events",
	}, RawMode: &raw})
	require.NoError(t, sink.Connect(context.Background()))
	t.Cleanup(func() { _ = sink.Close() })

	msgs := []*types.Message{mustHiveMessage(t, map[string]interface{}{"id": 7, "dt": "2024-01-02"})}
	require.NoError(t, sink.flushBatch(context.Background(), msgs))
	hms.mu.Lock()
	hms.hidePartitions = true
	hms.mu.Unlock()
	require.NoError(t, sink.flushBatch(context.Background(), msgs))
	hms.mu.Lock()
	assert.Equal(t, 2, hms.addCalls)
	assert.Equal(t, 1, hms.dupAdds)
	hms.mu.Unlock()

	partDir := filepath.Join(dir, "dt=2024-01-02")
	entries, err := os.ReadDir(partDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	blob, err := os.ReadFile(filepath.Join(partDir, entries[0].Name()))
	require.NoError(t, err)
	rows, err := decodeParquetRows(blob)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(7), rows[0]["id"])
}

func TestHiveSink_autoCreateRaw(t *testing.T) {
	dir := t.TempDir()
	hms := &fakeHiveMetastore{missing: true}
	useFakeMetastore(t, hms)
	raw := true
	auto := true
	sink := NewHiveSinkConnector(&v1.HiveSinkSpec{
		HiveEndpointSpec: v1.HiveEndpointSpec{MetastoreURI: "thrift://hive:9083", Database: "default", Table: "events"},
		RawMode:          &raw,
		AutoCreateTable:  &auto,
		Location:         "file://" + dir,
	})
	require.NoError(t, sink.Connect(context.Background()))
	t.Cleanup(func() { _ = sink.Close() })
	msg := types.NewMessage([]byte(`{"id":1}`))
	require.NoError(t, sink.flushBatch(context.Background(), []*types.Message{msg}))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
}

func TestThriftHiveGetTableRoundTrip(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	want := hiveTableMeta{
		DB: "default", Name: "events", Location: "file:///tmp/events",
		InputFormat: hiveParquetInputFormat, OutputFormat: hiveParquetOutputFormat, SerDe: hiveParquetSerDe,
		Columns: []hiveField{{Name: "id", Type: "bigint"}}, TableType: "EXTERNAL_TABLE",
		Parameters: map[string]string{"EXTERNAL": "TRUE"},
	}
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conf := &thrift.TConfiguration{TBinaryStrictRead: thrift.BoolPtr(true), TBinaryStrictWrite: thrift.BoolPtr(true)}
		sock := thrift.NewTSocketFromConnConf(conn, conf)
		trans := thrift.NewTBufferedTransport(sock, 8192)
		p := thrift.NewTBinaryProtocolConf(trans, conf)
		ctx := context.Background()
		_, _, seq, err := p.ReadMessageBegin(ctx)
		if err != nil {
			return
		}
		if _, err := p.ReadStructBegin(ctx); err != nil {
			return
		}
		for {
			_, typ, _, err := p.ReadFieldBegin(ctx)
			if err != nil || typ == thrift.STOP {
				break
			}
			_ = skipThrift(ctx, p, typ)
			_ = p.ReadFieldEnd(ctx)
		}
		_ = p.ReadStructEnd(ctx)
		_ = p.ReadMessageEnd(ctx)
		_ = p.WriteMessageBegin(ctx, "get_table", thrift.REPLY, seq)
		_ = p.WriteStructBegin(ctx, "get_table_result")
		_ = writeTableField(ctx, p, "success", 0, want)
		_ = p.WriteFieldStop(ctx)
		_ = p.WriteStructEnd(ctx)
		_ = p.WriteMessageEnd(ctx)
		_ = trans.Flush(ctx)
	}()

	client, err := dialThriftHiveMetastore(context.Background(), "thrift://"+ln.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := client.GetTable(ctx, "default", "events")
	require.NoError(t, err)
	assert.Equal(t, want.Name, got.Name)
	assert.Equal(t, want.DB, got.DB)
	assert.Equal(t, want.Location, got.Location)
	assert.Equal(t, want.InputFormat, got.InputFormat)
	assert.Equal(t, want.SerDe, got.SerDe)
	require.Len(t, got.Columns, 1)
	assert.Equal(t, "id", got.Columns[0].Name)
	assert.Equal(t, "TRUE", got.Parameters["EXTERNAL"])
}

func mustHiveMessage(t *testing.T, obj map[string]interface{}) *types.Message {
	t.Helper()
	msg, err := types.FromJSON(obj)
	require.NoError(t, err)
	return msg
}
