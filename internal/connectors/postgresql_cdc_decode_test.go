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
	"encoding/json"
	"testing"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresCDCColumnFilter(t *testing.T) {
	t.Parallel()
	cfg := &v1.PostgreSQLCDCSourceSpec{
		IncludeColumns: []string{"id", "name"},
		ExcludeColumns: []string{"secret"},
	}
	f := newPostgresCDCColumnFilter(cfg)
	assert.True(t, f.keep("id"))
	assert.True(t, f.keep("name"))
	assert.False(t, f.keep("value"))
	assert.False(t, f.keep("secret"))
}

func TestNormalizeCDCDecodedValue_numeric(t *testing.T) {
	t.Parallel()
	var num pgtype.Numeric
	require.NoError(t, num.Scan("123.45"))
	assert.InDelta(t, 123.45, normalizeCDCDecodedValue(num).(float64), 0.001)
}

func TestTupleToRow_toastUnchangedOmitted(t *testing.T) {
	t.Parallel()
	rel := &pglogrepl.RelationMessageV2{
		RelationMessage: pglogrepl.RelationMessage{
			Namespace:    "public",
			RelationName: "docs",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "id", DataType: 20},
				{Name: "body", DataType: 25},
			},
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("1")},
			{DataType: 'u'}, // unchanged TOAST column
		},
	}
	row, err := tupleToRow(rel, tuple, pgtype.NewMap(), nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), row["id"])
	_, hasBody := row["body"]
	assert.False(t, hasBody, "unchanged TOAST column should be omitted")
}

func TestTupleToRow_pgTypes(t *testing.T) {
	t.Parallel()
	typeMap := pgtype.NewMap()
	rel := &pglogrepl.RelationMessageV2{
		RelationMessage: pglogrepl.RelationMessage{
			Namespace:    "public",
			RelationName: "typed",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "uid", DataType: 2950},    // uuid
				{Name: "meta", DataType: 3802},   // jsonb
				{Name: "amount", DataType: 1700}, // numeric
				{Name: "payload", DataType: 17},  // bytea
			},
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("550e8400-e29b-41d4-a716-446655440000")},
			{DataType: 't', Data: []byte(`{"k":"v"}`)},
			{DataType: 't', Data: []byte("123.45")},
			{DataType: 't', Data: []byte("\\xdeadbeef")},
		},
	}
	row, err := tupleToRow(rel, tuple, typeMap, nil)
	require.NoError(t, err)
	require.Len(t, row, 4)
	assert.NotEmpty(t, row["uid"])
	assert.NotNil(t, row["meta"])
	assert.InDelta(t, 123.45, row["amount"].(float64), 0.001)
	assert.NotNil(t, row["payload"])
}

func TestTupleToRow_columnFilter(t *testing.T) {
	t.Parallel()
	rel := &pglogrepl.RelationMessageV2{
		RelationMessage: pglogrepl.RelationMessage{
			Namespace:    "public",
			RelationName: "orders",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "id", DataType: 20},
				{Name: "secret", DataType: 25},
				{Name: "name", DataType: 25},
			},
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("1")},
			{DataType: 't', Data: []byte("hidden")},
			{DataType: 't', Data: []byte("visible")},
		},
	}
	filter := newPostgresCDCColumnFilter(&v1.PostgreSQLCDCSourceSpec{
		IncludeColumns: []string{"id", "name"},
		ExcludeColumns: []string{"secret"},
	})
	row, err := tupleToRow(rel, tuple, pgtype.NewMap(), filter)
	require.NoError(t, err)
	assert.Equal(t, int64(1), row["id"])
	assert.Equal(t, "visible", row["name"])
	_, hasSecret := row["secret"]
	assert.False(t, hasSecret)
}

func TestTupleToRow_basic(t *testing.T) {
	t.Parallel()
	rel := &pglogrepl.RelationMessageV2{
		RelationMessage: pglogrepl.RelationMessage{
			Namespace:    "public",
			RelationName: "orders",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "id", DataType: 20},
				{Name: "name", DataType: 25},
			},
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("42")},
			{DataType: 't', Data: []byte("hello")},
		},
	}
	row, err := tupleToRow(rel, tuple, pgtype.NewMap(), nil)
	require.NoError(t, err)
	assert.Equal(t, int64(42), row["id"])
	assert.Equal(t, "hello", row["name"])
}

func TestTableInConfig(t *testing.T) {
	t.Parallel()
	tables := []string{"public.orders", "public.customers"}
	assert.True(t, tableInConfig("public.orders", tables))
	assert.False(t, tableInConfig("public.other", tables))
}

func TestDebeziumOp(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "c", debeziumOp("insert", false))
	assert.Equal(t, "u", debeziumOp("update", false))
	assert.Equal(t, "d", debeziumOp("delete", false))
	assert.Equal(t, "r", debeziumOp("insert", true))
}

func TestRelationSchemaChanged(t *testing.T) {
	t.Parallel()
	base := &pglogrepl.RelationMessageV2{
		RelationMessage: pglogrepl.RelationMessage{
			Namespace:    "public",
			RelationName: "orders",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "id", DataType: 20},
				{Name: "name", DataType: 25},
			},
		},
	}
	assert.False(t, relationSchemaChanged(base, base))

	addedColumn := &pglogrepl.RelationMessageV2{
		RelationMessage: pglogrepl.RelationMessage{
			Namespace:    "public",
			RelationName: "orders",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "id", DataType: 20},
				{Name: "name", DataType: 25},
				{Name: "value", DataType: 23},
			},
		},
	}
	assert.True(t, relationSchemaChanged(base, addedColumn))

	renamedColumn := &pglogrepl.RelationMessageV2{
		RelationMessage: pglogrepl.RelationMessage{
			Namespace:    "public",
			RelationName: "orders",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "id", DataType: 20},
				{Name: "title", DataType: 25},
			},
		},
	}
	assert.True(t, relationSchemaChanged(base, renamedColumn))
}

func TestPostgresCDCRelationCacheSchemaRefresh(t *testing.T) {
	t.Parallel()
	cache := newPostgresCDCRelationCache()
	rel := &pglogrepl.RelationMessageV2{
		RelationMessage: pglogrepl.RelationMessage{
			RelationID:   1,
			Namespace:    "public",
			RelationName: "orders",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "id", DataType: 20},
			},
		},
	}
	assert.False(t, cache.put(rel))

	updated := &pglogrepl.RelationMessageV2{
		RelationMessage: pglogrepl.RelationMessage{
			RelationID:   1,
			Namespace:    "public",
			RelationName: "orders",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "id", DataType: 20},
				{Name: "name", DataType: 25},
			},
		},
	}
	assert.True(t, cache.put(updated))

	got, ok := cache.get(1)
	require.True(t, ok)
	require.Len(t, got.Columns, 2)
}

func TestBuildDebeziumEnvelope(t *testing.T) {
	t.Parallel()
	after := map[string]interface{}{"id": int64(1), "name": "alice2"}
	before := map[string]interface{}{"id": int64(1), "name": "alice"}
	envelope := buildDebeziumEnvelope(after, before, "public.users", "update", 0, false)
	payload, ok := envelope["payload"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "u", payload["op"])
	assert.Equal(t, before, payload["before"])
	assert.Equal(t, after, payload["after"])
	source, ok := payload["source"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "public", source["schema"])
	assert.Equal(t, "users", source["table"])

	snapshot := buildDebeziumEnvelope(after, nil, "public.users", "insert", 0, true)
	snapshotPayload := snapshot["payload"].(map[string]interface{})
	assert.Equal(t, "r", snapshotPayload["op"])
	assert.Nil(t, snapshotPayload["before"])

	deleteEnvelope := buildDebeziumEnvelope(nil, before, "public.users", "delete", 0, false)
	deletePayload := deleteEnvelope["payload"].(map[string]interface{})
	assert.Equal(t, "d", deletePayload["op"])
	assert.Nil(t, deletePayload["after"])
	assert.Equal(t, before, deletePayload["before"])
}

func TestBuildCDCMessageDebeziumUpdate(t *testing.T) {
	t.Parallel()
	cfg := &v1.PostgreSQLCDCSourceSpec{
		SlotName:        "slot",
		PublicationName: "pub",
		Tables:          []string{"public.orders"},
		EnvelopeFormat:  "debezium",
	}
	source := NewPostgreSQLCDCSourceConnector(cfg)
	msg, err := source.buildCDCMessage(
		map[string]interface{}{"id": int64(1), "name": "new"},
		map[string]interface{}{"id": int64(1), "name": "old"},
		"public.orders",
		"update",
		0,
		"id",
		false,
	)
	require.NoError(t, err)
	require.NotNil(t, msg)

	var envelope map[string]interface{}
	require.NoError(t, json.Unmarshal(msg.Data, &envelope))
	payload := envelope["payload"].(map[string]interface{})
	assert.Equal(t, "u", payload["op"])
	assert.Equal(t, "update", msg.Metadata["operation"])
}
