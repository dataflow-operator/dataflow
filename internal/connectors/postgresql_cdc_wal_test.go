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
	"encoding/binary"
	"encoding/json"
	"testing"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var walBigEndian = binary.BigEndian

func walPutString(dst []byte, value string) int {
	copy(dst, value)
	dst[len(value)] = 0
	return len(value) + 1
}

func walTupleColumnLen(dataType byte, data []byte) int {
	switch dataType {
	case 'n', 'u':
		return 1
	case 't':
		return 1 + 4 + len(data)
	default:
		panic("unsupported tuple column type")
	}
}

func walPutTupleColumn(dst []byte, dataType byte, data []byte) int {
	dst[0] = dataType
	switch dataType {
	case 'n', 'u':
		return 1
	case 't':
		walBigEndian.PutUint32(dst[1:], uint32(len(data)))
		copy(dst[5:], data)
		return 5 + len(data)
	default:
		panic("unsupported tuple column type")
	}
}

// walOrdersRelationFixture builds a RelationMessage ('R') WAL frame for public.orders (id int8, name text).
func walOrdersRelationFixture(relationID uint32) []byte {
	namespace := "public"
	relationName := "orders"
	col1 := "id"
	col2 := "name"
	noAtttypmod := int32(-1)

	col1Len := 1 + len(col1) + 1 + 4 + 4
	col2Len := 1 + len(col2) + 1 + 4 + 4
	msg := make([]byte, 1+4+len(namespace)+1+len(relationName)+1+1+2+col1Len+col2Len)
	msg[0] = 'R'
	off := 1
	walBigEndian.PutUint32(msg[off:], relationID)
	off += 4
	off += walPutString(msg[off:], namespace)
	off += walPutString(msg[off:], relationName)
	msg[off] = 1 // REPLICA IDENTITY DEFAULT
	off++
	walBigEndian.PutUint16(msg[off:], 2)
	off += 2

	msg[off] = 1 // id is key column
	off++
	off += walPutString(msg[off:], col1)
	walBigEndian.PutUint32(msg[off:], 20) // int8
	off += 4
	walBigEndian.PutUint32(msg[off:], uint32(noAtttypmod))
	off += 4

	msg[off] = 0
	off++
	off += walPutString(msg[off:], col2)
	walBigEndian.PutUint32(msg[off:], 25) // text
	off += 4
	walBigEndian.PutUint32(msg[off:], uint32(noAtttypmod))

	return msg
}

func walInsertFixture(relationID uint32, id, name string) []byte {
	col1 := []byte(id)
	col2 := []byte(name)
	col1Len := walTupleColumnLen('t', col1)
	col2Len := walTupleColumnLen('t', col2)
	msg := make([]byte, 1+4+1+2+col1Len+col2Len)
	msg[0] = 'I'
	off := 1
	walBigEndian.PutUint32(msg[off:], relationID)
	off += 4
	msg[off] = 'N'
	off++
	walBigEndian.PutUint16(msg[off:], 2)
	off += 2
	off += walPutTupleColumn(msg[off:], 't', col1)
	walPutTupleColumn(msg[off:], 't', col2)
	return msg
}

func walUpdateFixtureTypeO(relationID uint32, oldID, oldName, newID, newName string) []byte {
	oldCol1 := []byte(oldID)
	oldCol2 := []byte(oldName)
	newCol1 := []byte(newID)
	newCol2 := []byte(newName)
	oldCol1Len := walTupleColumnLen('t', oldCol1)
	oldCol2Len := walTupleColumnLen('t', oldCol2)
	newCol1Len := walTupleColumnLen('t', newCol1)
	newCol2Len := walTupleColumnLen('t', newCol2)
	msg := make([]byte, 1+4+1+2+oldCol1Len+oldCol2Len+1+2+newCol1Len+newCol2Len)
	msg[0] = 'U'
	off := 1
	walBigEndian.PutUint32(msg[off:], relationID)
	off += 4
	msg[off] = 'O'
	off++
	walBigEndian.PutUint16(msg[off:], 2)
	off += 2
	off += walPutTupleColumn(msg[off:], 't', oldCol1)
	off += walPutTupleColumn(msg[off:], 't', oldCol2)
	msg[off] = 'N'
	off++
	walBigEndian.PutUint16(msg[off:], 2)
	off += 2
	off += walPutTupleColumn(msg[off:], 't', newCol1)
	walPutTupleColumn(msg[off:], 't', newCol2)
	return msg
}

func walDeleteFixtureTypeO(relationID uint32, id, name string) []byte {
	col1 := []byte(id)
	col2 := []byte(name)
	col1Len := walTupleColumnLen('t', col1)
	col2Len := walTupleColumnLen('t', col2)
	msg := make([]byte, 1+4+1+2+col1Len+col2Len)
	msg[0] = 'D'
	off := 1
	walBigEndian.PutUint32(msg[off:], relationID)
	off += 4
	msg[off] = 'O'
	off++
	walBigEndian.PutUint16(msg[off:], 2)
	off += 2
	off += walPutTupleColumn(msg[off:], 't', col1)
	walPutTupleColumn(msg[off:], 't', col2)
	return msg
}

func walCommitFixture(commitLSN pglogrepl.LSN) []byte {
	msg := make([]byte, 1+1+8+8+8)
	msg[0] = 'C'
	msg[1] = 0
	walBigEndian.PutUint64(msg[2:], uint64(commitLSN))
	walBigEndian.PutUint64(msg[10:], uint64(commitLSN))
	return msg
}

func TestProcessWALData_decodeInsertUpdateDelete(t *testing.T) {
	t.Parallel()

	const relationID uint32 = 42
	commitLSN, err := pglogrepl.ParseLSN("0/16B3748")
	require.NoError(t, err)

	cfg := &v1.PostgreSQLCDCSourceSpec{
		SlotName:        "test_slot",
		PublicationName: "test_pub",
		Tables:          []string{"public.orders"},
		SnapshotMode:    "never",
	}
	source := NewPostgreSQLCDCSourceConnector(cfg)

	ctx := context.Background()
	msgChan := make(chan *types.Message, 8)
	var inStream bool
	tables := cfg.Tables
	pkCol := "id"
	var txnMessages []*types.Message
	var txnCommitLSN pglogrepl.LSN

	process := func(wal []byte) {
		t.Helper()
		require.NoError(t, source.processWALData(ctx, msgChan, wal, &inStream, tables, pkCol, &txnMessages, &txnCommitLSN))
	}

	process(walOrdersRelationFixture(relationID))
	process(walInsertFixture(relationID, "1", "alice"))
	process(walUpdateFixtureTypeO(relationID, "1", "alice", "1", "alice2"))
	process(walDeleteFixtureTypeO(relationID, "1", "alice2"))
	process(walCommitFixture(commitLSN))

	var messages []*types.Message
	for {
		select {
		case msg := <-msgChan:
			messages = append(messages, msg)
		default:
			goto done
		}
	}
done:
	require.Len(t, messages, 3)

	assert.Equal(t, "insert", messages[0].Metadata["operation"])
	assert.Equal(t, "public.orders", messages[0].Metadata["table"])
	var insertRow map[string]interface{}
	require.NoError(t, json.Unmarshal(messages[0].Data, &insertRow))
	assert.Equal(t, float64(1), insertRow["id"])
	assert.Equal(t, "alice", insertRow["name"])

	assert.Equal(t, "update", messages[1].Metadata["operation"])
	var updateRow map[string]interface{}
	require.NoError(t, json.Unmarshal(messages[1].Data, &updateRow))
	assert.Equal(t, "alice2", updateRow["name"])

	assert.Equal(t, "delete", messages[2].Metadata["operation"])
	var deleteRow map[string]interface{}
	require.NoError(t, json.Unmarshal(messages[2].Data, &deleteRow))
	assert.Equal(t, float64(1), deleteRow["id"])
	assert.Equal(t, "alice2", deleteRow["name"])

	require.NotNil(t, messages[2].Ack)
	messages[2].Ack()
	assert.Equal(t, commitLSN.String(), source.cp.startLSN().String())
}

func TestParseV2_walFixtures(t *testing.T) {
	t.Parallel()

	const relationID uint32 = 7

	relMsg, err := pglogrepl.ParseV2(walOrdersRelationFixture(relationID), false)
	require.NoError(t, err)
	rel, ok := relMsg.(*pglogrepl.RelationMessageV2)
	require.True(t, ok)
	assert.Equal(t, relationID, rel.RelationID)
	assert.Equal(t, "public", rel.Namespace)
	assert.Equal(t, "orders", rel.RelationName)

	insertMsg, err := pglogrepl.ParseV2(walInsertFixture(relationID, "42", "hello"), false)
	require.NoError(t, err)
	insert, ok := insertMsg.(*pglogrepl.InsertMessageV2)
	require.True(t, ok)
	assert.Equal(t, relationID, insert.RelationID)

	updateMsg, err := pglogrepl.ParseV2(walUpdateFixtureTypeO(relationID, "1", "old", "1", "new"), false)
	require.NoError(t, err)
	_, ok = updateMsg.(*pglogrepl.UpdateMessageV2)
	require.True(t, ok)

	deleteMsg, err := pglogrepl.ParseV2(walDeleteFixtureTypeO(relationID, "1", "gone"), false)
	require.NoError(t, err)
	_, ok = deleteMsg.(*pglogrepl.DeleteMessageV2)
	require.True(t, ok)
}
