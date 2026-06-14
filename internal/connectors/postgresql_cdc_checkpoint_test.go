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
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresCDCCheckpointHolder_advanceMonotonic(t *testing.T) {
	t.Parallel()
	var h postgresCDCCheckpointHolder
	h.init(nil, "postgresql-cdc", "slot1", "pub1", []byte(`{"lastAckedLSN":"0/100"}`))

	lsn1, err := pglogrepl.ParseLSN("0/200")
	require.NoError(t, err)
	lsn2, err := pglogrepl.ParseLSN("0/300")
	require.NoError(t, err)
	lsnOld, err := pglogrepl.ParseLSN("0/100")
	require.NoError(t, err)

	h.advance(lsnOld)
	assert.Equal(t, "0/100", h.startLSN().String())

	h.advance(lsn1)
	assert.Equal(t, "0/200", h.startLSN().String())

	h.advance(lsn1)
	assert.Equal(t, "0/200", h.startLSN().String())

	h.advance(lsn2)
	assert.Equal(t, "0/300", h.startLSN().String())
}

func TestPostgresCDCCheckpointHolder_snapshotTables(t *testing.T) {
	t.Parallel()
	var h postgresCDCCheckpointHolder
	h.init(nil, "postgresql-cdc", "slot1", "pub1", nil)

	assert.False(t, h.allSnapshotTablesDone([]string{"public.a", "public.b"}))
	h.markSnapshotTableDone("public.a")
	assert.False(t, h.allSnapshotTablesDone([]string{"public.a", "public.b"}))
	h.markSnapshotTableDone("public.b")
	assert.True(t, h.allSnapshotTablesDone([]string{"public.a", "public.b"}))
}

func TestNormalizePostgreSQLTableRefs(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []string{"public.orders"}, normalizePostgreSQLTableRefs([]string{"orders"}))
	assert.Equal(t, []string{"custom.t"}, normalizePostgreSQLTableRefs([]string{"custom.t"}))
}

func TestReplicationConnectionString(t *testing.T) {
	t.Parallel()
	got := replicationConnectionString("postgres://u:p@host:5432/db?sslmode=disable")
	assert.Contains(t, got, "replication=database")
}

func TestPostgresCDCCheckpointHolder_makeAck(t *testing.T) {
	t.Parallel()
	var h postgresCDCCheckpointHolder
	h.init(nil, "postgresql-cdc", "slot1", "pub1", nil)

	lsn, err := pglogrepl.ParseLSN("0/500")
	require.NoError(t, err)
	h.makeAck(lsn)()
	assert.Equal(t, "0/500", h.startLSN().String())
}
