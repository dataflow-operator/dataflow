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
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldRunSnapshot(t *testing.T) {
	t.Parallel()

	tables := []string{"public.orders"}

	t.Run("never", func(t *testing.T) {
		t.Parallel()
		cfg := &v1.PostgreSQLCDCSourceSpec{SnapshotMode: "never", Tables: tables}
		c := NewPostgreSQLCDCSourceConnector(cfg)
		assert.False(t, c.shouldRunSnapshot())
	})

	t.Run("always", func(t *testing.T) {
		t.Parallel()
		cfg := &v1.PostgreSQLCDCSourceSpec{SnapshotMode: "always", Tables: tables}
		c := NewPostgreSQLCDCSourceConnector(cfg)
		c.cp.markSnapshotTableDone("public.orders")
		assert.True(t, c.shouldRunSnapshot())
	})

	t.Run("initial empty checkpoint", func(t *testing.T) {
		t.Parallel()
		cfg := &v1.PostgreSQLCDCSourceSpec{SnapshotMode: "initial", Tables: tables}
		c := NewPostgreSQLCDCSourceConnector(cfg)
		assert.True(t, c.shouldRunSnapshot())
	})

	t.Run("initial all tables done skips snapshot", func(t *testing.T) {
		t.Parallel()
		cfg := &v1.PostgreSQLCDCSourceSpec{SnapshotMode: "initial", Tables: tables}
		c := NewPostgreSQLCDCSourceConnector(cfg)
		lsn, err := pglogrepl.ParseLSN("0/100")
		require.NoError(t, err)
		c.cp.advance(lsn)
		c.cp.markSnapshotTableDone("public.orders")
		assert.False(t, c.shouldRunSnapshot())
	})

	t.Run("initial ack during partial snapshot still resumes", func(t *testing.T) {
		t.Parallel()
		cfg := &v1.PostgreSQLCDCSourceSpec{SnapshotMode: "initial", Tables: []string{"public.a", "public.b"}}
		c := NewPostgreSQLCDCSourceConnector(cfg)
		c.cp.setPhase(postgresCDCPhaseSnapshot)
		lsn, err := pglogrepl.ParseLSN("0/100")
		require.NoError(t, err)
		c.cp.advance(lsn)
		assert.True(t, c.cp.phaseSnapshot())
		c.cp.markSnapshotTableDone("public.a")
		assert.True(t, c.shouldRunSnapshot())
	})

	t.Run("initial partial snapshot resume", func(t *testing.T) {
		t.Parallel()
		cfg := &v1.PostgreSQLCDCSourceSpec{SnapshotMode: "initial", Tables: []string{"public.a", "public.b"}}
		c := NewPostgreSQLCDCSourceConnector(cfg)
		c.cp.markSnapshotTableDone("public.a")
		assert.True(t, c.shouldRunSnapshot())
	})

	t.Run("initial all tables snapshotted", func(t *testing.T) {
		t.Parallel()
		cfg := &v1.PostgreSQLCDCSourceSpec{SnapshotMode: "initial", Tables: []string{"public.a", "public.b"}}
		c := NewPostgreSQLCDCSourceConnector(cfg)
		c.cp.markSnapshotTableDone("public.a")
		c.cp.markSnapshotTableDone("public.b")
		assert.False(t, c.shouldRunSnapshot())
	})
}

func TestPostgresCDCSnapshotModeDefault(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "initial", postgresCDCSnapshotMode(nil))
	assert.Equal(t, "initial", postgresCDCSnapshotMode(&v1.PostgreSQLCDCSourceSpec{}))
}

func TestHeartbeatInterval(t *testing.T) {
	t.Parallel()
	sec := int32(5)
	cfg := &v1.PostgreSQLCDCSourceSpec{HeartbeatIntervalSeconds: &sec}
	c := NewPostgreSQLCDCSourceConnector(cfg)
	assert.Equal(t, 5*time.Second, c.heartbeatInterval())

	disabled := int32(0)
	cfgDisabled := &v1.PostgreSQLCDCSourceSpec{HeartbeatIntervalSeconds: &disabled}
	cDisabled := NewPostgreSQLCDCSourceConnector(cfgDisabled)
	assert.Equal(t, time.Duration(0), cDisabled.heartbeatInterval())

	cDefault := NewPostgreSQLCDCSourceConnector(&v1.PostgreSQLCDCSourceSpec{})
	assert.Equal(t, 10*time.Second, cDefault.heartbeatInterval())
}

func TestPostgresCDCCheckpointHolder_startLSN(t *testing.T) {
	t.Parallel()
	var h postgresCDCCheckpointHolder
	h.init(nil, "postgresql-cdc", "slot1", "pub1", nil)

	snapLSN, err := pglogrepl.ParseLSN("0/200")
	require.NoError(t, err)
	h.setSnapshotLSN(snapLSN)
	assert.Equal(t, "0/200", h.startLSN().String())

	ackLSN, err := pglogrepl.ParseLSN("0/300")
	require.NoError(t, err)
	h.advance(ackLSN)
	assert.Equal(t, "0/300", h.startLSN().String())
}

func TestPostgresCDCCheckpointHolder_resetSnapshotProgress(t *testing.T) {
	t.Parallel()
	var h postgresCDCCheckpointHolder
	h.init(nil, "postgresql-cdc", "slot1", "pub1", nil)
	h.markSnapshotTableDone("public.a")
	h.setPhase(postgresCDCPhaseSnapshot)

	h.resetSnapshotProgress()
	assert.Empty(t, h.snapshotTablesDone())
	assert.False(t, h.phaseSnapshot())
}
