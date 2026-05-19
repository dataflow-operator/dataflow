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

	"github.com/apache/iceberg-go/table"
	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func snapshot(id int64, parent *int64, seq int64) table.Snapshot {
	return table.Snapshot{
		SnapshotID:       id,
		ParentSnapshotID: parent,
		SequenceNumber:   seq,
	}
}

func makeSnapshotLookup(snaps map[int64]table.Snapshot) snapshotByIDFunc {
	return func(id int64) *table.Snapshot {
		s, ok := snaps[id]
		if !ok {
			return nil
		}
		out := s
		return &out
	}
}

func TestBuildSnapshotChain_linear(t *testing.T) {
	p1 := int64(1)
	p2 := int64(2)
	snaps := map[int64]table.Snapshot{
		1: snapshot(1, nil, 1),
		2: snapshot(2, &p1, 2),
		3: snapshot(3, &p2, 3),
	}
	cur := snaps[3]
	current := &cur
	after := int64(1)
	chain, found := buildSnapshotChain(current, makeSnapshotLookup(snaps), &after)
	require.True(t, found)
	require.Len(t, chain, 2)
	assert.Equal(t, int64(2), chain[0].SnapshotID)
	assert.Equal(t, int64(3), chain[1].SnapshotID)
}

func TestBuildSnapshotChain_noNewSnapshots(t *testing.T) {
	p1 := int64(1)
	snaps := map[int64]table.Snapshot{
		1: snapshot(1, nil, 1),
		2: snapshot(2, &p1, 2),
	}
	cur := snaps[2]
	current := &cur
	after := int64(2)
	chain, found := buildSnapshotChain(current, makeSnapshotLookup(snaps), &after)
	require.True(t, found)
	assert.Empty(t, chain)
}

func TestNessieSourceConnector_advanceCheckpoint_monotonic(t *testing.T) {
	trueVal := true
	cfg := &v1.NessieSourceSpec{
		BaseURL:               "http://localhost:19120",
		Namespace:             "ns",
		Table:                 "t",
		IncrementalBySnapshot: &trueVal,
	}
	c := NewNessieSourceConnectorWithOptions(cfg, nil)
	c.advanceCheckpoint(10, 1)
	c.advanceCheckpoint(11, 1)
	c.checkpointMu.Lock()
	assert.Equal(t, int64(10), c.lastAckedSnapshotID)
	assert.Equal(t, int64(1), c.lastAckedSnapshotSequence)
	c.checkpointMu.Unlock()

	c.advanceCheckpoint(12, 2)
	c.checkpointMu.Lock()
	assert.Equal(t, int64(12), c.lastAckedSnapshotID)
	assert.Equal(t, int64(2), c.lastAckedSnapshotSequence)
	c.checkpointMu.Unlock()
}

func TestNessieSourceConnector_applyInitialCheckpoint(t *testing.T) {
	data, err := json.Marshal(nessieSnapshotCheckpoint{
		LastAckedSnapshotID:       "42",
		LastAckedSnapshotSequence: 7,
	})
	require.NoError(t, err)
	cfg := &v1.NessieSourceSpec{BaseURL: "http://x", Namespace: "n", Table: "t"}
	c := NewNessieSourceConnector(cfg)
	c.applyInitialCheckpoint(data)
	c.checkpointMu.Lock()
	assert.Equal(t, int64(42), c.lastAckedSnapshotID)
	assert.Equal(t, int64(7), c.lastAckedSnapshotSequence)
	c.checkpointMu.Unlock()
}

func TestParseSnapshotIDString(t *testing.T) {
	id, err := parseSnapshotIDString("1234567890123456789")
	require.NoError(t, err)
	assert.Equal(t, int64(1234567890123456789), id)
	_, err = parseSnapshotIDString("-1")
	assert.Error(t, err)
}
