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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go/table"
	v1 "github.com/dataflow-operator/dataflow/api/v1"
)

// nessieSnapshotCheckpoint is persisted under checkpoint store key "nessie".
type nessieSnapshotCheckpoint struct {
	LastAckedSnapshotID       string `json:"lastAckedSnapshotID,omitempty"`
	LastAckedSnapshotSequence int64  `json:"lastAckedSnapshotSequence,omitempty"`
	Branch                    string `json:"branch,omitempty"`
	Namespace                 string `json:"namespace,omitempty"`
	Table                     string `json:"table,omitempty"`
}

func nessieIncrementalEnabled(cfg *v1.NessieSourceSpec) bool {
	return cfg != nil && cfg.IncrementalBySnapshot != nil && *cfg.IncrementalBySnapshot
}

func nessieSnapshotCheckpointsEnabled(cfg *v1.NessieSourceSpec) bool {
	if cfg == nil || cfg.SnapshotCheckpoints == nil {
		return true
	}
	return *cfg.SnapshotCheckpoints
}

func parseSnapshotIDString(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty snapshot ID")
	}
	u, err := strconv.ParseUint(s, 10, 63)
	if err != nil {
		return 0, fmt.Errorf("invalid snapshot ID %q: %w", s, err)
	}
	return int64(u), nil
}

func formatSnapshotID(id int64) string {
	return strconv.FormatInt(id, 10)
}

func (c *NessieSourceConnector) applyInitialCheckpoint(data []byte) {
	var cp nessieSnapshotCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return
	}
	c.checkpointMu.Lock()
	defer c.checkpointMu.Unlock()
	if cp.LastAckedSnapshotID != "" {
		if id, err := parseSnapshotIDString(cp.LastAckedSnapshotID); err == nil {
			c.lastAckedSnapshotID = id
		}
	}
	if cp.LastAckedSnapshotSequence > c.lastAckedSnapshotSequence {
		c.lastAckedSnapshotSequence = cp.LastAckedSnapshotSequence
	}
}

func (c *NessieSourceConnector) marshalCheckpointLocked() []byte {
	branch := c.config.Branch
	if branch == "" {
		branch = "main"
	}
	cp := nessieSnapshotCheckpoint{
		LastAckedSnapshotID:       formatSnapshotID(c.lastAckedSnapshotID),
		LastAckedSnapshotSequence: c.lastAckedSnapshotSequence,
		Branch:                    branch,
		Namespace:                 c.config.Namespace,
		Table:                     c.config.Table,
	}
	out, _ := json.Marshal(cp)
	return out
}

// advanceCheckpoint updates the last acked snapshot when sequence increases.
func (c *NessieSourceConnector) advanceCheckpoint(snapshotID int64, sequence int64) {
	c.checkpointMu.Lock()
	if sequence <= c.lastAckedSnapshotSequence {
		c.checkpointMu.Unlock()
		return
	}
	c.lastAckedSnapshotID = snapshotID
	c.lastAckedSnapshotSequence = sequence
	persist := c.checkpointStore != nil && nessieSnapshotCheckpointsEnabled(c.config)
	var data []byte
	if persist {
		data = c.marshalCheckpointLocked()
	}
	c.checkpointMu.Unlock()

	if persist && len(data) > 0 {
		sourceType := c.sourceType
		if sourceType == "" {
			sourceType = "nessie"
		}
		err := c.checkpointStore.Save(context.Background(), sourceType, data)
		reportCheckpointSaveError(c.logger, &c.connectorMetadata, sourceType, err)
	}
}

type snapshotByIDFunc func(int64) *table.Snapshot

// buildSnapshotChain returns snapshots to read: descendants of afterID up to and including current.
// Snapshots are ordered from oldest to newest. foundAfter is false when afterID is set but not in lineage.
func buildSnapshotChain(current *table.Snapshot, lookup snapshotByIDFunc, afterID *int64) ([]table.Snapshot, bool) {
	if current == nil {
		return nil, true
	}
	var chain []table.Snapshot
	cur := current
	foundAfter := afterID == nil
	for cur != nil {
		if afterID != nil && cur.SnapshotID == *afterID {
			foundAfter = true
			break
		}
		chain = append(chain, *cur)
		if cur.ParentSnapshotID == nil {
			break
		}
		cur = lookup(*cur.ParentSnapshotID)
	}
	reverseSnapshots(chain)
	return chain, foundAfter
}

func reverseSnapshots(s []table.Snapshot) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}
