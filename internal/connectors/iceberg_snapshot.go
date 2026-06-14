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

	v1 "github.com/dataflow-operator/dataflow/api/v1"
)

type icebergSnapshotCheckpoint struct {
	LastAckedSnapshotID       string `json:"lastAckedSnapshotID,omitempty"`
	LastAckedSnapshotSequence int64  `json:"lastAckedSnapshotSequence,omitempty"`
	Namespace                 string `json:"namespace,omitempty"`
	Table                     string `json:"table,omitempty"`
}

func icebergIncrementalEnabled(cfg *v1.IcebergSourceSpec) bool {
	return cfg != nil && cfg.IncrementalBySnapshot != nil && *cfg.IncrementalBySnapshot
}

func icebergSnapshotCheckpointsEnabled(cfg *v1.IcebergSourceSpec) bool {
	if cfg == nil || cfg.SnapshotCheckpoints == nil {
		return true
	}
	return *cfg.SnapshotCheckpoints
}

func (c *IcebergSourceConnector) applyInitialCheckpoint(data []byte) {
	var cp icebergSnapshotCheckpoint
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

func (c *IcebergSourceConnector) marshalCheckpointLocked() []byte {
	cp := icebergSnapshotCheckpoint{
		LastAckedSnapshotID:       formatSnapshotID(c.lastAckedSnapshotID),
		LastAckedSnapshotSequence: c.lastAckedSnapshotSequence,
		Namespace:                 c.config.Namespace,
		Table:                     c.config.Table,
	}
	out, _ := json.Marshal(cp)
	return out
}

func (c *IcebergSourceConnector) advanceCheckpoint(snapshotID int64, sequence int64) {
	c.checkpointMu.Lock()
	if sequence <= c.lastAckedSnapshotSequence {
		c.checkpointMu.Unlock()
		return
	}
	c.lastAckedSnapshotID = snapshotID
	c.lastAckedSnapshotSequence = sequence
	persist := c.checkpointStore != nil && icebergSnapshotCheckpointsEnabled(c.config)
	var data []byte
	if persist {
		data = c.marshalCheckpointLocked()
	}
	c.checkpointMu.Unlock()

	if persist && len(data) > 0 {
		sourceType := c.sourceType
		if sourceType == "" {
			sourceType = "iceberg"
		}
		_ = c.checkpointStore.Save(context.Background(), sourceType, data)
	}
}
