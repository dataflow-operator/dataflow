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
	"sync"

	"github.com/dataflow-operator/dataflow/internal/checkpoint"
	"github.com/jackc/pglogrepl"
)

const (
	postgresCDCPhaseSnapshot  = "snapshot"
	postgresCDCPhaseStreaming = "streaming"
)

// postgresCDCCheckpoint is persisted under checkpoint store key "postgresql-cdc".
type postgresCDCCheckpoint struct {
	LastAckedLSN            string   `json:"lastAckedLSN,omitempty"`
	SnapshotLSN             string   `json:"snapshotLSN,omitempty"`
	SlotName                string   `json:"slotName,omitempty"`
	PublicationName         string   `json:"publicationName,omitempty"`
	Phase                   string   `json:"phase,omitempty"`
	SnapshotCompletedTables []string `json:"snapshotCompletedTables,omitempty"`
	// Mid-table snapshot resume (requires explicit primaryKeyColumn).
	SnapshotCursorTable string `json:"snapshotCursorTable,omitempty"`
	SnapshotCursorKey   string `json:"snapshotCursorKey,omitempty"` // JSON-encoded PK value
}

type postgresCDCCheckpointHolder struct {
	mu              sync.Mutex
	store           checkpoint.Store
	sourceType      string
	slotName        string
	publicationName string
	lastAckedLSN    pglogrepl.LSN
	snapshotLSN     pglogrepl.LSN
	phase           string
	snapshotDone    []string
	cursorTable     string
	cursorKey       string
	onAdvance       func(lsn pglogrepl.LSN)
	reporter        checkpointSaveReporter
}

func (h *postgresCDCCheckpointHolder) setReporter(r checkpointSaveReporter) {
	h.reporter = r
}

func (h *postgresCDCCheckpointHolder) init(store checkpoint.Store, sourceType string, slotName, publicationName string, initial []byte) {
	h.store = store
	h.sourceType = sourceType
	h.slotName = slotName
	h.publicationName = publicationName
	h.phase = postgresCDCPhaseStreaming
	if initial != nil {
		h.applyInitial(initial)
	}
}

func (h *postgresCDCCheckpointHolder) applyInitial(data []byte) {
	var cp postgresCDCCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return
	}
	if cp.LastAckedLSN != "" {
		if lsn, err := pglogrepl.ParseLSN(cp.LastAckedLSN); err == nil {
			h.lastAckedLSN = lsn
		}
	}
	if cp.SnapshotLSN != "" {
		if lsn, err := pglogrepl.ParseLSN(cp.SnapshotLSN); err == nil {
			h.snapshotLSN = lsn
		}
	}
	if cp.Phase != "" {
		h.phase = cp.Phase
	}
	if len(cp.SnapshotCompletedTables) > 0 {
		h.snapshotDone = append([]string(nil), cp.SnapshotCompletedTables...)
	}
	h.cursorTable = cp.SnapshotCursorTable
	h.cursorKey = cp.SnapshotCursorKey
}

func (h *postgresCDCCheckpointHolder) startLSN() pglogrepl.LSN {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.lastAckedLSN != 0 {
		return h.lastAckedLSN
	}
	return h.snapshotLSN
}

func (h *postgresCDCCheckpointHolder) setSnapshotLSN(lsn pglogrepl.LSN) {
	if lsn == 0 {
		return
	}
	h.mu.Lock()
	h.snapshotLSN = lsn
	data := h.marshalLocked()
	h.mu.Unlock()
	h.persist(data)
}

func (h *postgresCDCCheckpointHolder) phaseSnapshot() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.phase == postgresCDCPhaseSnapshot
}

func (h *postgresCDCCheckpointHolder) setPhase(phase string) {
	h.mu.Lock()
	h.phase = phase
	data := h.marshalLocked()
	h.mu.Unlock()
	h.persist(data)
}

func (h *postgresCDCCheckpointHolder) markSnapshotTableDone(table string) {
	h.persistSnapshotProgress([]string{table}, 0, false)
}

func (h *postgresCDCCheckpointHolder) snapshotCursor() (table, keyJSON string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.cursorTable, h.cursorKey
}

// setSnapshotCursor persists mid-table snapshot resume position.
func (h *postgresCDCCheckpointHolder) setSnapshotCursor(table, keyJSON string) {
	h.mu.Lock()
	h.cursorTable = table
	h.cursorKey = keyJSON
	data := h.marshalLocked()
	h.mu.Unlock()
	h.persist(data)
}

func (h *postgresCDCCheckpointHolder) clearSnapshotCursor() {
	h.mu.Lock()
	if h.cursorTable == "" && h.cursorKey == "" {
		h.mu.Unlock()
		return
	}
	h.cursorTable = ""
	h.cursorKey = ""
	data := h.marshalLocked()
	h.mu.Unlock()
	h.persist(data)
}

// persistSnapshotProgress records snapshot table completion and optional LSN after the
// snapshot transaction commits. Phase transitions to streaming only when allTablesDone.
func (h *postgresCDCCheckpointHolder) persistSnapshotProgress(completedTables []string, lsn pglogrepl.LSN, allTablesDone bool) {
	h.mu.Lock()
	for _, table := range completedTables {
		duplicate := false
		for _, t := range h.snapshotDone {
			if t == table {
				duplicate = true
				break
			}
		}
		if !duplicate {
			h.snapshotDone = append(h.snapshotDone, table)
		}
	}
	if lsn != 0 {
		h.snapshotLSN = lsn
	}
	if allTablesDone {
		h.phase = postgresCDCPhaseStreaming
		h.cursorTable = ""
		h.cursorKey = ""
	}
	data := h.marshalLocked()
	h.mu.Unlock()
	h.persist(data)
}

func (h *postgresCDCCheckpointHolder) snapshotTablesDone() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.snapshotDone...)
}

func (h *postgresCDCCheckpointHolder) resetSnapshotProgress() {
	h.mu.Lock()
	h.snapshotDone = nil
	h.snapshotLSN = 0
	h.cursorTable = ""
	h.cursorKey = ""
	h.phase = postgresCDCPhaseStreaming
	data := h.marshalLocked()
	h.mu.Unlock()
	h.persist(data)
}

func (h *postgresCDCCheckpointHolder) allSnapshotTablesDone(tables []string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	done := make(map[string]struct{}, len(h.snapshotDone))
	for _, t := range h.snapshotDone {
		done[t] = struct{}{}
	}
	for _, t := range tables {
		if _, ok := done[t]; !ok {
			return false
		}
	}
	return true
}

func (h *postgresCDCCheckpointHolder) makeAck(lsn pglogrepl.LSN) func() {
	return func() {
		h.advance(lsn)
	}
}

func (h *postgresCDCCheckpointHolder) advance(lsn pglogrepl.LSN) {
	if lsn == 0 {
		return
	}
	h.mu.Lock()
	if lsn <= h.lastAckedLSN {
		h.mu.Unlock()
		return
	}
	h.lastAckedLSN = lsn
	data := h.marshalLocked()
	onAdvance := h.onAdvance
	h.mu.Unlock()

	h.persist(data)
	if onAdvance != nil {
		onAdvance(lsn)
	}
}

func (h *postgresCDCCheckpointHolder) marshalLocked() []byte {
	cp := postgresCDCCheckpoint{
		SlotName:                h.slotName,
		PublicationName:         h.publicationName,
		Phase:                   h.phase,
		SnapshotCompletedTables: append([]string(nil), h.snapshotDone...),
		SnapshotCursorTable:     h.cursorTable,
		SnapshotCursorKey:       h.cursorKey,
	}
	if h.lastAckedLSN != 0 {
		cp.LastAckedLSN = h.lastAckedLSN.String()
	}
	if h.snapshotLSN != 0 {
		cp.SnapshotLSN = h.snapshotLSN.String()
	}
	out, _ := json.Marshal(cp)
	return out
}

func (h *postgresCDCCheckpointHolder) persist(data []byte) {
	if h.store == nil || len(data) == 0 {
		return
	}
	sourceType := h.sourceType
	if sourceType == "" {
		sourceType = "postgresql-cdc"
	}
	err := h.store.Save(context.Background(), sourceType, data)
	h.reporter.report(err, checkpointOpSave)
}
