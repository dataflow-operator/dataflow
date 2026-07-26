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
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/table"
	"github.com/dataflow-operator/dataflow/internal/types"
)

// lakehousePollLimits caps work per poll cycle for Iceberg/Nessie sources.
type lakehousePollLimits struct {
	maxRows  int64
	maxBytes int64
}

func lakehousePollLimitsFrom(maxRows, maxBytes *int32) lakehousePollLimits {
	var l lakehousePollLimits
	if maxRows != nil && *maxRows > 0 {
		l.maxRows = int64(*maxRows)
	}
	if maxBytes != nil && *maxBytes > 0 {
		l.maxBytes = int64(*maxBytes)
	}
	return l
}

func (l lakehousePollLimits) active() bool {
	return l.maxRows > 0 || l.maxBytes > 0
}

type lakehouseEmitStats struct {
	Emitted    int
	Bytes      int64
	HitLimit   bool
	NextOffset int64 // skipRows + Emitted when HitLimit; 0 when complete
}

// collectIcebergScanPage reads up to one poll page from an Iceberg snapshot via ToArrowRecords.
// skipRows discards the first N rows (pagination resume). Messages are returned so the caller
// can attach Ack only when HitLimit is false (snapshot fully drained).
func collectIcebergScanPage(
	ctx context.Context,
	tbl *table.Table,
	snapshotID *int64,
	namespace, tableName string,
	limits lakehousePollLimits,
	skipRows int64,
) ([]*types.Message, lakehouseEmitStats, error) {
	var stats lakehouseEmitStats
	opts := make([]table.ScanOption, 0, 2)
	if snapshotID != nil {
		opts = append(opts, table.WithSnapshotID(*snapshotID))
	}
	if limits.maxRows > 0 {
		opts = append(opts, table.WithLimit(skipRows+limits.maxRows))
	}

	schema, itr, err := tbl.Scan(opts...).ToArrowRecords(ctx)
	if err != nil {
		return nil, stats, err
	}

	msgs := make([]*types.Message, 0, 256)
	var skipped int64
	for rec, iterErr := range itr {
		if iterErr != nil {
			return nil, stats, iterErr
		}
		batchTable := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
		batchMsgs := arrowTableToMessages(batchTable, namespace, tableName, false)
		batchTable.Release()
		rec.Release()

		for _, msg := range batchMsgs {
			if skipped < skipRows {
				skipped++
				continue
			}
			if limits.maxRows > 0 && int64(len(msgs)) >= limits.maxRows {
				stats.HitLimit = true
				stats.Emitted = len(msgs)
				stats.NextOffset = skipRows + int64(len(msgs))
				return msgs, stats, nil
			}
			if limits.maxBytes > 0 && stats.Bytes+int64(len(msg.Data)) > limits.maxBytes && len(msgs) > 0 {
				stats.HitLimit = true
				stats.Emitted = len(msgs)
				stats.NextOffset = skipRows + int64(len(msgs))
				return msgs, stats, nil
			}
			msgs = append(msgs, msg)
			stats.Bytes += int64(len(msg.Data))
		}
	}

	stats.Emitted = len(msgs)
	if limits.maxRows > 0 && int64(len(msgs)) >= limits.maxRows {
		stats.HitLimit = true
		stats.NextOffset = skipRows + int64(len(msgs))
		return msgs, stats, nil
	}
	stats.NextOffset = 0
	return msgs, stats, nil
}

func sendLakehouseMessages(
	ctx context.Context,
	msgChan chan *types.Message,
	msgs []*types.Message,
	reportFill func(*types.Message),
) error {
	for _, msg := range msgs {
		if reportFill != nil {
			reportFill(msg)
		}
		select {
		case msgChan <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// countAddedDataFiles returns how many data files appear in current but not in parent.
func countAddedDataFiles(ctx context.Context, tbl *table.Table, currentID int64, parentID *int64) (added int, err error) {
	curTasks, err := tbl.Scan(table.WithSnapshotID(currentID)).PlanFiles(ctx)
	if err != nil {
		return 0, fmt.Errorf("plan files snapshot %d: %w", currentID, err)
	}
	if parentID == nil {
		return len(curTasks), nil
	}
	parentTasks, err := tbl.Scan(table.WithSnapshotID(*parentID)).PlanFiles(ctx)
	if err != nil {
		return 0, fmt.Errorf("plan files parent snapshot %d: %w", *parentID, err)
	}
	parentPaths := make(map[string]struct{}, len(parentTasks))
	for _, t := range parentTasks {
		parentPaths[t.File.FilePath()] = struct{}{}
	}
	for _, t := range curTasks {
		if _, ok := parentPaths[t.File.FilePath()]; !ok {
			added++
		}
	}
	return added, nil
}
