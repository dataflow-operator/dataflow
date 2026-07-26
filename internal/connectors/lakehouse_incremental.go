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
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/dataflow-operator/dataflow/internal/types"
)

// listAddedFileScanTasks returns data-file scan tasks present in current but not parent.
// When parentID is nil, returns all tasks for current (bootstrap / first snapshot).
func listAddedFileScanTasks(ctx context.Context, tbl *table.Table, currentID int64, parentID *int64) ([]table.FileScanTask, error) {
	curTasks, err := tbl.Scan(table.WithSnapshotID(currentID)).PlanFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("plan files snapshot %d: %w", currentID, err)
	}
	if parentID == nil {
		return curTasks, nil
	}
	parentTasks, err := tbl.Scan(table.WithSnapshotID(*parentID)).PlanFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("plan files parent snapshot %d: %w", *parentID, err)
	}
	parentPaths := make(map[string]struct{}, len(parentTasks))
	for _, t := range parentTasks {
		parentPaths[t.File.FilePath()] = struct{}{}
	}
	added := make([]table.FileScanTask, 0, len(curTasks))
	for _, t := range curTasks {
		if _, ok := parentPaths[t.File.FilePath()]; !ok {
			added = append(added, t)
		}
	}
	return added, nil
}

// countAddedDataFiles returns how many data files appear in current but not in parent.
func countAddedDataFiles(ctx context.Context, tbl *table.Table, currentID int64, parentID *int64) (added int, err error) {
	tasks, err := listAddedFileScanTasks(ctx, tbl, currentID, parentID)
	if err != nil {
		return 0, err
	}
	return len(tasks), nil
}

// addedFilesReadableDirectly reports whether tasks can be read via parquet-only path
// (no positional/equality deletes). Otherwise callers must fall back to full Scan.
func addedFilesReadableDirectly(tasks []table.FileScanTask) bool {
	if len(tasks) == 0 {
		return true
	}
	for _, t := range tasks {
		if len(t.DeleteFiles) > 0 {
			return false
		}
		if t.File.FileFormat() != iceberg.ParquetFile {
			return false
		}
	}
	return true
}

// collectIcebergScanPage reads up to one poll page from an Iceberg snapshot.
// When parentSnapshotID is set, prefers file-level incremental (only added data files).
// Empty added-file delta returns a completed empty page without scanning the full snapshot.
func collectIcebergScanPage(
	ctx context.Context,
	tbl *table.Table,
	snapshotID *int64,
	parentSnapshotID *int64,
	namespace, tableName string,
	limits lakehousePollLimits,
	skipRows int64,
) ([]*types.Message, lakehouseEmitStats, error) {
	var stats lakehouseEmitStats
	if snapshotID != nil && parentSnapshotID != nil {
		added, err := listAddedFileScanTasks(ctx, tbl, *snapshotID, parentSnapshotID)
		if err != nil {
			return nil, stats, err
		}
		if len(added) == 0 {
			stats.NextOffset = 0
			return nil, stats, nil
		}
		if addedFilesReadableDirectly(added) {
			return collectFromAddedParquetFiles(ctx, tbl, added, namespace, tableName, limits, skipRows)
		}
		// Deletes or non-parquet → correctness requires full snapshot scan.
	}

	return collectIcebergFullScanPage(ctx, tbl, snapshotID, namespace, tableName, limits, skipRows)
}

func collectIcebergFullScanPage(
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
			if hit, done := appendWithLimits(&msgs, &stats, msg, limits, skipRows); hit {
				return done, stats, nil
			}
		}
	}

	return finalizePageStats(msgs, &stats, limits, skipRows)
}

func collectFromAddedParquetFiles(
	ctx context.Context,
	tbl *table.Table,
	tasks []table.FileScanTask,
	namespace, tableName string,
	limits lakehousePollLimits,
	skipRows int64,
) ([]*types.Message, lakehouseEmitStats, error) {
	var stats lakehouseEmitStats
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, stats, fmt.Errorf("lakehouse fs: %w", err)
	}

	msgs := make([]*types.Message, 0, 256)
	var skipped int64
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			return nil, stats, ctx.Err()
		default:
		}
		fileMsgs, err := readParquetDataFileMessages(ctx, fs, task.File, namespace, tableName)
		if err != nil {
			return nil, stats, err
		}
		for _, msg := range fileMsgs {
			if skipped < skipRows {
				skipped++
				continue
			}
			if hit, done := appendWithLimits(&msgs, &stats, msg, limits, skipRows); hit {
				return done, stats, nil
			}
		}
	}
	return finalizePageStats(msgs, &stats, limits, skipRows)
}

func finalizePageStats(msgs []*types.Message, stats *lakehouseEmitStats, limits lakehousePollLimits, skipRows int64) ([]*types.Message, lakehouseEmitStats, error) {
	stats.Emitted = len(msgs)
	if limits.maxRows > 0 && int64(len(msgs)) >= limits.maxRows {
		stats.HitLimit = true
		stats.NextOffset = skipRows + int64(len(msgs))
		return msgs, *stats, nil
	}
	stats.NextOffset = 0
	return msgs, *stats, nil
}

func appendWithLimits(
	msgs *[]*types.Message,
	stats *lakehouseEmitStats,
	msg *types.Message,
	limits lakehousePollLimits,
	skipRows int64,
) (hitLimit bool, out []*types.Message) {
	if limits.maxRows > 0 && int64(len(*msgs)) >= limits.maxRows {
		stats.HitLimit = true
		stats.Emitted = len(*msgs)
		stats.NextOffset = skipRows + int64(len(*msgs))
		return true, *msgs
	}
	if limits.maxBytes > 0 && stats.Bytes+int64(len(msg.Data)) > limits.maxBytes && len(*msgs) > 0 {
		stats.HitLimit = true
		stats.Emitted = len(*msgs)
		stats.NextOffset = skipRows + int64(len(*msgs))
		return true, *msgs
	}
	*msgs = append(*msgs, msg)
	stats.Bytes += int64(len(msg.Data))
	return false, nil
}

func readParquetDataFileMessages(
	ctx context.Context,
	fs iceio.IO,
	dataFile iceberg.DataFile,
	namespace, tableName string,
) ([]*types.Message, error) {
	pf, err := fs.Open(dataFile.FilePath())
	if err != nil {
		return nil, fmt.Errorf("open data file %s: %w", dataFile.FilePath(), err)
	}
	defer pf.Close()

	pqReader, err := file.NewParquetReader(pf,
		file.WithReadProps(parquet.NewReaderProperties(memory.DefaultAllocator)))
	if err != nil {
		return nil, fmt.Errorf("parquet reader %s: %w", dataFile.FilePath(), err)
	}
	defer pqReader.Close()

	fr, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{
		Parallel:  true,
		BatchSize: 1 << 17,
	}, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("pqarrow reader %s: %w", dataFile.FilePath(), err)
	}

	rr, err := fr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("record reader %s: %w", dataFile.FilePath(), err)
	}
	defer rr.Release()

	var msgs []*types.Message
	for rr.Next() {
		rec := rr.RecordBatch()
		tbl := array.NewTableFromRecords(rec.Schema(), []arrow.RecordBatch{rec})
		batchMsgs := arrowTableToMessages(tbl, namespace, tableName, false)
		tbl.Release()
		msgs = append(msgs, batchMsgs...)
	}
	if err := rr.Err(); err != nil {
		return nil, fmt.Errorf("record iterate %s: %w", dataFile.FilePath(), err)
	}
	return msgs, nil
}
