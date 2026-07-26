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

	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// snapshotCursorPersistEvery controls how often mid-table PK cursor is persisted.
const snapshotCursorPersistEvery = 500

func (c *PostgreSQLCDCSourceConnector) shouldRunSnapshot() bool {
	mode := postgresCDCSnapshotMode(c.config)
	switch mode {
	case "never":
		return false
	case "always":
		return true
	case "initial":
		return !c.cp.allSnapshotTablesDone(normalizePostgreSQLTableRefs(c.config.Tables))
	default:
		return false
	}
}

func (c *PostgreSQLCDCSourceConnector) runInitialSnapshot(ctx context.Context, msgChan chan *types.Message) error {
	tables := normalizePostgreSQLTableRefs(c.config.Tables)
	mode := postgresCDCSnapshotMode(c.config)
	if mode == "always" {
		c.cp.resetSnapshotProgress()
	}

	done := c.cp.snapshotTablesDone()
	doneSet := make(map[string]struct{}, len(done))
	for _, t := range done {
		doneSet[t] = struct{}{}
	}

	c.cp.setPhase(postgresCDCPhaseSnapshot)

	tx, err := c.sqlConn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return fmt.Errorf("begin snapshot transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// pg_export_snapshot pins a consistent MVCC view for all SELECTs in this transaction.
	var exportSnapshot string
	if err := tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&exportSnapshot); err != nil {
		return fmt.Errorf("export snapshot: %w", err)
	}
	c.logger.V(1).Info("PostgreSQL CDC snapshot exported", "snapshot", exportSnapshot)

	var snapshotLSN pglogrepl.LSN
	var lsnText string
	if err := tx.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsnText); err != nil {
		return fmt.Errorf("read snapshot LSN: %w", err)
	}
	parsed, err := pglogrepl.ParseLSN(lsnText)
	if err != nil {
		return fmt.Errorf("parse snapshot LSN %q: %w", lsnText, err)
	}
	snapshotLSN = parsed
	// Persist LSN early so a crash mid-snapshot can still start replication from this point
	// after remaining tables finish.
	c.cp.setSnapshotLSN(snapshotLSN)

	pkCol := postgresCDCPrimaryKeyColumn(c.config)
	useCursor := c.config != nil && c.config.PrimaryKeyColumn != ""
	filter := newPostgresCDCColumnFilter(c.config)
	cursorTable, cursorKeyJSON := c.cp.snapshotCursor()

	for _, table := range tables {
		if _, ok := doneSet[table]; ok {
			continue
		}
		if err := c.snapshotOneTable(ctx, tx, msgChan, table, pkCol, useCursor, cursorTable, cursorKeyJSON, snapshotLSN, filter); err != nil {
			return err
		}
		// Clear mid-table cursor and mark table done immediately so resume skips it.
		c.cp.clearSnapshotCursor()
		c.cp.markSnapshotTableDone(table)
		doneSet[table] = struct{}{}
		cursorTable, cursorKeyJSON = "", ""
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit snapshot transaction: %w", err)
	}

	allTablesDone := len(doneSet) >= len(tables)
	c.cp.persistSnapshotProgress(nil, snapshotLSN, allTablesDone)
	return nil
}

func (c *PostgreSQLCDCSourceConnector) snapshotOneTable(
	ctx context.Context,
	tx pgx.Tx,
	msgChan chan *types.Message,
	table, pkCol string,
	useCursor bool,
	cursorTable, cursorKeyJSON string,
	snapshotLSN pglogrepl.LSN,
	filter *postgresCDCColumnFilter,
) error {
	quoted := QuotePostgreSQLTableRef(table)
	var (
		rows pgx.Rows
		err  error
	)

	resumeKey := ""
	if useCursor && cursorTable == table && cursorKeyJSON != "" {
		resumeKey = cursorKeyJSON
	}

	if useCursor {
		qpk := quotePostgreSQLIdentifier(pkCol)
		if resumeKey != "" {
			var keyVal interface{}
			if uerr := json.Unmarshal([]byte(resumeKey), &keyVal); uerr != nil {
				return fmt.Errorf("decode snapshot cursor for %s: %w", table, uerr)
			}
			query := fmt.Sprintf("SELECT * FROM %s WHERE %s > $1 ORDER BY %s", quoted, qpk, qpk)
			rows, err = tx.Query(ctx, query, keyVal)
		} else {
			query := fmt.Sprintf("SELECT * FROM %s ORDER BY %s", quoted, qpk)
			rows, err = tx.Query(ctx, query)
		}
	} else {
		query := fmt.Sprintf("SELECT * FROM %s", quoted)
		rows, err = tx.Query(ctx, query)
	}
	if err != nil {
		return fmt.Errorf("snapshot query %s: %w", table, err)
	}
	defer rows.Close()

	fieldNames := rows.FieldDescriptions()
	colNames := make([]string, len(fieldNames))
	for i, f := range fieldNames {
		colNames[i] = f.Name
	}

	rowsSinceCursor := 0
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("snapshot scan %s: %w", table, err)
		}
		rowMap := make(map[string]interface{}, len(colNames))
		for i, name := range colNames {
			if filter != nil && !filter.keep(name) {
				continue
			}
			rowMap[name] = values[i]
		}
		msg, err := c.buildCDCMessage(rowMap, nil, table, "insert", snapshotLSN, pkCol, true)
		if err != nil {
			return err
		}
		if err := c.sendCDCMessage(ctx, msgChan, msg); err != nil {
			return err
		}
		c.RecordMessageRead()

		if useCursor {
			if pkVal, ok := rowMap[pkCol]; ok {
				if keyJSON, mErr := json.Marshal(pkVal); mErr == nil {
					rowsSinceCursor++
					if rowsSinceCursor >= snapshotCursorPersistEvery {
						c.cp.setSnapshotCursor(table, string(keyJSON))
						rowsSinceCursor = 0
					}
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("snapshot rows %s: %w", table, err)
	}

	// Persist final cursor key once more is unnecessary once table is marked done.
	return nil
}

func (c *PostgreSQLCDCSourceConnector) sendCDCMessage(ctx context.Context, msgChan chan *types.Message, msg *types.Message) error {
	c.reportChannelFill(msgChan, "source")
	select {
	case msgChan <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *PostgreSQLCDCSourceConnector) reportChannelFill(msgChan chan *types.Message, channel string) {
	if !c.hasMetadata() {
		return
	}
	ratio := 0.0
	if cap(msgChan) > 0 {
		ratio = float64(len(msgChan)) / float64(cap(msgChan))
	}
	c.SetChannelFillRatio(channel, ratio)
}

func (c *PostgreSQLCDCSourceConnector) buildCDCMessage(
	after, before map[string]interface{},
	table, operation string,
	lsn pglogrepl.LSN,
	pkCol string,
	snapshot bool,
) (*types.Message, error) {
	row := after
	if operation == "delete" {
		row = before
	}

	var data []byte
	var err error
	if postgresCDCEnvelopeDebezium(c.config) {
		envelope := buildDebeziumEnvelope(after, before, table, operation, lsn, snapshot)
		data, err = json.Marshal(envelope)
	} else {
		data, err = json.Marshal(row)
	}
	if err != nil {
		return nil, fmt.Errorf("marshal CDC row: %w", err)
	}

	msg := types.NewMessage(data)
	msg.Metadata["table"] = table
	msg.Metadata["operation"] = operation
	if lsn != 0 {
		msg.Metadata["lsn"] = lsn.String()
	}
	if pkCol != "" && row != nil {
		if id, ok := row[pkCol]; ok {
			SetSourceRowIDMetadata(msg, id)
		}
	}
	if lsn != 0 {
		msg.Ack = c.cp.makeAck(lsn)
	}
	return msg, nil
}
