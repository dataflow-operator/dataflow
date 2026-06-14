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

	pkCol := postgresCDCPrimaryKeyColumn(c.config)
	filter := newPostgresCDCColumnFilter(c.config)
	var completedThisRun []string

	for _, table := range tables {
		if _, ok := doneSet[table]; ok {
			continue
		}
		quoted := QuotePostgreSQLTableRef(table)
		query := fmt.Sprintf("SELECT * FROM %s", quoted)
		rows, err := tx.Query(ctx, query)
		if err != nil {
			return fmt.Errorf("snapshot query %s: %w", table, err)
		}

		fieldNames := rows.FieldDescriptions()
		colNames := make([]string, len(fieldNames))
		for i, f := range fieldNames {
			colNames[i] = f.Name
		}

		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				rows.Close()
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
				rows.Close()
				return err
			}
			select {
			case msgChan <- msg:
				c.RecordMessageRead()
			case <-ctx.Done():
				rows.Close()
				return ctx.Err()
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("snapshot rows %s: %w", table, err)
		}
		rows.Close()
		completedThisRun = append(completedThisRun, table)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit snapshot transaction: %w", err)
	}

	allTablesDone := len(completedThisRun)+len(doneSet) >= len(tables)
	c.cp.persistSnapshotProgress(completedThisRun, snapshotLSN, allTablesDone)
	return nil
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
