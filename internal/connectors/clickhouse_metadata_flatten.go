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
	"sort"
	"strings"

	"github.com/dataflow-operator/dataflow/internal/types"
)

func (c *ClickHouseSinkConnector) loadClickHouseFlattenMetaColumns(ctx context.Context) error {
	query := fmt.Sprintf("SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = '%s' ORDER BY position",
		strings.ReplaceAll(c.config.Table, "'", "''"))
	rows, err := c.conn.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("list table columns: %w", err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return err
		}
		if strings.EqualFold(name, "_metadata") {
			return fmt.Errorf("flattenMetadataColumns is incompatible with \"_metadata\" column; recreate the table or disable flattenMetadataColumns")
		}
		if isReservedFlattenPayloadColumn(name, "data") {
			continue
		}
		cols = append(cols, name)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	sort.Strings(cols)
	c.metaColumnNames = cols
	return nil
}

func (c *ClickHouseSinkConnector) ensureClickHouseFlattenTable(ctx context.Context, msgs []*types.Message) error {
	if len(c.metaColumnNames) > 0 || !c.deferredTableCreate {
		return nil
	}
	cols, err := collectFlattenMetadataColumnNames(msgs, c.flattenMetadataPrefix())
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return fmt.Errorf("flattenMetadataColumns: no metadata keys found in first batch")
	}
	categories := inferFlattenColumnCategories(msgs, cols, c.flattenMetadataPrefix())

	colDefs := []string{"data String"}
	for _, col := range cols {
		chType := inferClickHouseTypeFromCategory(categories[col])
		colDefs = append(colDefs, fmt.Sprintf("`%s` %s", col, chType))
	}
	colDefs = append(colDefs, "created_at DateTime DEFAULT now()")

	createQuery := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n\t%s\n) ENGINE = MergeTree()\nORDER BY created_at",
		c.config.Table, strings.Join(colDefs, ",\n\t"),
	)
	if _, err := c.conn.ExecContext(ctx, createQuery); err != nil {
		return fmt.Errorf("failed to create flattened metadata table: %w", err)
	}
	c.metaColumnNames = cols
	c.metaColumnTypes = categories
	c.deferredTableCreate = false
	c.logger.Info("Created ClickHouse table with flattened metadata columns", "table", c.config.Table, "columns", cols)
	return nil
}

func inferClickHouseTypeFromCategory(cat flattenValueCategory) string {
	switch cat {
	case flattenCategoryBool:
		return "UInt8"
	case flattenCategoryInt32:
		return "Int32"
	case flattenCategoryInt64:
		return "Int64"
	case flattenCategoryFloat64:
		return "Float64"
	default:
		return "String"
	}
}

func (c *ClickHouseSinkConnector) flushBatchFlattened(ctx context.Context, msgs []*types.Message) error {
	if err := c.ensureClickHouseFlattenTable(ctx, msgs); err != nil {
		return err
	}
	if len(c.metaColumnNames) == 0 {
		return fmt.Errorf("flattenMetadataColumns: metadata columns not initialized")
	}
	knownCols := make(map[string]struct{}, len(c.metaColumnNames))
	for _, col := range c.metaColumnNames {
		knownCols[col] = struct{}{}
	}

	colNames := append([]string{"data"}, c.metaColumnNames...)
	colNames = append(colNames, "created_at")
	placeholders := make([]string, len(colNames))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	colsQuoted := make([]string, len(colNames))
	for i, col := range colNames {
		colsQuoted[i] = fmt.Sprintf("`%s`", col)
	}
	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", c.config.Table, strings.Join(colsQuoted, ", "), strings.Join(placeholders, ", "))

	tx, err := c.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	stmt, err := tx.PrepareContext(ctx, insertQuery)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, m := range msgs {
		dataStr, err := extractPayloadDataStr(m)
		if err != nil {
			tx.Rollback()
			return err
		}
		meta, err := parseMetadataMapFromMessage(m)
		if err != nil {
			tx.Rollback()
			return err
		}
		logSkippedUnknownMetadataKeys(meta, knownCols, c.flattenMetadataPrefix(), c.logger)

		args := []interface{}{dataStr}
		for _, col := range c.metaColumnNames {
			key := metadataKeyFromColumn(col, c.flattenMetadataPrefix())
			args = append(args, flattenMetadataValueForSQL(meta[key]))
		}
		args = append(args, nil) // created_at default
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to exec flatten insert: %w", err)
		}
		if m.Ack != nil {
			m.Ack()
		}
	}
	return tx.Commit()
}

func (c *ClickHouseSinkConnector) connectFlattenMetadata(ctx context.Context) error {
	exists, err := c.tableExists(ctx)
	if err != nil {
		return err
	}
	if !exists {
		if c.config.AutoCreateTable != nil && *c.config.AutoCreateTable {
			c.deferredTableCreate = true
			c.logger.Info("Deferring ClickHouse table creation until first batch with metadata keys", "table", c.config.Table)
			return nil
		}
		return fmt.Errorf("table %s does not exist and AutoCreateTable is not set", c.config.Table)
	}
	return c.loadClickHouseFlattenMetaColumns(ctx)
}
