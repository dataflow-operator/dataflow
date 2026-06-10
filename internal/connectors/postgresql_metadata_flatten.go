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

func (p *PostgreSQLSinkConnector) pgFlattenPayloadColumn() string {
	return "data"
}

func (p *PostgreSQLSinkConnector) loadPostgreSQLFlattenMetaColumns(ctx context.Context) error {
	schema, tableName := ParseTableRef(p.config.Table)
	rows, err := p.conn.Query(ctx, `
		SELECT column_name FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position`, schema, tableName)
	if err != nil {
		return fmt.Errorf("list table columns: %w", err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		if isReservedFlattenPayloadColumn(name, p.pgFlattenPayloadColumn()) {
			continue
		}
		cols = append(cols, name)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	sort.Strings(cols)
	p.metaColumnNames = cols
	return nil
}

func (p *PostgreSQLSinkConnector) ensurePostgreSQLFlattenTable(ctx context.Context, msgs []*types.Message) error {
	if len(p.metaColumnNames) > 0 || !p.deferredTableCreate {
		return nil
	}
	cols, err := collectFlattenMetadataColumnNames(msgs, p.flattenMetadataPrefix())
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return fmt.Errorf("flattenMetadataColumns: no metadata keys found in first batch")
	}
	categories := inferFlattenColumnCategories(msgs, cols, p.flattenMetadataPrefix())
	quotedTable := QuotePostgreSQLTableRef(p.config.Table)

	colDefs := []string{
		"id SERIAL PRIMARY KEY",
		fmt.Sprintf("%s JSONB NOT NULL", quotePostgreSQLIdentifier(p.pgFlattenPayloadColumn())),
	}
	for _, col := range cols {
		colDefs = append(colDefs, fmt.Sprintf("%s %s", quotePostgreSQLIdentifier(col), postgreSQLTypeForCategory(categories[col])))
	}
	colDefs = append(colDefs,
		"created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
		"updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
		"deleted_at TIMESTAMP",
	)

	createQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n\t%s\n)", quotedTable, strings.Join(colDefs, ",\n\t"))
	if _, err := p.conn.Exec(ctx, createQuery); err != nil {
		return fmt.Errorf("failed to create flattened metadata table: %w", err)
	}
	trueVal := true
	p.tableExistsCached = &trueVal
	p.metaColumnNames = cols
	p.metaColumnTypes = categories
	p.deferredTableCreate = false
	p.logger.Info("Created PostgreSQL table with flattened metadata columns", "table", p.config.Table, "columns", cols)
	return nil
}

func (p *PostgreSQLSinkConnector) buildFlattenInsertForMessage(ctx context.Context, msg *types.Message) (query string, values []interface{}, err error) {
	if err := p.ensurePostgreSQLFlattenTable(ctx, []*types.Message{msg}); err != nil {
		return "", nil, err
	}
	dataStr, err := extractPayloadDataStr(msg)
	if err != nil {
		return "", nil, err
	}
	meta, err := parseMetadataMapFromMessage(msg)
	if err != nil {
		return "", nil, err
	}
	knownCols := make(map[string]struct{}, len(p.metaColumnNames))
	for _, c := range p.metaColumnNames {
		knownCols[c] = struct{}{}
	}
	logSkippedUnknownMetadataKeys(meta, knownCols, p.flattenMetadataPrefix(), p.logger)

	colNames := []string{p.pgFlattenPayloadColumn()}
	placeholders := []string{"$1::jsonb"}
	values = []interface{}{dataStr}
	idx := 2
	for _, col := range p.metaColumnNames {
		colNames = append(colNames, col)
		placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
		key := metadataKeyFromColumn(col, p.flattenMetadataPrefix())
		values = append(values, flattenMetadataValueForSQL(meta[key]))
		idx++
	}

	quotedTable := QuotePostgreSQLTableRef(p.config.Table)
	quotedCols := make([]string, len(colNames))
	for i, c := range colNames {
		quotedCols[i] = quotePostgreSQLIdentifier(c)
	}
	upsertMode := p.config.UpsertMode != nil && *p.config.UpsertMode
	if upsertMode {
		conflictKey := resolvePostgreSQLConflictKey(p.config)
		updateCols := []string{p.pgFlattenPayloadColumn()}
		for _, col := range p.metaColumnNames {
			updateCols = append(updateCols, col)
		}
		onConflict := buildPostgreSQLOnConflictClause(quotedTable, conflictKey, updateCols, p.config)
		query = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) %s",
			quotedTable,
			strings.Join(quotedCols, ", "),
			strings.Join(placeholders, ", "),
			onConflict,
		)
	} else {
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", quotedTable, strings.Join(quotedCols, ", "), strings.Join(placeholders, ", "))
	}
	return query, values, nil
}

func (p *PostgreSQLSinkConnector) connectFlattenMetadata(ctx context.Context) error {
	exists, err := p.tableExists(ctx)
	if err != nil {
		return err
	}
	if !exists {
		if p.config.AutoCreateTable != nil && *p.config.AutoCreateTable {
			p.deferredTableCreate = true
			p.logger.Info("Deferring PostgreSQL table creation until first batch with metadata keys", "table", p.config.Table)
			return nil
		}
		return fmt.Errorf("table %s does not exist and AutoCreateTable is not set", p.config.Table)
	}
	schema, tableName := ParseTableRef(p.config.Table)
	var hasMeta bool
	err = p.conn.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = $2 AND column_name = '_metadata'
		)`, schema, tableName).Scan(&hasMeta)
	if err != nil {
		return err
	}
	if hasMeta {
		return fmt.Errorf("flattenMetadataColumns is incompatible with \"_metadata\" column; recreate the table or disable flattenMetadataColumns")
	}
	return p.loadPostgreSQLFlattenMetaColumns(ctx)
}
