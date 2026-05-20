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
	"time"

	"github.com/dataflow-operator/dataflow/internal/types"
)

func (t *TrinoSinkConnector) trinoFlattenPayloadColumn() string {
	for _, col := range t.tableColumns {
		if col.Name == "data" {
			return "data"
		}
	}
	for _, col := range t.tableColumns {
		if col.Name == "value" {
			return "value"
		}
	}
	return "data"
}

func (t *TrinoSinkConnector) loadTrinoFlattenMetaColumns(columns []TableColumnInfo) error {
	payloadCol := ""
	hasMetadata := false
	for _, col := range columns {
		if col.Name == "_metadata" {
			hasMetadata = true
		}
		if col.Name == "data" || col.Name == "value" {
			payloadCol = col.Name
		}
	}
	if hasMetadata {
		return fmt.Errorf("flattenMetadataColumns is incompatible with \"_metadata\" column; recreate the table or disable flattenMetadataColumns")
	}
	if payloadCol == "" {
		return fmt.Errorf("flattenMetadataColumns requires a \"data\" or \"value\" column in the table")
	}
	var cols []string
	for _, col := range columns {
		if isReservedFlattenPayloadColumn(col.Name, "data", "value") {
			continue
		}
		cols = append(cols, col.Name)
	}
	sort.Strings(cols)
	t.metaColumnNames = cols
	return nil
}

func (t *TrinoSinkConnector) ensureTrinoFlattenTable(ctx context.Context, msgs []*types.Message) error {
	if len(t.metaColumnNames) > 0 || !t.deferredTableCreate {
		return nil
	}
	cols, err := collectFlattenMetadataColumnNames(msgs, t.flattenMetadataPrefix())
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return fmt.Errorf("flattenMetadataColumns: no metadata keys found in first batch")
	}
	quotedCatalog := quoteTrinoIdentifier(t.config.Catalog)
	quotedSchema := quoteTrinoIdentifier(t.config.Schema)
	quotedTable := quoteTrinoIdentifier(t.config.Table)

	categories := inferFlattenColumnCategories(msgs, cols, t.flattenMetadataPrefix())
	colDefs := []string{`"data" VARCHAR(1048576)`}
	for _, col := range cols {
		colDefs = append(colDefs, fmt.Sprintf("%s %s", quoteTrinoIdentifier(col), trinoTypeForCategory(categories[col])))
	}
	createQuery := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s.%s.%s (%s) WITH (format = 'ORC')",
		quotedCatalog, quotedSchema, quotedTable, strings.Join(colDefs, ", "),
	)
	if _, err := t.client.executeQuery(ctx, createQuery); err != nil {
		return fmt.Errorf("failed to create flattened metadata table: %w", err)
	}
	t.metaColumnNames = cols
	t.deferredTableCreate = false
	tableColumns, err := t.getTableColumns(ctx)
	if err != nil {
		return err
	}
	t.columnsMu.Lock()
	t.tableColumns = tableColumns
	t.columnsMu.Unlock()
	t.logger.Info("Created Trino table with flattened metadata columns", "table", t.config.Table, "columns", cols)
	return nil
}

func (t *TrinoSinkConnector) executeBatchFlattened(ctx context.Context, batch []*types.Message) error {
	if err := t.ensureTrinoFlattenTable(ctx, batch); err != nil {
		return err
	}
	if len(t.metaColumnNames) == 0 {
		return fmt.Errorf("flattenMetadataColumns: metadata columns not initialized")
	}
	payloadCol := t.trinoFlattenPayloadColumn()
	knownCols := make(map[string]struct{}, len(t.metaColumnNames))
	for _, c := range t.metaColumnNames {
		knownCols[c] = struct{}{}
	}

	colList := append([]string{quoteTrinoIdentifier(payloadCol)}, make([]string, len(t.metaColumnNames))...)
	for i, c := range t.metaColumnNames {
		colList[i+1] = quoteTrinoIdentifier(c)
	}

	valueRows := make([]string, 0, len(batch))
	for _, msg := range batch {
		dataStr, err := extractPayloadDataStr(msg)
		if err != nil {
			return err
		}
		meta, err := parseMetadataMapFromMessage(msg)
		if err != nil {
			return err
		}
		logSkippedUnknownMetadataKeys(meta, knownCols, t.flattenMetadataPrefix(), t.logger)

		parts := []string{"'" + strings.ReplaceAll(dataStr, "'", "''") + "'"}
		for _, col := range t.metaColumnNames {
			key := metadataKeyFromColumn(col, t.flattenMetadataPrefix())
			v := meta[key]
			if v == nil {
				parts = append(parts, "NULL")
				continue
			}
			if isTimestampMetadataKey(key) {
				if ts, ok := parseFlattenTimestampValue(v); ok {
					parts = append(parts, "TIMESTAMP '"+ts.UTC().Format("2006-01-02 15:04:05.000")+"'")
					continue
				}
			}
			switch val := v.(type) {
			case time.Time:
				parts = append(parts, "TIMESTAMP '"+val.UTC().Format("2006-01-02 15:04:05.000")+"'")
			case string:
				parts = append(parts, "'"+strings.ReplaceAll(val, "'", "''")+"'")
			case bool:
				parts = append(parts, fmt.Sprintf("%t", val))
			case int, int32, int64, uint, uint32, uint64, float32, float64:
				parts = append(parts, fmt.Sprint(val))
			default:
				parts = append(parts, "'"+strings.ReplaceAll(fmt.Sprint(val), "'", "''")+"'")
			}
		}
		valueRows = append(valueRows, "("+strings.Join(parts, ", ")+")")
	}

	query := fmt.Sprintf(
		"INSERT INTO %s.%s.%s (%s) VALUES %s",
		quoteTrinoIdentifier(t.config.Catalog),
		quoteTrinoIdentifier(t.config.Schema),
		quoteTrinoIdentifier(t.config.Table),
		strings.Join(colList, ", "),
		strings.Join(valueRows, ", "),
	)
	_, err := t.client.executeQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute batch insert (flatten metadata): %w", err)
	}
	return nil
}

func (t *TrinoSinkConnector) hasFlattenMetadataColumns(columns []TableColumnInfo) (bool, string) {
	hasPayload := false
	payloadCol := ""
	hasMetadata := false
	for _, col := range columns {
		switch col.Name {
		case "data":
			hasPayload = true
			payloadCol = "data"
		case "value":
			if payloadCol == "" {
				hasPayload = true
				payloadCol = "value"
			}
		case "_metadata":
			hasMetadata = true
		}
	}
	if !hasPayload || hasMetadata {
		return false, ""
	}
	for _, col := range columns {
		if !isReservedFlattenPayloadColumn(col.Name, "data", "value") {
			return true, payloadCol
		}
	}
	return false, ""
}
