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
	"fmt"
	"strings"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
)

func trinoUpsertEnabled(config *v1.TrinoSinkSpec) bool {
	return config.UpsertMode != nil && *config.UpsertMode
}

func resolveTrinoConflictKey(config *v1.TrinoSinkSpec) string {
	if config.ConflictKey == nil {
		return ""
	}
	return strings.TrimSpace(*config.ConflictKey)
}

// isTrinoIcebergCatalog returns true when the catalog name indicates an Iceberg catalog.
func isTrinoIcebergCatalog(catalog string) bool {
	return strings.Contains(strings.ToLower(catalog), "iceberg")
}

// buildTrinoMergeQuery builds a MERGE statement for idempotent batch upserts (Iceberg).
func buildTrinoMergeQuery(catalog, schema, table, conflictKey string, columns []TableColumnInfo, valueRows []string) (string, error) {
	if conflictKey == "" {
		return "", fmt.Errorf("conflictKey is required when upsertMode is enabled")
	}
	if len(valueRows) == 0 {
		return "", fmt.Errorf("empty batch")
	}

	targetRef := fmt.Sprintf("%s.%s.%s", catalog, schema, table)
	sourceCols := make([]string, len(columns))
	quotedCols := make([]string, len(columns))
	updateSets := make([]string, 0, len(columns))
	insertCols := make([]string, len(columns))
	insertVals := make([]string, len(columns))

	for i, col := range columns {
		quoted := fmt.Sprintf(`"%s"`, col.Name)
		sourceCols[i] = quoted
		quotedCols[i] = quoted
		insertCols[i] = quoted
		insertVals[i] = fmt.Sprintf("source.%s", quoted)
		if col.Name != conflictKey {
			updateSets = append(updateSets, fmt.Sprintf("%s = source.%s", quoted, quoted))
		}
	}

	onClause := fmt.Sprintf(`target."%s" = source."%s"`, conflictKey, conflictKey)
	sourceColList := strings.Join(sourceCols, ", ")
	valuesClause := strings.Join(valueRows, ", ")

	if len(updateSets) == 0 {
		return fmt.Sprintf(
			`MERGE INTO %s AS target USING (VALUES %s) AS source(%s) ON %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)`,
			targetRef,
			valuesClause,
			sourceColList,
			onClause,
			strings.Join(insertCols, ", "),
			strings.Join(insertVals, ", "),
		), nil
	}

	return fmt.Sprintf(
		`MERGE INTO %s AS target USING (VALUES %s) AS source(%s) ON %s WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)`,
		targetRef,
		valuesClause,
		sourceColList,
		onClause,
		strings.Join(updateSets, ", "),
		strings.Join(insertCols, ", "),
		strings.Join(insertVals, ", "),
	), nil
}
