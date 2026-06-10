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

const postgresqlUpsertStrategyIfNewer = "ifNewer"

func postgresqlUpsertIfNewer(config *v1.PostgreSQLSinkSpec) bool {
	return config.UpsertStrategy != nil && *config.UpsertStrategy == postgresqlUpsertStrategyIfNewer
}

func postgresqlUpsertVersionColumn(config *v1.PostgreSQLSinkSpec) string {
	if config.UpsertVersionColumn == nil {
		return ""
	}
	return strings.TrimSpace(*config.UpsertVersionColumn)
}

func resolvePostgreSQLConflictKey(config *v1.PostgreSQLSinkSpec) string {
	if config.ConflictKey != nil && *config.ConflictKey != "" {
		return *config.ConflictKey
	}
	return "id"
}

// buildPostgreSQLOnConflictClause builds ON CONFLICT ... DO UPDATE/NOTHING for upsert inserts.
func buildPostgreSQLOnConflictClause(quotedTable, conflictKey string, updateColumns []string, config *v1.PostgreSQLSinkSpec) string {
	quotedConflictKey := quotePostgreSQLIdentifier(conflictKey)
	updateClauses := make([]string, 0, len(updateColumns))
	for _, col := range updateColumns {
		if col == conflictKey {
			continue
		}
		q := quotePostgreSQLIdentifier(col)
		updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", q, q))
	}
	if len(updateClauses) == 0 {
		return fmt.Sprintf("ON CONFLICT (%s) DO NOTHING", quotedConflictKey)
	}
	clause := fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET %s", quotedConflictKey, strings.Join(updateClauses, ", "))
	if postgresqlUpsertIfNewer(config) {
		versionCol := postgresqlUpsertVersionColumn(config)
		if versionCol != "" {
			qVersion := quotePostgreSQLIdentifier(versionCol)
			clause += fmt.Sprintf(" WHERE EXCLUDED.%s > %s.%s", qVersion, quotedTable, qVersion)
		}
	}
	return clause
}
