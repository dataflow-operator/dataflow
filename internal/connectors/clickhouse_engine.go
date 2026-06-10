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

const (
	clickHouseEngineMergeTree          = "MergeTree"
	clickHouseEngineReplacingMergeTree = "ReplacingMergeTree"
)

func clickHouseUpsertEnabled(spec *v1.ClickHouseSinkSpec) bool {
	return spec.UpsertMode != nil && *spec.UpsertMode
}

func resolveClickHouseTableEngine(spec *v1.ClickHouseSinkSpec) string {
	if spec.TableEngine != nil && *spec.TableEngine != "" {
		return *spec.TableEngine
	}
	if clickHouseUpsertEnabled(spec) {
		return clickHouseEngineReplacingMergeTree
	}
	return clickHouseEngineMergeTree
}

func resolveClickHouseOrderByColumns(spec *v1.ClickHouseSinkSpec, messageColumns []string) []string {
	if spec.ConflictKey != nil && strings.TrimSpace(*spec.ConflictKey) != "" {
		return []string{strings.TrimSpace(*spec.ConflictKey)}
	}
	if len(messageColumns) > 0 {
		return []string{messageColumns[0]}
	}
	return []string{"created_at"}
}

func quoteClickHouseColumns(cols []string) string {
	quoted := make([]string, len(cols))
	for i, col := range cols {
		quoted[i] = fmt.Sprintf("`%s`", col)
	}
	return strings.Join(quoted, ", ")
}

// buildClickHouseCreateEngineClause returns ENGINE and ORDER BY clauses for CREATE TABLE.
func buildClickHouseCreateEngineClause(spec *v1.ClickHouseSinkSpec, orderByColumns []string) (engineClause, orderByClause string) {
	engine := resolveClickHouseTableEngine(spec)
	switch engine {
	case clickHouseEngineReplacingMergeTree:
		if spec.ReplacingVersionColumn != nil && strings.TrimSpace(*spec.ReplacingVersionColumn) != "" {
			engineClause = fmt.Sprintf("ENGINE = ReplacingMergeTree(`%s`)", strings.TrimSpace(*spec.ReplacingVersionColumn))
		} else {
			engineClause = "ENGINE = ReplacingMergeTree()"
		}
	default:
		engineClause = "ENGINE = MergeTree()"
	}
	orderByClause = "ORDER BY " + quoteClickHouseColumns(orderByColumns)
	return engineClause, orderByClause
}
