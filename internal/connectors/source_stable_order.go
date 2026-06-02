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
	"strings"

	"github.com/dataflow-operator/dataflow/internal/types"
)

const (
	defaultOrderByColumn     = "id"
	stableOrderSubqueryAlias = "__dataflow_src"
)

// ResolveOrderByColumn returns the configured order-by column or default "id".
func ResolveOrderByColumn(configured string) string {
	if configured != "" {
		return configured
	}
	return defaultOrderByColumn
}

// WrapQueryStableOrder wraps a user query in a subquery with ORDER BY for stable pagination.
// orderByParts are passed through to SQL as-is (caller handles quoting per dialect).
func WrapQueryStableOrder(userQuery string, orderByParts ...string) string {
	if len(orderByParts) == 0 {
		return strings.TrimSpace(userQuery)
	}
	orderClause := strings.Join(orderByParts, ", ")
	return "SELECT * FROM (" + strings.TrimSpace(userQuery) + ") AS " + stableOrderSubqueryAlias +
		" ORDER BY " + orderClause
}

// ColumnIndex returns the index of col in names, or -1 if not found.
func ColumnIndex(names []string, col string) int {
	for i, n := range names {
		if n == col {
			return i
		}
	}
	return -1
}

// SetSourceRowIDMetadata stores the row key in msg.Metadata["id"] for downstream sinks and MessageID.
func SetSourceRowIDMetadata(msg *types.Message, rowID interface{}) {
	if msg == nil || rowID == nil {
		return
	}
	if msg.Metadata == nil {
		msg.Metadata = make(map[string]interface{})
	}
	msg.Metadata["id"] = rowID
}
