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
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
)

// flattenMetadataSinkState tracks flattened metadata column names for sinks that defer table creation.
type flattenMetadataSinkState struct {
	metaColumnNames     []string
	metaColumnTypes     map[string]flattenValueCategory
	deferredTableCreate bool
}

type flattenValueCategory int

const (
	flattenCategoryString flattenValueCategory = iota
	flattenCategoryInt32
	flattenCategoryInt64
	flattenCategoryBool
	flattenCategoryFloat64
)

func metadataColumnName(key, prefix string) string {
	return prefix + key
}

func metadataKeyFromColumn(column, prefix string) string {
	if prefix != "" && strings.HasPrefix(column, prefix) {
		return strings.TrimPrefix(column, prefix)
	}
	return column
}

func parseMetadataMapFromMessage(msg *types.Message) (map[string]interface{}, error) {
	var parsed map[string]interface{}
	if err := json.Unmarshal(msg.Data, &parsed); err == nil {
		if _, wrapped := parsed["value"]; wrapped && len(parsed) <= 2 {
			if m, ok := parsed["_metadata"].(map[string]interface{}); ok {
				meta := make(map[string]interface{}, len(m))
				for k, v := range m {
					meta[k] = v
				}
				return meta, nil
			}
		}
	}
	// Plain message: use msg.Metadata directly to preserve int32/int64 types (avoid JSON float64 round-trip).
	if msg.Metadata != nil && len(msg.Metadata) > 0 {
		meta := make(map[string]interface{}, len(msg.Metadata))
		for k, v := range msg.Metadata {
			meta[k] = v
		}
		return meta, nil
	}
	_, metaStr := extractDataAndMetadata(msg)
	if metaStr == "" || metaStr == "{}" {
		return map[string]interface{}{}, nil
	}
	var meta map[string]interface{}
	if err := json.Unmarshal([]byte(metaStr), &meta); err != nil {
		return nil, fmt.Errorf("parse metadata JSON: %w", err)
	}
	return meta, nil
}

func extractPayloadDataStr(msg *types.Message) (string, error) {
	dataStr, _ := extractDataAndMetadata(msg)
	return dataStr, nil
}

// collectFlattenMetadataColumnNames returns sorted column names for metadata keys seen in msgs.
func collectFlattenMetadataColumnNames(msgs []*types.Message, prefix string) ([]string, error) {
	seen := make(map[string]struct{})
	for _, msg := range msgs {
		meta, err := parseMetadataMapFromMessage(msg)
		if err != nil {
			return nil, err
		}
		for k := range meta {
			seen[metadataColumnName(k, prefix)] = struct{}{}
		}
	}
	cols := make([]string, 0, len(seen))
	for c := range seen {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return cols, nil
}

func inferFlattenValueCategory(v interface{}) flattenValueCategory {
	switch val := v.(type) {
	case nil:
		return flattenCategoryString
	case bool:
		return flattenCategoryBool
	case int:
		return inferFlattenIntCategory(int64(val))
	case int32:
		return flattenCategoryInt32
	case int64:
		return inferFlattenIntCategory(val)
	case uint, uint32, uint64:
		return flattenCategoryInt64
	case float32:
		if isWholeNumber(float64(val)) {
			return inferFlattenIntCategory(int64(val))
		}
		return flattenCategoryFloat64
	case float64:
		if isWholeNumber(val) {
			return inferFlattenIntCategory(int64(val))
		}
		return flattenCategoryFloat64
	default:
		return flattenCategoryString
	}
}

func inferFlattenIntCategory(v int64) flattenValueCategory {
	if v >= math.MinInt32 && v <= math.MaxInt32 {
		return flattenCategoryInt32
	}
	return flattenCategoryInt64
}

func mergeFlattenCategories(current, next flattenValueCategory) flattenValueCategory {
	rank := func(c flattenValueCategory) int {
		switch c {
		case flattenCategoryBool:
			return 1
		case flattenCategoryInt32:
			return 2
		case flattenCategoryInt64:
			return 3
		case flattenCategoryFloat64:
			return 4
		default:
			return 5
		}
	}
	if rank(next) > rank(current) {
		return next
	}
	return current
}

func flattenCategoryToArrowType(cat flattenValueCategory) arrow.DataType {
	switch cat {
	case flattenCategoryBool:
		return arrow.FixedWidthTypes.Boolean
	case flattenCategoryInt32:
		return arrow.PrimitiveTypes.Int32
	case flattenCategoryInt64:
		return arrow.PrimitiveTypes.Int64
	case flattenCategoryFloat64:
		return arrow.PrimitiveTypes.Float64
	default:
		return arrow.BinaryTypes.String
	}
}

func flattenCategoryToIcebergType(cat flattenValueCategory) iceberg.Type {
	switch cat {
	case flattenCategoryBool:
		return iceberg.PrimitiveTypes.Bool
	case flattenCategoryInt32:
		return iceberg.PrimitiveTypes.Int32
	case flattenCategoryInt64:
		return iceberg.PrimitiveTypes.Int64
	case flattenCategoryFloat64:
		return iceberg.PrimitiveTypes.Float64
	default:
		return iceberg.PrimitiveTypes.String
	}
}

func inferFlattenColumnCategories(msgs []*types.Message, columnNames []string, prefix string) map[string]flattenValueCategory {
	allowed := make(map[string]struct{}, len(columnNames))
	for _, col := range columnNames {
		allowed[col] = struct{}{}
	}
	types := make(map[string]flattenValueCategory, len(columnNames))
	for _, msg := range msgs {
		meta, err := parseMetadataMapFromMessage(msg)
		if err != nil {
			continue
		}
		for k, v := range meta {
			col := metadataColumnName(k, prefix)
			if _, ok := allowed[col]; !ok {
				continue
			}
			cat := inferFlattenValueCategory(v)
			if existing, ok := types[col]; ok {
				types[col] = mergeFlattenCategories(existing, cat)
			} else {
				types[col] = cat
			}
		}
	}
	for _, col := range columnNames {
		if _, ok := types[col]; !ok {
			types[col] = flattenCategoryString
		}
	}
	return types
}

func logSkippedUnknownMetadataKeys(meta map[string]interface{}, knownCols map[string]struct{}, prefix string, logger logr.Logger) {
	for k := range meta {
		col := metadataColumnName(k, prefix)
		if _, ok := knownCols[col]; !ok {
			logger.Info("Skipping unknown metadata key for flattened table schema", "column", col, "key", k)
		}
	}
}

func postgreSQLTypeForCategory(cat flattenValueCategory) string {
	switch cat {
	case flattenCategoryBool:
		return "BOOLEAN"
	case flattenCategoryInt32:
		return "INTEGER"
	case flattenCategoryInt64:
		return "BIGINT"
	case flattenCategoryFloat64:
		return "DOUBLE PRECISION"
	default:
		return "TEXT"
	}
}

func trinoTypeForCategory(flattenValueCategory) string {
	return "VARCHAR(1048576)"
}

func flattenMetadataValueForSQL(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	return v
}

// isReservedFlattenPayloadColumn reports standard payload/system columns not treated as metadata flatten columns.
func isReservedFlattenPayloadColumn(name string, payloadNames ...string) bool {
	lower := strings.ToLower(name)
	for _, p := range payloadNames {
		if strings.EqualFold(lower, p) {
			return true
		}
	}
	switch lower {
	case "_metadata", "id", "created_at", "updated_at", "deleted_at":
		return true
	default:
		return false
	}
}

func detectCommonMetadataPrefix(columnNames []string) string {
	if len(columnNames) <= 1 {
		return ""
	}
	prefix := columnNames[0]
	for _, name := range columnNames[1:] {
		for len(prefix) > 0 && !strings.HasPrefix(name, prefix) {
			prefix = prefix[:len(prefix)-1]
		}
	}
	if prefix == "" {
		return ""
	}
	if !strings.HasSuffix(prefix, "_") {
		lastUnderscore := strings.LastIndex(prefix, "_")
		if lastUnderscore >= 0 {
			prefix = prefix[:lastUnderscore+1]
		}
	}
	return prefix
}
