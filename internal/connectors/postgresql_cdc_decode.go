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
	"sync"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

type postgresCDCRelationCache struct {
	mu        sync.RWMutex
	relations map[uint32]*pglogrepl.RelationMessageV2
}

func newPostgresCDCRelationCache() *postgresCDCRelationCache {
	return &postgresCDCRelationCache{
		relations: make(map[uint32]*pglogrepl.RelationMessageV2),
	}
}

// put stores relation metadata and reports whether the column layout changed (schema evolution).
func (c *postgresCDCRelationCache) put(rel *pglogrepl.RelationMessageV2) (schemaChanged bool) {
	if rel == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	prev, exists := c.relations[rel.RelationID]
	c.relations[rel.RelationID] = rel
	if !exists {
		return false
	}
	return relationSchemaChanged(prev, rel)
}

func relationSchemaChanged(prev, next *pglogrepl.RelationMessageV2) bool {
	if prev == nil || next == nil {
		return false
	}
	if prev.Namespace != next.Namespace || prev.RelationName != next.RelationName {
		return true
	}
	if len(prev.Columns) != len(next.Columns) {
		return true
	}
	for i := range prev.Columns {
		if prev.Columns[i].Name != next.Columns[i].Name ||
			prev.Columns[i].DataType != next.Columns[i].DataType {
			return true
		}
	}
	return false
}

func (c *postgresCDCRelationCache) get(id uint32) (*pglogrepl.RelationMessageV2, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	rel, ok := c.relations[id]
	return rel, ok
}

type postgresCDCColumnFilter struct {
	include map[string]struct{}
	exclude map[string]struct{}
}

func newPostgresCDCColumnFilter(cfg *v1.PostgreSQLCDCSourceSpec) *postgresCDCColumnFilter {
	f := &postgresCDCColumnFilter{}
	if cfg == nil {
		return f
	}
	if len(cfg.IncludeColumns) > 0 {
		f.include = make(map[string]struct{}, len(cfg.IncludeColumns))
		for _, col := range cfg.IncludeColumns {
			f.include[strings.ToLower(col)] = struct{}{}
		}
	}
	if len(cfg.ExcludeColumns) > 0 {
		f.exclude = make(map[string]struct{}, len(cfg.ExcludeColumns))
		for _, col := range cfg.ExcludeColumns {
			f.exclude[strings.ToLower(col)] = struct{}{}
		}
	}
	return f
}

func (f *postgresCDCColumnFilter) keep(name string) bool {
	lower := strings.ToLower(name)
	if f.include != nil {
		if _, ok := f.include[lower]; !ok {
			return false
		}
	}
	if f.exclude != nil {
		if _, ok := f.exclude[lower]; ok {
			return false
		}
	}
	return true
}

func decodeTextColumnData(typeMap *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		val, err := dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
		if err != nil {
			return nil, err
		}
		return normalizeCDCDecodedValue(val), nil
	}
	return string(data), nil
}

func normalizeCDCDecodedValue(val interface{}) interface{} {
	switch v := val.(type) {
	case pgtype.Numeric:
		f, err := v.Float64Value()
		if err == nil && f.Valid {
			return f.Float64
		}
		if s, err := v.Value(); err == nil {
			return s
		}
	case pgtype.UUID:
		return v.String()
	}
	return val
}

func tupleToRow(rel *pglogrepl.RelationMessageV2, tuple *pglogrepl.TupleData, typeMap *pgtype.Map, filter *postgresCDCColumnFilter) (map[string]interface{}, error) {
	if rel == nil || tuple == nil {
		return nil, fmt.Errorf("relation or tuple is nil")
	}
	values := make(map[string]interface{}, len(rel.Columns))
	for idx, col := range tuple.Columns {
		if idx >= len(rel.Columns) {
			break
		}
		colMeta := rel.Columns[idx]
		if filter != nil && !filter.keep(colMeta.Name) {
			continue
		}
		switch col.DataType {
		case 'n':
			values[colMeta.Name] = nil
		case 'u':
			// unchanged TOAST — omit from payload
		case 't':
			val, err := decodeTextColumnData(typeMap, col.Data, colMeta.DataType)
			if err != nil {
				return nil, fmt.Errorf("decode column %q: %w", colMeta.Name, err)
			}
			values[colMeta.Name] = val
		default:
			return nil, fmt.Errorf("unsupported column data type %q for %q", col.DataType, colMeta.Name)
		}
	}
	return values, nil
}

func relationTableRef(rel *pglogrepl.RelationMessageV2) string {
	if rel == nil {
		return ""
	}
	return rel.Namespace + "." + rel.RelationName
}

func tableInConfig(tableRef string, tables []string) bool {
	for _, t := range tables {
		if t == tableRef {
			return true
		}
	}
	return false
}

func postgresCDCPrimaryKeyColumn(cfg *v1.PostgreSQLCDCSourceSpec) string {
	if cfg == nil || cfg.PrimaryKeyColumn == "" {
		return "id"
	}
	return cfg.PrimaryKeyColumn
}

func postgresCDCEnvelopeDebezium(cfg *v1.PostgreSQLCDCSourceSpec) bool {
	return cfg != nil && cfg.EnvelopeFormat == "debezium"
}

func debeziumOp(operation string, snapshot bool) string {
	if snapshot && operation == "insert" {
		return "r"
	}
	switch operation {
	case "insert":
		return "c"
	case "update":
		return "u"
	case "delete":
		return "d"
	default:
		return "c"
	}
}

func buildDebeziumEnvelope(
	after, before map[string]interface{},
	table, operation string,
	lsn pglogrepl.LSN,
	snapshot bool,
) map[string]interface{} {
	schema, tableName := ParseTableRef(table)
	payload := map[string]interface{}{
		"before": before,
		"after":  after,
		"op":     debeziumOp(operation, snapshot),
		"source": map[string]interface{}{
			"schema": schema,
			"table":  tableName,
			"lsn":    lsn.String(),
		},
	}
	if operation == "insert" {
		payload["before"] = nil
	} else if operation == "delete" {
		payload["after"] = nil
	}
	return map[string]interface{}{"payload": payload}
}
