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
	"math"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
)

func nessieFlattenMetaColumnNamesFromTable(tbl *table.Table) ([]string, error) {
	if tbl == nil || tbl.Schema() == nil {
		return nil, fmt.Errorf("table schema is nil")
	}
	schema := tbl.Schema()
	if _, ok := schema.FindFieldByNameCaseInsensitive("data"); !ok {
		return nil, fmt.Errorf("flattenMetadataColumns requires a \"data\" column in the Iceberg table")
	}
	if _, ok := schema.FindFieldByNameCaseInsensitive("_metadata"); ok {
		return nil, fmt.Errorf("flattenMetadataColumns is incompatible with \"_metadata\" column; recreate the table or disable flattenMetadataColumns")
	}
	var cols []string
	for _, f := range schema.Fields() {
		name := f.Name
		if strings.EqualFold(name, "data") {
			continue
		}
		cols = append(cols, name)
	}
	sort.Strings(cols)
	return cols, nil
}

func validateNessieFlattenMetadataSchema(tbl *table.Table) error {
	_, err := nessieFlattenMetaColumnNamesFromTable(tbl)
	return err
}

func inferIcebergPrimitiveType(v interface{}) iceberg.Type {
	switch val := v.(type) {
	case nil:
		return iceberg.PrimitiveTypes.String
	case bool:
		return iceberg.PrimitiveTypes.Bool
	case int:
		return inferIcebergIntType(int64(val))
	case int32:
		return iceberg.PrimitiveTypes.Int32
	case int64:
		return inferIcebergIntType(val)
	case uint:
		return inferIcebergIntType(int64(val))
	case uint32:
		return iceberg.PrimitiveTypes.Int64
	case uint64:
		return iceberg.PrimitiveTypes.Int64
	case float32:
		if isWholeNumber(float64(val)) {
			return inferIcebergIntType(int64(val))
		}
		return iceberg.PrimitiveTypes.Float64
	case float64:
		if isWholeNumber(val) {
			return inferIcebergIntType(int64(val))
		}
		return iceberg.PrimitiveTypes.Float64
	default:
		return iceberg.PrimitiveTypes.String
	}
}

func inferIcebergIntType(v int64) iceberg.Type {
	if v >= math.MinInt32 && v <= math.MaxInt32 {
		return iceberg.PrimitiveTypes.Int32
	}
	return iceberg.PrimitiveTypes.Int64
}

func icebergTypeRank(t iceberg.Type) int {
	switch t.Type() {
	case "boolean":
		return 1
	case "int":
		return 2
	case "long":
		return 3
	case "float", "double":
		return 4
	case "timestamptz", "timestamp":
		return 5
	default:
		return 6
	}
}

func mergeIcebergTypes(current, next iceberg.Type) iceberg.Type {
	if current.Type() == next.Type() {
		return current
	}
	if icebergTypeRank(next) > icebergTypeRank(current) {
		return next
	}
	return current
}

func inferFlattenColumnTypes(msgs []*types.Message, columnNames []string, prefix string) map[string]iceberg.Type {
	categories := inferFlattenColumnCategories(msgs, columnNames, prefix)
	types := make(map[string]iceberg.Type, len(columnNames))
	for _, col := range columnNames {
		types[col] = flattenCategoryToIcebergType(categories[col])
	}
	return types
}

func nessieIcebergSchemaFlattened(metaColumns []string, colTypes map[string]iceberg.Type) *iceberg.Schema {
	fields := []iceberg.NestedField{
		{ID: 1, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	}
	for i, col := range metaColumns {
		t := colTypes[col]
		if t == nil {
			t = iceberg.PrimitiveTypes.String
		}
		fields = append(fields, iceberg.NestedField{
			ID:       i + 2,
			Name:     col,
			Type:     t,
			Required: false,
		})
	}
	return iceberg.NewSchema(0, fields...)
}

func detectFlattenMetadataFromArrowFields(fields []arrow.Field) (isFlatten bool, prefix string, metaCols []string) {
	hasData := false
	hasMetadata := false
	for _, f := range fields {
		if strings.EqualFold(f.Name, "data") {
			hasData = true
		}
		if strings.EqualFold(f.Name, "_metadata") {
			hasMetadata = true
		}
	}
	if !hasData || hasMetadata {
		return false, "", nil
	}
	for _, f := range fields {
		if !strings.EqualFold(f.Name, "data") {
			metaCols = append(metaCols, f.Name)
		}
	}
	if len(metaCols) == 0 {
		return false, "", nil
	}
	sort.Strings(metaCols)
	return true, detectCommonMetadataPrefix(metaCols), metaCols
}

func arrowTypeForIceberg(t iceberg.Type) arrow.DataType {
	switch t.Type() {
	case "boolean":
		return arrow.FixedWidthTypes.Boolean
	case "int":
		return arrow.PrimitiveTypes.Int32
	case "long":
		return arrow.PrimitiveTypes.Int64
	case "float":
		return arrow.PrimitiveTypes.Float32
	case "double":
		return arrow.PrimitiveTypes.Float64
	case "timestamptz", "timestamp":
		return flattenTimestampArrowType
	default:
		return arrow.BinaryTypes.String
	}
}

func timeToArrowTimestamp(t time.Time) arrow.Timestamp {
	return arrow.Timestamp(t.UTC().UnixMicro())
}

func appendFlattenMetadataValue(b array.Builder, v interface{}) {
	if v == nil {
		b.AppendNull()
		return
	}
	switch builder := b.(type) {
	case *array.StringBuilder:
		builder.Append(fmt.Sprint(v))
	case *array.Int32Builder:
		switch n := v.(type) {
		case int32:
			builder.Append(n)
		case int:
			builder.Append(int32(n))
		case int64:
			builder.Append(int32(n))
		case float64:
			builder.Append(int32(n))
		default:
			builder.AppendNull()
		}
	case *array.Int64Builder:
		switch n := v.(type) {
		case int64:
			builder.Append(n)
		case int:
			builder.Append(int64(n))
		case int32:
			builder.Append(int64(n))
		case float64:
			builder.Append(int64(n))
		default:
			builder.AppendNull()
		}
	case *array.BooleanBuilder:
		if b, ok := v.(bool); ok {
			builder.Append(b)
		} else {
			builder.AppendNull()
		}
	case *array.Float64Builder:
		switch n := v.(type) {
		case float64:
			builder.Append(n)
		case float32:
			builder.Append(float64(n))
		case int:
			builder.Append(float64(n))
		case int32:
			builder.Append(float64(n))
		case int64:
			builder.Append(float64(n))
		default:
			builder.AppendNull()
		}
	case *array.TimestampBuilder:
		if t, ok := parseFlattenTimestampValue(v); ok {
			builder.Append(timeToArrowTimestamp(t))
		} else {
			builder.AppendNull()
		}
	default:
		b.AppendNull()
	}
}

func messagesToArrowTableFlattened(
	msgs []*types.Message,
	metaColumns []string,
	colTypes map[string]iceberg.Type,
	prefix string,
	logger logr.Logger,
) (arrow.Table, error) {
	mem := memory.DefaultAllocator
	dataBuilder := array.NewStringBuilder(mem)
	defer dataBuilder.Release()

	builders := make([]array.Builder, len(metaColumns))
	arrowFields := make([]arrow.Field, 0, 1+len(metaColumns))
	arrowFields = append(arrowFields, arrow.Field{Name: "data", Type: arrow.BinaryTypes.String})
	categories := inferFlattenColumnCategories(msgs, metaColumns, prefix)
	for i, col := range metaColumns {
		at := flattenCategoryToArrowType(categories[col])
		arrowFields = append(arrowFields, arrow.Field{Name: col, Type: at, Nullable: true})
		switch {
		case arrow.TypeEqual(at, arrow.FixedWidthTypes.Boolean):
			builders[i] = array.NewBooleanBuilder(mem)
		case arrow.TypeEqual(at, arrow.PrimitiveTypes.Int32):
			builders[i] = array.NewInt32Builder(mem)
		case arrow.TypeEqual(at, arrow.PrimitiveTypes.Int64):
			builders[i] = array.NewInt64Builder(mem)
		case arrow.TypeEqual(at, arrow.PrimitiveTypes.Float64):
			builders[i] = array.NewFloat64Builder(mem)
		case arrow.TypeEqual(at, flattenTimestampArrowType):
			builders[i] = array.NewTimestampBuilder(mem, flattenTimestampArrowType)
		default:
			builders[i] = array.NewStringBuilder(mem)
		}
	}
	defer func() {
		for _, b := range builders {
			if b != nil {
				b.Release()
			}
		}
	}()

	knownCols := make(map[string]struct{}, len(metaColumns))
	for _, c := range metaColumns {
		knownCols[c] = struct{}{}
	}

	for _, m := range msgs {
		dataStr, _ := extractDataAndMetadata(m)
		dataBuilder.Append(dataStr)
		meta, err := parseMetadataMapFromMessage(m)
		if err != nil {
			return nil, err
		}
		logSkippedUnknownMetadataKeys(meta, knownCols, prefix, logger)
		for i, col := range metaColumns {
			key := metadataKeyFromColumn(col, prefix)
			appendFlattenMetadataValue(builders[i], meta[key])
		}
	}

	arrays := make([]arrow.Array, 0, 1+len(metaColumns))
	dataArr := dataBuilder.NewArray()
	arrays = append(arrays, dataArr)
	for _, b := range builders {
		arrays = append(arrays, b.NewArray())
	}
	defer func() {
		for _, a := range arrays {
			a.Release()
		}
	}()

	schema := arrow.NewSchema(arrowFields, nil)
	rec := array.NewRecord(schema, arrays, int64(len(msgs)))
	defer rec.Release()
	return array.NewTableFromRecords(schema, []arrow.Record{rec}), nil
}
