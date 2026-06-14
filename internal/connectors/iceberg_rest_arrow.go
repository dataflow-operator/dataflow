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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/dataflow-operator/dataflow/internal/types"
)

// arrowTableToMessages converts an Arrow table to types.Message slice.
func arrowTableToMessages(tbl arrow.Table, namespace, tableName string, rawMode bool) []*types.Message {
	if tbl.NumRows() == 0 {
		return nil
	}
	schema := tbl.Schema()
	cols := schema.Fields()
	nCols := int(tbl.NumCols())
	colArrs := make([]arrow.Array, nCols)
	for i := 0; i < nCols; i++ {
		chunked := tbl.Column(i).Data()
		chunks := chunked.Chunks()
		arr, err := array.Concatenate(chunks, memory.DefaultAllocator)
		if err != nil {
			return nil
		}
		defer arr.Release()
		colArrs[i] = arr
	}
	msgs := make([]*types.Message, 0, tbl.NumRows())
	for r := int64(0); r < tbl.NumRows(); r++ {
		rowMap := make(map[string]interface{})
		for i, f := range cols {
			if colArrs[i].Len() <= int(r) {
				continue
			}
			v := valueAt(colArrs[i], int(r))
			rowMap[f.Name] = v
		}
		var jsonData []byte
		var err error
		isFlatten, flattenPrefix, metaCols := detectFlattenMetadataFromArrowFields(cols)
		if isFlatten {
			var value interface{}
			dataVal := rowMap["data"]
			if s, ok := dataVal.(string); ok {
				if uerr := json.Unmarshal([]byte(s), &value); uerr != nil {
					value = dataVal
				}
			} else {
				value = dataVal
			}
			meta := make(map[string]interface{})
			for _, col := range metaCols {
				key := metadataKeyFromColumn(col, flattenPrefix)
				meta[key] = rowMap[col]
			}
			jsonData, err = buildRawModeJSON(value, meta)
			if err != nil {
				continue
			}
			msg := types.NewMessage(jsonData)
			for k, v := range meta {
				msg.Metadata[k] = v
			}
			msg.Metadata["namespace"] = namespace
			msg.Metadata["table"] = tableName
			msgs = append(msgs, msg)
			continue
		}
		if rawMode {
			metadata := map[string]interface{}{"namespace": namespace, "table": tableName}
			jsonData, err = buildRawModeJSON(rowMap, metadata)
		} else {
			jsonData, err = json.Marshal(rowMap)
		}
		if err != nil {
			continue
		}
		msg := types.NewMessage(jsonData)
		msg.Metadata["namespace"] = namespace
		msg.Metadata["table"] = tableName
		msgs = append(msgs, msg)
	}
	return msgs
}

func valueAt(arr arrow.Array, i int) interface{} {
	if arr.IsNull(i) {
		return nil
	}
	switch a := arr.(type) {
	case *array.String:
		return a.Value(i)
	case *array.Int64:
		return a.Value(i)
	case *array.Int32:
		return a.Value(i)
	case *array.Float64:
		return a.Value(i)
	case *array.Float32:
		return a.Value(i)
	case *array.Boolean:
		return a.Value(i)
	case *array.Timestamp:
		if a.IsNull(i) {
			return nil
		}
		tsType, ok := a.DataType().(*arrow.TimestampType)
		if !ok {
			return a.Value(i)
		}
		toTime, err := tsType.GetToTimeFunc()
		if err != nil {
			return a.Value(i)
		}
		return toTime(a.Value(i)).UTC()
	case *array.Binary:
		return a.Value(i)
	case *array.LargeString:
		return a.Value(i)
	default:
		return arr.ValueStr(i)
	}
}

// messagesToArrowTable builds an Arrow table from messages.
func messagesToArrowTable(msgs []*types.Message, rawMode bool) (arrow.Table, error) {
	mem := memory.DefaultAllocator
	if rawMode {
		dataBuilder := array.NewStringBuilder(mem)
		metaBuilder := array.NewStringBuilder(mem)
		defer dataBuilder.Release()
		defer metaBuilder.Release()
		for _, m := range msgs {
			dataStr, metaStr := extractDataAndMetadata(m)
			dataBuilder.Append(dataStr)
			metaBuilder.Append(metaStr)
		}
		dataArr := dataBuilder.NewArray()
		metaArr := metaBuilder.NewArray()
		defer dataArr.Release()
		defer metaArr.Release()

		schema := arrow.NewSchema([]arrow.Field{
			{Name: "data", Type: arrow.BinaryTypes.String},
			{Name: "_metadata", Type: arrow.BinaryTypes.String},
		}, nil)
		rec := array.NewRecord(schema, []arrow.Array{dataArr, metaArr}, int64(len(msgs)))
		defer rec.Release()
		return array.NewTableFromRecords(schema, []arrow.Record{rec}), nil
	}

	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	for _, m := range msgs {
		builder.Append(string(m.Data))
	}
	arr := builder.NewArray()
	defer arr.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "data", Type: arrow.BinaryTypes.String}}, nil)
	rec := array.NewRecord(schema, []arrow.Array{arr}, int64(len(msgs)))
	defer rec.Release()
	return array.NewTableFromRecords(schema, []arrow.Record{rec}), nil
}
