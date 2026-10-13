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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func arrowTypeForHive(hiveType string) (arrow.DataType, error) {
	switch canonicalHiveType(hiveType) {
	case "string":
		return arrow.BinaryTypes.String, nil
	case "int":
		return arrow.PrimitiveTypes.Int32, nil
	case "bigint":
		return arrow.PrimitiveTypes.Int64, nil
	case "float":
		return arrow.PrimitiveTypes.Float32, nil
	case "double":
		return arrow.PrimitiveTypes.Float64, nil
	case "boolean":
		return arrow.FixedWidthTypes.Boolean, nil
	case "timestamp":
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil
	case "date":
		return arrow.FixedWidthTypes.Date32, nil
	default:
		return nil, fmt.Errorf("unsupported Hive column type %q", hiveType)
	}
}

func encodeParquet(cols []hiveField, rows []map[string]interface{}) ([]byte, error) {
	fields := make([]arrow.Field, len(cols))
	for i, col := range cols {
		dt, err := arrowTypeForHive(col.Type)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col.Name, err)
		}
		fields[i] = arrow.Field{Name: col.Name, Type: dt, Nullable: true}
	}
	schema := arrow.NewSchema(fields, nil)
	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	for _, row := range rows {
		for i, col := range cols {
			if err := appendHiveValue(builder.Field(i), col.Type, row[col.Name]); err != nil {
				return nil, fmt.Errorf("column %s: %w", col.Name, err)
			}
		}
	}
	rec := builder.NewRecord()
	defer rec.Release()

	var buf bytes.Buffer
	fw, err := pqarrow.NewFileWriter(schema, &buf, parquet.NewWriterProperties(parquet.WithAllocator(mem)), pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}
	if err := fw.Write(rec); err != nil {
		_ = fw.Close()
		return nil, err
	}
	if err := fw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeParquetRows(data []byte) ([]map[string]interface{}, error) {
	mem := memory.NewGoAllocator()
	reader, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(parquet.NewReaderProperties(mem)))
	if err != nil {
		return nil, fmt.Errorf("open parquet: %w", err)
	}
	defer reader.Close()
	fr, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		return nil, err
	}
	tbl, err := fr.ReadTable(context.Background())
	if err != nil {
		return nil, fmt.Errorf("read parquet: %w", err)
	}
	defer tbl.Release()

	tr := array.NewTableReader(tbl, tbl.NumRows())
	defer tr.Release()
	var rows []map[string]interface{}
	for tr.Next() {
		rec := tr.Record()
		for i := 0; i < int(rec.NumRows()); i++ {
			row := make(map[string]interface{}, rec.NumCols())
			for c := 0; c < int(rec.NumCols()); c++ {
				val, err := arrowArrayValue(rec.Column(c), i)
				if err != nil {
					return nil, fmt.Errorf("column %s: %w", rec.ColumnName(c), err)
				}
				row[rec.ColumnName(c)] = val
			}
			rows = append(rows, row)
		}
	}
	if err := tr.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

func appendHiveValue(b array.Builder, hiveType string, v interface{}) error {
	if v == nil {
		b.AppendNull()
		return nil
	}
	switch canonicalHiveType(hiveType) {
	case "string":
		b.(*array.StringBuilder).Append(asHiveString(v))
	case "int":
		n, ok := asInt64(v)
		if !ok {
			return fmt.Errorf("cannot convert %T to int", v)
		}
		b.(*array.Int32Builder).Append(int32(n))
	case "bigint":
		n, ok := asInt64(v)
		if !ok {
			return fmt.Errorf("cannot convert %T to bigint", v)
		}
		b.(*array.Int64Builder).Append(n)
	case "float":
		n, ok := asFloat64(v)
		if !ok {
			return fmt.Errorf("cannot convert %T to float", v)
		}
		b.(*array.Float32Builder).Append(float32(n))
	case "double":
		n, ok := asFloat64(v)
		if !ok {
			return fmt.Errorf("cannot convert %T to double", v)
		}
		b.(*array.Float64Builder).Append(n)
	case "boolean":
		bv, ok := v.(bool)
		if !ok {
			return fmt.Errorf("cannot convert %T to boolean", v)
		}
		b.(*array.BooleanBuilder).Append(bv)
	case "timestamp":
		ts, err := asTime(v)
		if err != nil {
			return err
		}
		b.(*array.TimestampBuilder).Append(arrow.Timestamp(ts.UTC().UnixMicro()))
	case "date":
		ts, err := asTime(v)
		if err != nil {
			if s, ok := v.(string); ok {
				parsed, perr := time.Parse("2006-01-02", s)
				if perr != nil {
					return perr
				}
				ts = parsed
			} else {
				return err
			}
		}
		utc := ts.UTC()
		days := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC).Unix() / 86400
		b.(*array.Date32Builder).Append(arrow.Date32(days))
	default:
		return fmt.Errorf("unsupported Hive column type %q", hiveType)
	}
	return nil
}

func arrowArrayValue(col arrow.Array, i int) (interface{}, error) {
	if col.IsNull(i) {
		return nil, nil
	}
	switch a := col.(type) {
	case *array.String:
		return a.Value(i), nil
	case *array.LargeString:
		return a.Value(i), nil
	case *array.Int32:
		return int64(a.Value(i)), nil
	case *array.Int64:
		return a.Value(i), nil
	case *array.Float32:
		return float64(a.Value(i)), nil
	case *array.Float64:
		return a.Value(i), nil
	case *array.Boolean:
		return a.Value(i), nil
	case *array.Timestamp:
		unit := arrow.Microsecond
		if dt, ok := a.DataType().(*arrow.TimestampType); ok {
			unit = dt.Unit
		}
		return a.Value(i).ToTime(unit).UTC().Format(time.RFC3339Nano), nil
	case *array.Date32:
		days := int64(a.Value(i))
		return time.Unix(days*86400, 0).UTC().Format("2006-01-02"), nil
	case *array.Binary:
		return base64.StdEncoding.EncodeToString(a.Value(i)), nil
	default:
		return nil, fmt.Errorf("unsupported parquet value type %T", col)
	}
}

func asHiveString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprint(t)
		}
		return string(b)
	}
}

func asInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		if n == math.Trunc(n) && !math.IsNaN(n) && !math.IsInf(n, 0) {
			return int64(n), true
		}
	case float32:
		f := float64(n)
		if f == math.Trunc(f) {
			return int64(f), true
		}
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	case string:
		i, err := strconv.ParseInt(n, 10, 64)
		return i, err == nil
	}
	return 0, false
}

func asFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	}
	return 0, false
}

func asTime(v interface{}) (time.Time, error) {
	switch t := v.(type) {
	case time.Time:
		return t, nil
	case string:
		if ts, err := time.Parse(time.RFC3339Nano, t); err == nil {
			return ts, nil
		}
		if ts, err := time.Parse("2006-01-02 15:04:05", t); err == nil {
			return ts, nil
		}
		return time.Parse("2006-01-02", t)
	case float64:
		return time.Unix(int64(t), 0).UTC(), nil
	case int64:
		return time.Unix(t, 0).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to timestamp", v)
	}
}
