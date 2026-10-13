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
	"errors"
	"fmt"
	"strings"
)

const (
	hiveParquetInputFormat  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
	hiveParquetOutputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
	hiveParquetSerDe        = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
)

var (
	errHiveNotFound      = errors.New("hive object not found")
	errHiveAlreadyExists = errors.New("hive object already exists")
)

type hiveField struct {
	Name string
	Type string
}

type hiveTableMeta struct {
	DB            string
	Name          string
	Location      string
	InputFormat   string
	OutputFormat  string
	SerDe         string
	Columns       []hiveField
	PartitionKeys []hiveField
	TableType     string
	Parameters    map[string]string
}

type hivePartitionMeta struct {
	DB       string
	Table    string
	Values   []string
	Location string
}

func hiveFormatSupported(table hiveTableMeta) error {
	if strings.EqualFold(table.Parameters["transactional"], "true") {
		return fmt.Errorf("Hive ACID tables are not supported")
	}
	blob := strings.ToLower(table.InputFormat + " " + table.SerDe)
	switch {
	case strings.Contains(blob, "orc"):
		return fmt.Errorf("Hive ORC tables are not supported")
	case strings.Contains(blob, "avro"):
		return fmt.Errorf("Hive Avro tables are not supported")
	case strings.Contains(blob, "text"):
		return fmt.Errorf("Hive text tables are not supported")
	case strings.Contains(blob, "parquet"):
		return nil
	default:
		if strings.TrimSpace(table.InputFormat) == "" && strings.TrimSpace(table.SerDe) == "" {
			return fmt.Errorf("Hive table %s.%s has no storage format", table.DB, table.Name)
		}
		return fmt.Errorf("unsupported Hive storage format %q (only Parquet is supported)", table.InputFormat)
	}
}

func canonicalHiveType(t string) string {
	t = strings.ToLower(strings.TrimSpace(t))
	switch {
	case t == "string" || strings.HasPrefix(t, "varchar") || strings.HasPrefix(t, "char"):
		return "string"
	case strings.HasPrefix(t, "decimal"):
		return "string"
	case t == "int" || t == "integer" || t == "tinyint" || t == "smallint":
		return "int"
	case t == "bigint" || t == "long":
		return "bigint"
	case t == "float":
		return "float"
	case t == "double" || t == "double precision":
		return "double"
	case t == "boolean" || t == "bool":
		return "boolean"
	case t == "timestamp" || strings.HasPrefix(t, "timestamp"):
		return "timestamp"
	case t == "date":
		return "date"
	default:
		return t
	}
}

func supportedHiveType(t string) bool {
	switch canonicalHiveType(t) {
	case "string", "int", "bigint", "float", "double", "boolean", "timestamp", "date":
		return true
	default:
		return false
	}
}

func hivePartitionPath(keys []hiveField, values []string) (string, error) {
	if len(keys) != len(values) {
		return "", fmt.Errorf("partition value count %d does not match keys %d", len(values), len(keys))
	}
	parts := make([]string, len(keys))
	for i, key := range keys {
		if strings.ContainsAny(values[i], "/=") {
			return "", fmt.Errorf("invalid partition value %q", values[i])
		}
		parts[i] = key.Name + "=" + values[i]
	}
	return strings.Join(parts, "/"), nil
}

func hiveRawColumns() []hiveField {
	return []hiveField{{Name: "data", Type: "string"}, {Name: "_metadata", Type: "string"}}
}

func hiveTableIsRaw(cols []hiveField) bool {
	if len(cols) != 2 {
		return false
	}
	return cols[0].Name == "data" && cols[1].Name == "_metadata"
}
