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

// Command genschemas exports machine-readable JSON Schemas for DataFlow Operator.
//
// Outputs (relative to -out):
//   - dataflow.json / dataflowcron.json — CRD OpenAPI as JSON Schema
//   - connectors/sources|sinks/<type>.json — typed connector config schemas
//   - transforms/<type>.json — typed transformation config schemas
//   - catalog.json — allow-list index + operator appVersion
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/pkg/providers"
	"github.com/dataflow-operator/dataflow/pkg/transformtypes"
)

type connectorEntry struct {
	Type        string
	Sample      any
	Description string
}

type transformEntry struct {
	Type        string
	Sample      any
	Description string
}

func main() {
	outDir := flag.String("out", "", "output directory for schemas (required)")
	crdDir := flag.String("crd-dir", "", "directory with CRD bases (required)")
	chartYAML := flag.String("chart", "", "Helm Chart.yaml path for appVersion (required)")
	flag.Parse()

	if *outDir == "" || *crdDir == "" || *chartYAML == "" {
		fmt.Fprintln(os.Stderr, "usage: genschemas -out DIR -crd-dir DIR -chart Chart.yaml")
		flag.PrintDefaults()
		os.Exit(2)
	}

	if err := generate(*outDir, *crdDir, *chartYAML); err != nil {
		fmt.Fprintf(os.Stderr, "genschemas: %v\n", err)
		os.Exit(1)
	}
}

func generate(outDir, crdDir, chartYAML string) error {
	appVersion, err := readAppVersion(chartYAML)
	if err != nil {
		return err
	}

	sources := sourceEntries()
	sinks := sinkEntries()
	transforms := transformEntries()

	if err := assertProviderAlignment(sources, sinks); err != nil {
		return err
	}
	if err := assertTransformAlignment(transforms); err != nil {
		return err
	}

	dataflowSchema, err := crdToJSONSchema(
		filepath.Join(crdDir, "dataflow.dataflow.io_dataflows.yaml"),
		"DataFlow",
		"https://dataflow-operator.github.io/docs/schemas/dataflow.json",
	)
	if err != nil {
		return err
	}
	dataflowSchema["x-dataflow-operator-version"] = appVersion
	if err := writeJSONSchema(filepath.Join(outDir, "dataflow.json"), dataflowSchema); err != nil {
		return err
	}

	cronSchema, err := crdToJSONSchema(
		filepath.Join(crdDir, "dataflow.dataflow.io_dataflowcrons.yaml"),
		"DataFlowCron",
		"https://dataflow-operator.github.io/docs/schemas/dataflowcron.json",
	)
	if err != nil {
		return err
	}
	cronSchema["x-dataflow-operator-version"] = appVersion
	if err := writeJSONSchema(filepath.Join(outDir, "dataflowcron.json"), cronSchema); err != nil {
		return err
	}

	sourcePaths := map[string]string{}
	for _, e := range sources {
		rel := filepath.ToSlash(filepath.Join("connectors", "sources", e.Type+".json"))
		schema := structToSchema(e.Sample, e.Type+" source config", e.Description)
		schema["$id"] = "https://dataflow-operator.github.io/docs/schemas/" + rel
		schema["x-dataflow-connector"] = e.Type
		schema["x-dataflow-role"] = "source"
		schema["x-dataflow-operator-version"] = appVersion
		if err := writeJSONSchema(filepath.Join(outDir, rel), schema); err != nil {
			return err
		}
		sourcePaths[e.Type] = rel
	}

	sinkPaths := map[string]string{}
	for _, e := range sinks {
		rel := filepath.ToSlash(filepath.Join("connectors", "sinks", e.Type+".json"))
		schema := structToSchema(e.Sample, e.Type+" sink config", e.Description)
		schema["$id"] = "https://dataflow-operator.github.io/docs/schemas/" + rel
		schema["x-dataflow-connector"] = e.Type
		schema["x-dataflow-role"] = "sink"
		schema["x-dataflow-operator-version"] = appVersion
		if err := writeJSONSchema(filepath.Join(outDir, rel), schema); err != nil {
			return err
		}
		sinkPaths[e.Type] = rel
	}

	transformPaths := map[string]string{}
	for _, e := range transforms {
		rel := filepath.ToSlash(filepath.Join("transforms", e.Type+".json"))
		schema := structToSchema(e.Sample, e.Type+" transformation config", e.Description)
		schema["$id"] = "https://dataflow-operator.github.io/docs/schemas/" + rel
		schema["x-dataflow-transform"] = e.Type
		schema["x-dataflow-operator-version"] = appVersion
		if err := writeJSONSchema(filepath.Join(outDir, rel), schema); err != nil {
			return err
		}
		transformPaths[e.Type] = rel
	}

	catalog := map[string]any{
		"$schema":         "http://json-schema.org/draft-07/schema#",
		"$id":             "https://dataflow-operator.github.io/docs/schemas/catalog.json",
		"title":           "DataFlow Operator type catalog",
		"description":     "Allow-list of connector and transformation types with schema paths. Generated from Go (providers + transformtypes + typed config structs).",
		"operatorVersion": appVersion,
		"crdSchemas": map[string]string{
			"DataFlow":     "dataflow.json",
			"DataFlowCron": "dataflowcron.json",
		},
		"sources":         orderedPathMap(sourcePaths),
		"sinks":           orderedPathMap(sinkPaths),
		"transformations": orderedPathMap(transformPaths),
	}
	data, err := marshalCanonicalJSON(catalog)
	if err != nil {
		return err
	}
	return writeFile(filepath.Join(outDir, "catalog.json"), data)
}

func orderedPathMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for _, k := range sortedKeys(m) {
		out[k] = m[k]
	}
	return out
}

func sourceEntries() []connectorEntry {
	return []connectorEntry{
		{Type: "kafka", Sample: v1.KafkaSourceSpec{}, Description: "Kafka source connector configuration"},
		{Type: "postgresql", Sample: v1.PostgreSQLSourceSpec{}, Description: "PostgreSQL polling source configuration"},
		{Type: "postgresql-cdc", Sample: v1.PostgreSQLCDCSourceSpec{}, Description: "PostgreSQL logical replication (CDC) source configuration"},
		{Type: "trino", Sample: v1.TrinoSourceSpec{}, Description: "Trino source configuration"},
		{Type: "clickhouse", Sample: v1.ClickHouseSourceSpec{}, Description: "ClickHouse source configuration"},
		{Type: "nessie", Sample: v1.NessieSourceSpec{}, Description: "Nessie / Iceberg catalog source configuration"},
		{Type: "iceberg", Sample: v1.IcebergSourceSpec{}, Description: "Apache Iceberg REST catalog source configuration"},
	}
}

func sinkEntries() []connectorEntry {
	return []connectorEntry{
		{Type: "kafka", Sample: v1.KafkaSinkSpec{}, Description: "Kafka sink connector configuration"},
		{Type: "postgresql", Sample: v1.PostgreSQLSinkSpec{}, Description: "PostgreSQL sink configuration"},
		{Type: "trino", Sample: v1.TrinoSinkSpec{}, Description: "Trino sink configuration"},
		{Type: "clickhouse", Sample: v1.ClickHouseSinkSpec{}, Description: "ClickHouse sink configuration"},
		{Type: "nessie", Sample: v1.NessieSinkSpec{}, Description: "Nessie / Iceberg catalog sink configuration"},
		{Type: "iceberg", Sample: v1.IcebergSinkSpec{}, Description: "Apache Iceberg REST catalog sink configuration"},
	}
}

func transformEntries() []transformEntry {
	return []transformEntry{
		{Type: transformtypes.Timestamp, Sample: v1.TimestampTransformation{}, Description: "Add a timestamp field"},
		{Type: transformtypes.Flatten, Sample: v1.FlattenTransformation{}, Description: "Flatten an array into messages"},
		{Type: transformtypes.Filter, Sample: v1.FilterTransformation{}, Description: "Filter messages by condition"},
		{Type: transformtypes.Mask, Sample: v1.MaskTransformation{}, Description: "Mask sensitive fields"},
		{Type: transformtypes.Router, Sample: v1.RouterTransformation{}, Description: "Route messages to alternate sinks"},
		{Type: transformtypes.Select, Sample: v1.SelectTransformation{}, Description: "Select fields"},
		{Type: transformtypes.Remove, Sample: v1.RemoveTransformation{}, Description: "Remove fields"},
		{Type: transformtypes.SnakeCase, Sample: v1.SnakeCaseTransformation{}, Description: "Convert keys to snake_case"},
		{Type: transformtypes.CamelCase, Sample: v1.CamelCaseTransformation{}, Description: "Convert keys to CamelCase"},
		{Type: transformtypes.DebeziumUnwrap, Sample: v1.DebeziumUnwrapTransformation{}, Description: "Unwrap Debezium envelope"},
		{Type: transformtypes.ReplaceField, Sample: v1.ReplaceFieldTransformation{}, Description: "Rename / include / exclude fields"},
		{Type: transformtypes.HeadersToPayload, Sample: v1.HeadersToPayloadTransformation{}, Description: "Copy headers into payload"},
		{Type: transformtypes.StructFlatten, Sample: v1.StructFlattenTransformation{}, Description: "Flatten nested objects"},
		{Type: transformtypes.ExtractField, Sample: v1.ExtractFieldTransformation{}, Description: "Replace payload with one field"},
		{Type: transformtypes.HoistField, Sample: v1.HoistFieldTransformation{}, Description: "Wrap payload under a key"},
		{Type: transformtypes.Cast, Sample: v1.CastTransformation{}, Description: "Cast field types"},
		{Type: transformtypes.Timezone, Sample: v1.TimezoneTransformation{}, Description: "Convert temporal fields timezone"},
		{Type: transformtypes.InsertField, Sample: v1.InsertFieldTransformation{}, Description: "Insert or overwrite fields"},
	}
}

func assertProviderAlignment(sources, sinks []connectorEntry) error {
	wantSources := providers.ListSourceTypes()
	gotSources := make([]string, 0, len(sources))
	for _, e := range sources {
		gotSources = append(gotSources, e.Type)
	}
	if !equalStringSets(wantSources, gotSources) {
		return fmt.Errorf("source schema map %v does not match providers.ListSourceTypes() %v", gotSources, wantSources)
	}

	wantSinks := providers.ListSinkTypes()
	gotSinks := make([]string, 0, len(sinks))
	for _, e := range sinks {
		gotSinks = append(gotSinks, e.Type)
	}
	if !equalStringSets(wantSinks, gotSinks) {
		return fmt.Errorf("sink schema map %v does not match providers.ListSinkTypes() %v", gotSinks, wantSinks)
	}
	return nil
}

func assertTransformAlignment(transforms []transformEntry) error {
	want := transformtypes.All()
	got := make([]string, 0, len(transforms))
	for _, e := range transforms {
		got = append(got, e.Type)
	}
	sort.Strings(got)
	wantSorted := append([]string(nil), want...)
	sort.Strings(wantSorted)
	if !equalStringSets(wantSorted, got) {
		return fmt.Errorf("transform schema map %v does not match transformtypes.All() %v", got, want)
	}
	return nil
}
