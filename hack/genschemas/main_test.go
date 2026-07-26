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

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/pkg/providers"
	"github.com/dataflow-operator/dataflow/pkg/transformtypes"
)

func TestStructToSchema_KafkaSourceRequired(t *testing.T) {
	schema := structToSchema(v1.KafkaSourceSpec{}, "kafka source", "test")
	req, _ := schema["required"].([]string)
	want := map[string]bool{"brokers": true, "topic": true}
	for _, r := range req {
		delete(want, r)
	}
	if len(want) != 0 {
		t.Fatalf("missing required fields %v in %v", want, req)
	}
	props, _ := schema["properties"].(map[string]any)
	if props["brokers"] == nil || props["topic"] == nil {
		t.Fatalf("expected brokers/topic properties, got %#v", props)
	}
}

func TestStructToSchema_InlineFlatten(t *testing.T) {
	schema := structToSchema(v1.NessieSinkSpec{}, "nessie sink", "test")
	props, _ := schema["properties"].(map[string]any)
	if props["flattenMetadataColumns"] == nil {
		t.Fatalf("expected inlined flattenMetadataColumns, props=%v", keysOf(props))
	}
}

func TestParseJSONTag(t *testing.T) {
	name, omit, inline := parseJSONTag("foo,omitempty", "Foo")
	if name != "foo" || !omit || inline {
		t.Fatalf("got %q omit=%v inline=%v", name, omit, inline)
	}
	name, omit, inline = parseJSONTag(",inline", "Embed")
	if name != "embed" || omit || !inline {
		t.Fatalf("got %q omit=%v inline=%v", name, omit, inline)
	}
}

func TestGenerate_WritesCatalogAndAlignsProviders(t *testing.T) {
	repoRoot := findRepoRoot(t)
	out := t.TempDir()
	crdDir := filepath.Join(repoRoot, "dataflow", "config", "crd", "bases")
	chart := filepath.Join(repoRoot, "helm-charts", "charts", "dataflow-operator", "Chart.yaml")

	if err := generate(out, crdDir, chart); err != nil {
		t.Fatalf("generate: %v", err)
	}

	for _, name := range []string{"dataflow.json", "dataflowcron.json", "catalog.json"} {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}

	raw, err := os.ReadFile(filepath.Join(out, "catalog.json"))
	if err != nil {
		t.Fatal(err)
	}
	var catalog map[string]any
	if err := json.Unmarshal(raw, &catalog); err != nil {
		t.Fatal(err)
	}
	if catalog["operatorVersion"] == "" {
		t.Fatal("operatorVersion empty")
	}

	sources, _ := catalog["sources"].(map[string]any)
	sinks, _ := catalog["sinks"].(map[string]any)
	transforms, _ := catalog["transformations"].(map[string]any)

	assertKeysMatch(t, "sources", keysOfAny(sources), providers.ListSourceTypes())
	assertKeysMatch(t, "sinks", keysOfAny(sinks), providers.ListSinkTypes())
	assertKeysMatch(t, "transforms", keysOfAny(transforms), transformtypes.All())

	// Spot-check one connector schema file exists and is valid JSON.
	kafkaPath, _ := sources["kafka"].(string)
	body, err := os.ReadFile(filepath.Join(out, kafkaPath))
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal(body, &schema); err != nil {
		t.Fatal(err)
	}
	if schema["type"] != "object" {
		t.Fatalf("expected object schema, got %v", schema["type"])
	}
}

func TestCRDToJSONSchema(t *testing.T) {
	repoRoot := findRepoRoot(t)
	path := filepath.Join(repoRoot, "dataflow", "config", "crd", "bases", "dataflow.dataflow.io_dataflows.yaml")
	schema, err := crdToJSONSchema(path, "DataFlow", "https://example.com/dataflow.json")
	if err != nil {
		t.Fatal(err)
	}
	if schema["$id"] != "https://example.com/dataflow.json" {
		t.Fatalf("$id = %v", schema["$id"])
	}
	props, _ := schema["properties"].(map[string]any)
	if props["spec"] == nil {
		t.Fatal("expected spec property from CRD")
	}
}

func assertKeysMatch(t *testing.T, label string, got, want []string) {
	t.Helper()
	if !equalStringSets(got, want) {
		t.Fatalf("%s keys %v != %v", label, got, want)
	}
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func keysOfAny(m map[string]any) []string {
	return keysOf(m)
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := wd
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "helm-charts", "charts", "dataflow-operator", "Chart.yaml")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatal("repo root not found")
	return ""
}
