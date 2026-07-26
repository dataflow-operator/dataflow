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
	"fmt"
	"os"

	"sigs.k8s.io/yaml"
)

// crdToJSONSchema extracts openAPIV3Schema from a CRD YAML file and wraps it as JSON Schema.
func crdToJSONSchema(crdPath, kind, id string) (schemaObject, error) {
	raw, err := os.ReadFile(crdPath)
	if err != nil {
		return nil, fmt.Errorf("read CRD %s: %w", crdPath, err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse CRD %s: %w", crdPath, err)
	}
	spec, _ := doc["spec"].(map[string]any)
	if spec == nil {
		return nil, fmt.Errorf("CRD %s: missing spec", crdPath)
	}
	versions, _ := spec["versions"].([]any)
	if len(versions) == 0 {
		return nil, fmt.Errorf("CRD %s: no versions", crdPath)
	}
	var openAPI map[string]any
	for _, v := range versions {
		vm, _ := v.(map[string]any)
		if vm == nil {
			continue
		}
		schemaWrap, _ := vm["schema"].(map[string]any)
		if schemaWrap == nil {
			continue
		}
		candidate, _ := schemaWrap["openAPIV3Schema"].(map[string]any)
		if candidate != nil {
			openAPI = candidate
			break
		}
	}
	if openAPI == nil {
		return nil, fmt.Errorf("CRD %s: openAPIV3Schema not found", crdPath)
	}

	out := schemaObject{}
	for k, v := range openAPI {
		out[k] = v
	}
	out["$schema"] = "http://json-schema.org/draft-07/schema#"
	out["$id"] = id
	if _, ok := out["title"]; !ok {
		out["title"] = kind
	}
	return out, nil
}
