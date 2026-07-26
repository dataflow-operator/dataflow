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
	"reflect"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// schemaObject is a JSON Schema draft-07 fragment.
type schemaObject map[string]any

func structToSchema(sample any, title, description string) schemaObject {
	t := reflect.TypeOf(sample)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	schema := typeToSchema(t, map[reflect.Type]bool{})
	schema["$schema"] = "http://json-schema.org/draft-07/schema#"
	if title != "" {
		schema["title"] = title
	}
	if description != "" {
		schema["description"] = description
	}
	return schema
}

func typeToSchema(t reflect.Type, visiting map[reflect.Type]bool) schemaObject {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	switch {
	case t == reflect.TypeOf(metav1.Duration{}):
		return schemaObject{
			"type":        "string",
			"description": "Kubernetes duration string (e.g. 30s, 5m)",
		}
	case t == reflect.TypeOf(time.Duration(0)):
		return schemaObject{"type": "string", "description": "Go duration string"}
	case t == reflect.TypeOf(runtime.RawExtension{}):
		return schemaObject{
			"type":                 "object",
			"additionalProperties": true,
			"description":          "Opaque JSON object (Kubernetes RawExtension)",
		}
	case t == reflect.TypeOf(intstr.IntOrString{}):
		return schemaObject{
			"oneOf": []any{
				schemaObject{"type": "integer"},
				schemaObject{"type": "string"},
			},
		}
	}

	switch t.Kind() {
	case reflect.Bool:
		return schemaObject{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return schemaObject{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return schemaObject{"type": "number"}
	case reflect.String:
		return schemaObject{"type": "string"}
	case reflect.Slice, reflect.Array:
		return schemaObject{
			"type":  "array",
			"items": typeToSchema(t.Elem(), visiting),
		}
	case reflect.Map:
		return schemaObject{
			"type":                 "object",
			"additionalProperties": typeToSchema(t.Elem(), visiting),
		}
	case reflect.Struct:
		return structSchema(t, visiting)
	case reflect.Interface:
		return schemaObject{"type": "object", "additionalProperties": true}
	default:
		return schemaObject{"type": "object", "additionalProperties": true}
	}
}

func structSchema(t reflect.Type, visiting map[reflect.Type]bool) schemaObject {
	if visiting[t] {
		// Break cycles (e.g. nested SinkSpec → RawExtension already terminal).
		return schemaObject{"type": "object", "additionalProperties": true}
	}
	visiting[t] = true
	defer delete(visiting, t)

	properties := map[string]any{}
	required := []string{}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" && !field.Anonymous {
			continue // unexported
		}
		jsonTag := field.Tag.Get("json")
		if jsonTag == "-" {
			continue
		}
		name, omitempty, inline := parseJSONTag(jsonTag, field.Name)
		ft := field.Type
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}

		if inline && field.Anonymous && ft.Kind() == reflect.Struct {
			embedded := structSchema(ft, visiting)
			if props, ok := embedded["properties"].(map[string]any); ok {
				for k, v := range props {
					properties[k] = v
				}
			}
			if req, ok := embedded["required"].([]string); ok {
				required = append(required, req...)
			}
			continue
		}

		if name == "" {
			continue
		}
		properties[name] = typeToSchema(field.Type, visiting)
		if !omitempty {
			required = append(required, name)
		}
	}

	out := schemaObject{
		"type":       "object",
		"properties": properties,
	}
	if len(required) > 0 {
		out["required"] = required
	}
	return out
}

func parseJSONTag(tag, fieldName string) (name string, omitempty, inline bool) {
	if tag == "" {
		return lowerFirst(fieldName), false, false
	}
	parts := strings.Split(tag, ",")
	name = parts[0]
	if name == "" {
		name = lowerFirst(fieldName)
	}
	for _, opt := range parts[1:] {
		switch strings.TrimSpace(opt) {
		case "omitempty":
			omitempty = true
		case "inline":
			inline = true
		}
	}
	return name, omitempty, inline
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}

func writeJSONSchema(path string, schema schemaObject) error {
	data, err := marshalCanonicalJSON(schema)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", path, err)
	}
	return writeFile(path, data)
}
