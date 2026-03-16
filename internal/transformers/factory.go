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

package transformers

import (
	"encoding/json"
	"fmt"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// transformerEntry defines how to create a transformer from raw config.
type transformerEntry struct {
	create func(raw *runtime.RawExtension) (Transformer, error)
}

var transformerRegistry = map[string]transformerEntry{
	"timestamp": {create: createTransformer[v1.TimestampTransformation]("timestamp", func(cfg *v1.TimestampTransformation) Transformer { return NewTimestampTransformer(cfg) })},
	"flatten":   {create: createTransformer[v1.FlattenTransformation]("flatten", func(cfg *v1.FlattenTransformation) Transformer { return NewFlattenTransformer(cfg) })},
	"filter":    {create: createTransformer[v1.FilterTransformation]("filter", func(cfg *v1.FilterTransformation) Transformer { return NewFilterTransformer(cfg) })},
	"mask":      {create: createTransformer[v1.MaskTransformation]("mask", func(cfg *v1.MaskTransformation) Transformer { return NewMaskTransformer(cfg) })},
	"router":    {create: createTransformer[v1.RouterTransformation]("router", func(cfg *v1.RouterTransformation) Transformer { return NewRouterTransformer(cfg) })},
	"select":    {create: createTransformer[v1.SelectTransformation]("select", func(cfg *v1.SelectTransformation) Transformer { return NewSelectTransformer(cfg) })},
	"remove":    {create: createTransformer[v1.RemoveTransformation]("remove", func(cfg *v1.RemoveTransformation) Transformer { return NewRemoveTransformer(cfg) })},
	"snakeCase": {create: createTransformer[v1.SnakeCaseTransformation]("snakeCase", func(cfg *v1.SnakeCaseTransformation) Transformer { return NewSnakeCaseTransformer(cfg) })},
	"camelCase": {create: createTransformer[v1.CamelCaseTransformation]("camelCase", func(cfg *v1.CamelCaseTransformation) Transformer { return NewCamelCaseTransformer(cfg) })},
}

// createTransformer returns a factory function that unmarshals raw config into T and calls newFn.
func createTransformer[T any](typeName string, newFn func(*T) Transformer) func(*runtime.RawExtension) (Transformer, error) {
	return func(raw *runtime.RawExtension) (Transformer, error) {
		if raw == nil {
			return nil, fmt.Errorf("%s transformation configuration is required", typeName)
		}
		var cfg T
		if err := json.Unmarshal(raw.Raw, &cfg); err != nil {
			return nil, fmt.Errorf("configuration: %w", err)
		}
		return newFn(&cfg), nil
	}
}

// CreateTransformer creates a transformer based on the transformation spec.
func CreateTransformer(transformation *v1.TransformationSpec) (Transformer, error) {
	entry, ok := transformerRegistry[transformation.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported transformation type: %s", transformation.Type)
	}
	return entry.create(transformation.Config)
}
