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
	"github.com/dataflow-operator/dataflow/pkg/transformtypes"
	"k8s.io/apimachinery/pkg/runtime"
)

// transformerEntry defines how to create a transformer from raw config.
type transformerEntry struct {
	create func(raw *runtime.RawExtension) (Transformer, error)
}

var transformerRegistry = map[string]transformerEntry{
	transformtypes.Timestamp: {create: createTransformer[v1.TimestampTransformation](transformtypes.Timestamp, func(cfg *v1.TimestampTransformation) Transformer { return NewTimestampTransformer(cfg) })},
	transformtypes.Flatten:   {create: createTransformer[v1.FlattenTransformation](transformtypes.Flatten, func(cfg *v1.FlattenTransformation) Transformer { return NewFlattenTransformer(cfg) })},
	transformtypes.Filter:    {create: createTransformer[v1.FilterTransformation](transformtypes.Filter, func(cfg *v1.FilterTransformation) Transformer { return NewFilterTransformer(cfg) })},
	transformtypes.Mask:      {create: createTransformer[v1.MaskTransformation](transformtypes.Mask, func(cfg *v1.MaskTransformation) Transformer { return NewMaskTransformer(cfg) })},
	transformtypes.Router:    {create: createTransformer[v1.RouterTransformation](transformtypes.Router, func(cfg *v1.RouterTransformation) Transformer { return NewRouterTransformer(cfg) })},
	transformtypes.Select:    {create: createTransformer[v1.SelectTransformation](transformtypes.Select, func(cfg *v1.SelectTransformation) Transformer { return NewSelectTransformer(cfg) })},
	transformtypes.Remove:    {create: createTransformer[v1.RemoveTransformation](transformtypes.Remove, func(cfg *v1.RemoveTransformation) Transformer { return NewRemoveTransformer(cfg) })},
	transformtypes.SnakeCase: {create: createTransformer[v1.SnakeCaseTransformation](transformtypes.SnakeCase, func(cfg *v1.SnakeCaseTransformation) Transformer { return NewSnakeCaseTransformer(cfg) })},
	transformtypes.CamelCase: {create: createTransformer[v1.CamelCaseTransformation](transformtypes.CamelCase, func(cfg *v1.CamelCaseTransformation) Transformer { return NewCamelCaseTransformer(cfg) })},
	transformtypes.DebeziumUnwrap: {
		create: createTransformer[v1.DebeziumUnwrapTransformation](transformtypes.DebeziumUnwrap, func(cfg *v1.DebeziumUnwrapTransformation) Transformer {
			return NewDebeziumUnwrapTransformer(cfg)
		}),
	},
	transformtypes.ReplaceField: {create: createTransformer[v1.ReplaceFieldTransformation](transformtypes.ReplaceField, func(cfg *v1.ReplaceFieldTransformation) Transformer { return NewReplaceFieldTransformer(cfg) })},
	transformtypes.HeadersToPayload: {create: createTransformer[v1.HeadersToPayloadTransformation](transformtypes.HeadersToPayload, func(cfg *v1.HeadersToPayloadTransformation) Transformer {
		return NewHeadersToPayloadTransformer(cfg)
	})},
	transformtypes.StructFlatten: {create: createTransformer[v1.StructFlattenTransformation](transformtypes.StructFlatten, func(cfg *v1.StructFlattenTransformation) Transformer {
		return NewStructFlattenTransformer(cfg)
	})},
	transformtypes.ExtractField: {create: createTransformer[v1.ExtractFieldTransformation](transformtypes.ExtractField, func(cfg *v1.ExtractFieldTransformation) Transformer {
		return NewExtractFieldTransformer(cfg)
	})},
	transformtypes.HoistField: {create: createTransformer[v1.HoistFieldTransformation](transformtypes.HoistField, func(cfg *v1.HoistFieldTransformation) Transformer {
		return NewHoistFieldTransformer(cfg)
	})},
	transformtypes.Cast: {create: createTransformer[v1.CastTransformation](transformtypes.Cast, func(cfg *v1.CastTransformation) Transformer {
		return NewCastTransformer(cfg)
	})},
	transformtypes.Timezone: {create: createTransformer[v1.TimezoneTransformation](transformtypes.Timezone, func(cfg *v1.TimezoneTransformation) Transformer {
		return NewTimezoneTransformer(cfg)
	})},
	transformtypes.InsertField: {create: createTransformer[v1.InsertFieldTransformation](transformtypes.InsertField, func(cfg *v1.InsertFieldTransformation) Transformer {
		return NewInsertFieldTransformer(cfg)
	})},
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
