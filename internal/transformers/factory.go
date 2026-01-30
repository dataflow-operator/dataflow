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
	"fmt"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
)

// CreateTransformer creates a transformer based on the transformation spec
func CreateTransformer(transformation *v1.TransformationSpec) (Transformer, error) {
	switch transformation.Type {
	case "timestamp":
		if transformation.Timestamp == nil {
			return nil, fmt.Errorf("timestamp transformation configuration is required")
		}
		return NewTimestampTransformer(transformation.Timestamp), nil
	case "flatten":
		if transformation.Flatten == nil {
			return nil, fmt.Errorf("flatten transformation configuration is required")
		}
		return NewFlattenTransformer(transformation.Flatten), nil
	case "filter":
		if transformation.Filter == nil {
			return nil, fmt.Errorf("filter transformation configuration is required")
		}
		return NewFilterTransformer(transformation.Filter), nil
	case "mask":
		if transformation.Mask == nil {
			return nil, fmt.Errorf("mask transformation configuration is required")
		}
		return NewMaskTransformer(transformation.Mask), nil
	case "router":
		if transformation.Router == nil {
			return nil, fmt.Errorf("router transformation configuration is required")
		}
		return NewRouterTransformer(transformation.Router), nil
	case "select":
		if transformation.Select == nil {
			return nil, fmt.Errorf("select transformation configuration is required")
		}
		return NewSelectTransformer(transformation.Select), nil
	case "remove":
		if transformation.Remove == nil {
			return nil, fmt.Errorf("remove transformation configuration is required")
		}
		return NewRemoveTransformer(transformation.Remove), nil
	case "snakeCase":
		if transformation.SnakeCase == nil {
			return nil, fmt.Errorf("snakeCase transformation configuration is required")
		}
		return NewSnakeCaseTransformer(transformation.SnakeCase), nil
	case "camelCase":
		if transformation.CamelCase == nil {
			return nil, fmt.Errorf("camelCase transformation configuration is required")
		}
		return NewCamelCaseTransformer(transformation.CamelCase), nil
	// TODO: replaceField and headerFrom transformations are not yet implemented in API
	// case "replaceField":
	// 	if transformation.ReplaceField == nil {
	// 		return nil, fmt.Errorf("replaceField transformation configuration is required")
	// 	}
	// 	return NewReplaceFieldTransformer(transformation.ReplaceField), nil
	// case "headerFrom":
	// 	if transformation.HeaderFrom == nil {
	// 		return nil, fmt.Errorf("headerFrom transformation configuration is required")
	// 	}
	// 	return NewHeaderFromTransformer(transformation.HeaderFrom), nil
	default:
		return nil, fmt.Errorf("unsupported transformation type: %s", transformation.Type)
	}
}
