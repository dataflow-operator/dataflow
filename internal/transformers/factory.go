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

// CreateTransformer creates a transformer based on the transformation spec.
// Supports both config (type+config) and legacy (type+timestamp/flatten/etc) formats.
func CreateTransformer(transformation *v1.TransformationSpec) (Transformer, error) {
	switch transformation.Type {
	case "timestamp":
		cfg, err := transformation.GetTimestampConfig()
		if err != nil {
			return nil, fmt.Errorf("timestamp transformation configuration: %w", err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("timestamp transformation configuration is required")
		}
		return NewTimestampTransformer(cfg), nil
	case "flatten":
		cfg, err := transformation.GetFlattenConfig()
		if err != nil {
			return nil, fmt.Errorf("flatten transformation configuration: %w", err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("flatten transformation configuration is required")
		}
		return NewFlattenTransformer(cfg), nil
	case "filter":
		cfg, err := transformation.GetFilterConfig()
		if err != nil {
			return nil, fmt.Errorf("filter transformation configuration: %w", err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("filter transformation configuration is required")
		}
		return NewFilterTransformer(cfg), nil
	case "mask":
		cfg, err := transformation.GetMaskConfig()
		if err != nil {
			return nil, fmt.Errorf("mask transformation configuration: %w", err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("mask transformation configuration is required")
		}
		return NewMaskTransformer(cfg), nil
	case "router":
		cfg, err := transformation.GetRouterConfig()
		if err != nil {
			return nil, fmt.Errorf("router transformation configuration: %w", err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("router transformation configuration is required")
		}
		return NewRouterTransformer(cfg), nil
	case "select":
		cfg, err := transformation.GetSelectConfig()
		if err != nil {
			return nil, fmt.Errorf("select transformation configuration: %w", err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("select transformation configuration is required")
		}
		return NewSelectTransformer(cfg), nil
	case "remove":
		cfg, err := transformation.GetRemoveConfig()
		if err != nil {
			return nil, fmt.Errorf("remove transformation configuration: %w", err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("remove transformation configuration is required")
		}
		return NewRemoveTransformer(cfg), nil
	case "snakeCase":
		cfg, err := transformation.GetSnakeCaseConfig()
		if err != nil {
			return nil, fmt.Errorf("snakeCase transformation configuration: %w", err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("snakeCase transformation configuration is required")
		}
		return NewSnakeCaseTransformer(cfg), nil
	case "camelCase":
		cfg, err := transformation.GetCamelCaseConfig()
		if err != nil {
			return nil, fmt.Errorf("camelCase transformation configuration: %w", err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("camelCase transformation configuration is required")
		}
		return NewCamelCaseTransformer(cfg), nil
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
