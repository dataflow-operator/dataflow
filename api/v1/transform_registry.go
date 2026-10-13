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

package v1

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/dataflow-operator/dataflow/pkg/transformtypes"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func init() {
	registerTransformValidators()
	want := append([]string(nil), transformtypes.All()...)
	sort.Strings(want)
	got := transformtypes.RegisteredTypes()
	if !slices.Equal(want, got) {
		panic(fmt.Sprintf("api/v1: transform validators %v do not match transformtypes.All() %v", got, want))
	}
}

func registerTransformValidators() {
	transformtypes.Register(transformtypes.Definition{Type: transformtypes.Timestamp, ValidateConfig: validateTimestampTransformation})
	transformtypes.Register(transformtypes.Definition{Type: transformtypes.Flatten, ValidateConfig: validateFlattenTransformation})
	transformtypes.Register(transformtypes.Definition{Type: transformtypes.Filter, ValidateConfig: validateFilterTransformation})
	transformtypes.Register(transformtypes.Definition{Type: transformtypes.Mask, ValidateConfig: validateMaskTransformation})
	transformtypes.Register(transformtypes.Definition{Type: transformtypes.Router, ValidateConfig: validateRouterTransformation})
	transformtypes.Register(transformtypes.Definition{Type: transformtypes.Select, ValidateConfig: validateSelectTransformation})
	transformtypes.Register(transformtypes.Definition{Type: transformtypes.Remove, ValidateConfig: validateRemoveTransformation})
	transformtypes.Register(transformtypes.Definition{Type: transformtypes.SnakeCase, ValidateConfig: validateSnakeCaseTransformation})
	transformtypes.Register(transformtypes.Definition{Type: transformtypes.CamelCase, ValidateConfig: validateCamelCaseTransformation})
	transformtypes.Register(transformtypes.Definition{Type: transformtypes.DebeziumUnwrap, ValidateConfig: validateDebeziumUnwrapTransformation})
	transformtypes.Register(transformtypes.Definition{Type: transformtypes.ReplaceField, ValidateConfig: validateReplaceFieldTransformation})
	transformtypes.Register(transformtypes.Definition{Type: transformtypes.HeadersToPayload, ValidateConfig: validateHeadersToPayloadTransformation})
	transformtypes.Register(transformtypes.Definition{Type: transformtypes.StructFlatten, ValidateConfig: validateStructFlattenTransformation})
	transformtypes.Register(transformtypes.Definition{Type: transformtypes.ExtractField, ValidateConfig: validateExtractFieldTransformation})
	transformtypes.Register(transformtypes.Definition{Type: transformtypes.HoistField, ValidateConfig: validateHoistFieldTransformation})
	transformtypes.Register(transformtypes.Definition{Type: transformtypes.Cast, ValidateConfig: validateCastTransformation})
	transformtypes.Register(transformtypes.Definition{Type: transformtypes.Timezone, ValidateConfig: validateTimezoneTransformation})
	transformtypes.Register(transformtypes.Definition{Type: transformtypes.InsertField, ValidateConfig: validateInsertFieldTransformation})
}

func unmarshalTransformConfig[T any](raw []byte, path *field.Path, typeName string) (*T, field.ErrorList) {
	if len(raw) == 0 {
		return nil, field.ErrorList{field.Required(path.Child("config"), typeName+" transformation configuration is required")}
	}
	var cfg T
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, field.ErrorList{field.Invalid(path.Child("config"), string(raw), "invalid "+typeName+" config: "+err.Error())}
	}
	return &cfg, nil
}

func validateTimestampTransformation(raw []byte, path *field.Path) field.ErrorList {
	_, errs := unmarshalTransformConfig[TimestampTransformation](raw, path, "timestamp")
	return errs
}

func validateFlattenTransformation(raw []byte, path *field.Path) field.ErrorList {
	cfg, errs := unmarshalTransformConfig[FlattenTransformation](raw, path, "flatten")
	if cfg == nil {
		return errs
	}
	if cfg.Field == "" {
		errs = append(errs, field.Required(path.Child("config", "field"), "field is required"))
	}
	return errs
}

func validateFilterTransformation(raw []byte, path *field.Path) field.ErrorList {
	cfg, errs := unmarshalTransformConfig[FilterTransformation](raw, path, "filter")
	if cfg == nil {
		return errs
	}
	if cfg.Condition == "" {
		errs = append(errs, field.Required(path.Child("config", "condition"), "condition is required"))
	}
	return errs
}

func validateMaskTransformation(raw []byte, path *field.Path) field.ErrorList {
	cfg, errs := unmarshalTransformConfig[MaskTransformation](raw, path, "mask")
	if cfg == nil {
		return errs
	}
	if len(cfg.Fields) == 0 {
		errs = append(errs, field.Required(path.Child("config", "fields"), "at least one field is required"))
	}
	return errs
}

func validateRouterTransformation(raw []byte, path *field.Path) field.ErrorList {
	if len(raw) == 0 {
		return field.ErrorList{field.Required(path.Child("config"), "router transformation configuration is required (config or router)")}
	}
	var cfg RouterTransformation
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return field.ErrorList{field.Required(path.Child("config"), "router transformation configuration is required (config or router)")}
	}
	var all field.ErrorList
	routesPath := path.Child("config", "routes")
	for j, route := range cfg.Routes {
		if route.Condition == "" {
			all = append(all, field.Required(routesPath.Index(j).Child("condition"), "condition is required"))
		}
		all = append(all, validateSink(&route.Sink, routesPath.Index(j).Child("sink"))...)
	}
	return all
}

func validateSelectTransformation(raw []byte, path *field.Path) field.ErrorList {
	cfg, errs := unmarshalTransformConfig[SelectTransformation](raw, path, "select")
	if cfg == nil {
		return errs
	}
	if len(cfg.Fields) == 0 {
		errs = append(errs, field.Required(path.Child("config", "fields"), "at least one field is required"))
	}
	return errs
}

func validateRemoveTransformation(raw []byte, path *field.Path) field.ErrorList {
	cfg, errs := unmarshalTransformConfig[RemoveTransformation](raw, path, "remove")
	if cfg == nil {
		return errs
	}
	if len(cfg.Fields) == 0 {
		errs = append(errs, field.Required(path.Child("config", "fields"), "at least one field is required"))
	}
	return errs
}

func validateSnakeCaseTransformation(raw []byte, path *field.Path) field.ErrorList {
	_, errs := unmarshalTransformConfig[SnakeCaseTransformation](raw, path, "snakeCase")
	return errs
}

func validateCamelCaseTransformation(raw []byte, path *field.Path) field.ErrorList {
	_, errs := unmarshalTransformConfig[CamelCaseTransformation](raw, path, "camelCase")
	return errs
}

func validateDebeziumUnwrapTransformation(raw []byte, path *field.Path) field.ErrorList {
	cfg, errs := unmarshalTransformConfig[DebeziumUnwrapTransformation](raw, path, "debeziumUnwrap")
	if cfg == nil {
		return errs
	}
	if cfg.SnapshotOperation != "" && cfg.SnapshotOperation != "insert" && cfg.SnapshotOperation != "update" {
		errs = append(errs, field.NotSupported(path.Child("config", "snapshotOperation"), cfg.SnapshotOperation, []string{"insert", "update"}))
	}
	for j, name := range cfg.AddSourceFields {
		if strings.TrimSpace(name) == "" {
			errs = append(errs, field.Required(path.Child("config", "addSourceFields").Index(j), "source field name must not be empty"))
		}
	}
	return errs
}

func validateReplaceFieldTransformation(raw []byte, path *field.Path) field.ErrorList {
	cfg, errs := unmarshalTransformConfig[ReplaceFieldTransformation](raw, path, "replaceField")
	if cfg == nil {
		return errs
	}
	if len(cfg.Include) > 0 && len(cfg.Exclude) > 0 {
		errs = append(errs, field.Invalid(path.Child("config"), string(raw), "include and exclude are mutually exclusive"))
	}
	if len(cfg.Renames) == 0 && len(cfg.Include) == 0 && len(cfg.Exclude) == 0 {
		errs = append(errs, field.Required(path.Child("config"), "at least one of renames, include, or exclude is required"))
	}
	for j, rename := range cfg.Renames {
		if !isValidColonMapping(rename) {
			errs = append(errs, field.Invalid(path.Child("config", "renames").Index(j), rename, "rename must be in oldPath:newPath format"))
		}
	}
	return errs
}

func validateHeadersToPayloadTransformation(raw []byte, path *field.Path) field.ErrorList {
	cfg, errs := unmarshalTransformConfig[HeadersToPayloadTransformation](raw, path, "headersToPayload")
	if cfg == nil {
		return errs
	}
	if len(cfg.Mappings) == 0 {
		errs = append(errs, field.Required(path.Child("config", "mappings"), "at least one mapping is required"))
	} else {
		for j, mapping := range cfg.Mappings {
			if !isValidColonMapping(mapping) {
				errs = append(errs, field.Invalid(path.Child("config", "mappings").Index(j), mapping, "mapping must be in headerName:fieldPath format"))
			}
		}
	}
	return errs
}

func validateStructFlattenTransformation(raw []byte, path *field.Path) field.ErrorList {
	cfg, errs := unmarshalTransformConfig[StructFlattenTransformation](raw, path, "structFlatten")
	if cfg == nil {
		return errs
	}
	if err := validateStructFlattenDelimiter(raw); err != nil {
		errs = append(errs, field.Invalid(path.Child("config", "delimiter"), cfg.Delimiter, err.Error()))
	}
	return errs
}

func validateExtractFieldTransformation(raw []byte, path *field.Path) field.ErrorList {
	cfg, errs := unmarshalTransformConfig[ExtractFieldTransformation](raw, path, "extractField")
	if cfg == nil {
		return errs
	}
	if normalizeJSONPathField(cfg.Field) == "" {
		errs = append(errs, field.Required(path.Child("config", "field"), "field is required"))
	}
	return errs
}

func validateHoistFieldTransformation(raw []byte, path *field.Path) field.ErrorList {
	cfg, errs := unmarshalTransformConfig[HoistFieldTransformation](raw, path, "hoistField")
	if cfg == nil {
		return errs
	}
	fieldName := strings.TrimSpace(cfg.Field)
	if fieldName == "" {
		errs = append(errs, field.Required(path.Child("config", "field"), "field is required"))
	} else if strings.Contains(fieldName, ".") {
		errs = append(errs, field.Invalid(path.Child("config", "field"), cfg.Field, "field must be a simple top-level key without dots"))
	}
	return errs
}

func validateCastTransformation(raw []byte, path *field.Path) field.ErrorList {
	cfg, errs := unmarshalTransformConfig[CastTransformation](raw, path, "cast")
	if cfg == nil {
		return errs
	}
	if len(cfg.Spec) == 0 {
		errs = append(errs, field.Required(path.Child("config", "spec"), "spec is required and must be non-empty"))
		return errs
	}
	for jsonPath, typ := range cfg.Spec {
		if normalizeJSONPathField(jsonPath) == "" {
			errs = append(errs, field.Invalid(path.Child("config", "spec"), jsonPath, "spec keys must be non-empty JSONPaths"))
			continue
		}
		if !isValidCastType(typ) {
			errs = append(errs, field.NotSupported(path.Child("config", "spec").Key(jsonPath), typ, validCastTypes))
		}
	}
	return errs
}

func validateTimezoneTransformation(raw []byte, path *field.Path) field.ErrorList {
	cfg, errs := unmarshalTransformConfig[TimezoneTransformation](raw, path, "timezone")
	if cfg == nil {
		return errs
	}
	if strings.TrimSpace(cfg.Timezone) == "" {
		errs = append(errs, field.Required(path.Child("config", "timezone"), "timezone is required"))
	} else if _, err := LoadTimezoneLocation(cfg.Timezone); err != nil {
		errs = append(errs, field.Invalid(path.Child("config", "timezone"), cfg.Timezone, "must be a valid IANA timezone or ±HH:MM offset"))
	}
	if len(cfg.Fields) == 0 {
		errs = append(errs, field.Required(path.Child("config", "fields"), "fields is required and must be non-empty"))
	} else {
		for i, fieldPath := range cfg.Fields {
			if normalizeJSONPathField(fieldPath) == "" {
				errs = append(errs, field.Invalid(path.Child("config", "fields").Index(i), fieldPath, "field path must be a non-empty JSONPath"))
			}
		}
	}
	if src := strings.TrimSpace(cfg.SourceTimezone); src != "" {
		if _, err := LoadTimezoneLocation(src); err != nil {
			errs = append(errs, field.Invalid(path.Child("config", "sourceTimezone"), cfg.SourceTimezone, "must be a valid IANA timezone or ±HH:MM offset"))
		}
	}
	if format := strings.TrimSpace(cfg.Format); format != "" && !isValidTimezoneFormat(format) {
		errs = append(errs, field.NotSupported(path.Child("config", "format"), cfg.Format, validTimezoneFormats))
	}
	return errs
}

func validateInsertFieldTransformation(raw []byte, path *field.Path) field.ErrorList {
	cfg, errs := unmarshalTransformConfig[InsertFieldTransformation](raw, path, "insertField")
	if cfg == nil {
		return errs
	}
	if len(cfg.Fields) == 0 {
		errs = append(errs, field.Required(path.Child("config", "fields"), "fields is required and must be non-empty"))
		return errs
	}
	for jsonPath, value := range cfg.Fields {
		if normalizeJSONPathField(jsonPath) == "" {
			errs = append(errs, field.Invalid(path.Child("config", "fields"), jsonPath, "fields keys must be non-empty JSONPaths"))
			continue
		}
		if strings.HasPrefix(value, "json:") {
			rawJSON := strings.TrimPrefix(value, "json:")
			if !json.Valid([]byte(rawJSON)) {
				errs = append(errs, field.Invalid(path.Child("config", "fields").Key(jsonPath), value, "json: value must be valid JSON"))
			}
		}
	}
	return errs
}
