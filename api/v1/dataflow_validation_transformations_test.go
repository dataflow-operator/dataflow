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
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

func mustRawConfigForValidation(v interface{}) *runtime.RawExtension {
	b, _ := json.Marshal(v)
	return &runtime.RawExtension{Raw: b}
}

func TestValidateTransformations_DebeziumUnwrap(t *testing.T) {
	baseSpec := DataFlowSpec{
		Source: SourceSpec{
			Type:   "kafka",
			Config: mustRawConfigForValidation(KafkaSourceSpec{Brokers: []string{"broker:9092"}, Topic: "src"}),
		},
		Sink: SinkSpec{
			Type:   "kafka",
			Config: mustRawConfigForValidation(KafkaSinkSpec{Brokers: []string{"broker:9092"}, Topic: "dst"}),
		},
	}

	t.Run("valid config", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type: "debeziumUnwrap",
			Config: mustRawConfigForValidation(DebeziumUnwrapTransformation{
				InferDeleteFromTombstone: true,
				IncludeSourceInMetadata:  true,
				SnapshotOperation:        "update",
			}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) != 0 {
			t.Fatalf("expected no validation errors, got %v", errs)
		}
	})

	t.Run("missing config", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{Type: "debeziumUnwrap"}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for missing config")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "debeziumUnwrap transformation configuration is required") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})

	t.Run("invalid snapshot operation", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type: "debeziumUnwrap",
			Config: mustRawConfigForValidation(DebeziumUnwrapTransformation{
				SnapshotOperation: "noop",
			}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for snapshotOperation")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "Unsupported value") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})
}

func TestValidateTransformations_ReplaceField(t *testing.T) {
	baseSpec := DataFlowSpec{
		Source: SourceSpec{
			Type:   "kafka",
			Config: mustRawConfigForValidation(KafkaSourceSpec{Brokers: []string{"broker:9092"}, Topic: "src"}),
		},
		Sink: SinkSpec{
			Type:   "kafka",
			Config: mustRawConfigForValidation(KafkaSinkSpec{Brokers: []string{"broker:9092"}, Topic: "dst"}),
		},
	}

	t.Run("valid renames", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type: "replaceField",
			Config: mustRawConfigForValidation(ReplaceFieldTransformation{
				Renames: []string{"old:new", "a.b:c"},
			}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) != 0 {
			t.Fatalf("expected no validation errors, got %v", errs)
		}
	})

	t.Run("valid include", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type: "replaceField",
			Config: mustRawConfigForValidation(ReplaceFieldTransformation{
				Include: []string{"id", "user.name"},
			}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) != 0 {
			t.Fatalf("expected no validation errors, got %v", errs)
		}
	})

	t.Run("missing config", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{Type: "replaceField"}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for missing config")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "replaceField transformation configuration is required") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})

	t.Run("empty config", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type:   "replaceField",
			Config: mustRawConfigForValidation(ReplaceFieldTransformation{}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for empty replaceField config")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "at least one of renames, include, or exclude is required") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})

	t.Run("include and exclude mutually exclusive", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type: "replaceField",
			Config: mustRawConfigForValidation(ReplaceFieldTransformation{
				Include: []string{"a"},
				Exclude: []string{"b"},
			}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for include+exclude")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "mutually exclusive") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})

	t.Run("invalid rename format", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type: "replaceField",
			Config: mustRawConfigForValidation(ReplaceFieldTransformation{
				Renames: []string{"bad-format"},
			}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for invalid rename")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "oldPath:newPath") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})
}

func TestValidateTransformations_HeadersToPayload(t *testing.T) {
	baseSpec := DataFlowSpec{
		Source: SourceSpec{
			Type:   "kafka",
			Config: mustRawConfigForValidation(KafkaSourceSpec{Brokers: []string{"broker:9092"}, Topic: "src"}),
		},
		Sink: SinkSpec{
			Type:   "kafka",
			Config: mustRawConfigForValidation(KafkaSinkSpec{Brokers: []string{"broker:9092"}, Topic: "dst"}),
		},
	}

	t.Run("valid mappings", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type: "headersToPayload",
			Config: mustRawConfigForValidation(HeadersToPayloadTransformation{
				Mappings: []string{"X-Request-Id:requestId", "X-Language:metadata.language"},
			}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) != 0 {
			t.Fatalf("expected no validation errors, got %v", errs)
		}
	})

	t.Run("missing config", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{Type: "headersToPayload"}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for missing config")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "headersToPayload transformation configuration is required") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})

	t.Run("empty mappings", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type:   "headersToPayload",
			Config: mustRawConfigForValidation(HeadersToPayloadTransformation{}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for empty mappings")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "at least one mapping is required") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})

	t.Run("invalid mapping format", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type: "headersToPayload",
			Config: mustRawConfigForValidation(HeadersToPayloadTransformation{
				Mappings: []string{"bad-format"},
			}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for invalid mapping")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "headerName:fieldPath") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})
}

func TestValidateTransformations_StructFlatten(t *testing.T) {
	baseSpec := DataFlowSpec{
		Source: SourceSpec{
			Type:   "kafka",
			Config: mustRawConfigForValidation(KafkaSourceSpec{Brokers: []string{"broker:9092"}, Topic: "src"}),
		},
		Sink: SinkSpec{
			Type:   "kafka",
			Config: mustRawConfigForValidation(KafkaSinkSpec{Brokers: []string{"broker:9092"}, Topic: "dst"}),
		},
	}

	t.Run("valid empty config", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type:   "structFlatten",
			Config: mustRawConfigForValidation(StructFlattenTransformation{}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) != 0 {
			t.Fatalf("expected no validation errors, got %v", errs)
		}
	})

	t.Run("valid custom delimiter", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type: "structFlatten",
			Config: mustRawConfigForValidation(StructFlattenTransformation{
				Delimiter: "_",
			}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) != 0 {
			t.Fatalf("expected no validation errors, got %v", errs)
		}
	})

	t.Run("missing config", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{Type: "structFlatten"}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for missing config")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "structFlatten transformation configuration is required") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})

	t.Run("empty delimiter", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type:   "structFlatten",
			Config: &runtime.RawExtension{Raw: []byte(`{"delimiter":""}`)},
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for empty delimiter")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "delimiter must be a non-empty string") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})
}

func TestValidateTransformations_ExtractField(t *testing.T) {
	baseSpec := DataFlowSpec{
		Source: SourceSpec{
			Type:   "kafka",
			Config: mustRawConfigForValidation(KafkaSourceSpec{Brokers: []string{"broker:9092"}, Topic: "src"}),
		},
		Sink: SinkSpec{
			Type:   "kafka",
			Config: mustRawConfigForValidation(KafkaSinkSpec{Brokers: []string{"broker:9092"}, Topic: "dst"}),
		},
	}

	t.Run("valid", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type: "extractField",
			Config: mustRawConfigForValidation(ExtractFieldTransformation{
				Field: "payload.after",
			}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) != 0 {
			t.Fatalf("expected no validation errors, got %v", errs)
		}
	})

	t.Run("valid with JSONPath prefix", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type: "extractField",
			Config: mustRawConfigForValidation(ExtractFieldTransformation{
				Field: "$.payload.after",
			}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) != 0 {
			t.Fatalf("expected no validation errors, got %v", errs)
		}
	})

	t.Run("missing config", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{Type: "extractField"}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for missing config")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "extractField transformation configuration is required") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})

	t.Run("empty field", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type:   "extractField",
			Config: mustRawConfigForValidation(ExtractFieldTransformation{}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for empty field")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "field is required") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})

	t.Run("field only dollar prefix", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type: "extractField",
			Config: mustRawConfigForValidation(ExtractFieldTransformation{
				Field: "$",
			}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for empty path after normalize")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "field is required") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})
}

func TestValidateTransformations_HoistField(t *testing.T) {
	baseSpec := DataFlowSpec{
		Source: SourceSpec{
			Type:   "kafka",
			Config: mustRawConfigForValidation(KafkaSourceSpec{Brokers: []string{"broker:9092"}, Topic: "src"}),
		},
		Sink: SinkSpec{
			Type:   "kafka",
			Config: mustRawConfigForValidation(KafkaSinkSpec{Brokers: []string{"broker:9092"}, Topic: "dst"}),
		},
	}

	t.Run("valid", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type: "hoistField",
			Config: mustRawConfigForValidation(HoistFieldTransformation{
				Field: "record",
			}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) != 0 {
			t.Fatalf("expected no validation errors, got %v", errs)
		}
	})

	t.Run("missing config", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{Type: "hoistField"}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for missing config")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "hoistField transformation configuration is required") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})

	t.Run("empty field", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type:   "hoistField",
			Config: mustRawConfigForValidation(HoistFieldTransformation{}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for empty field")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "field is required") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})

	t.Run("field with dots", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type: "hoistField",
			Config: mustRawConfigForValidation(HoistFieldTransformation{
				Field: "payload.after",
			}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for dotted field")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "simple top-level key without dots") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})
}

func TestValidateTransformations_Cast(t *testing.T) {
	baseSpec := DataFlowSpec{
		Source: SourceSpec{
			Type:   "kafka",
			Config: mustRawConfigForValidation(KafkaSourceSpec{Brokers: []string{"broker:9092"}, Topic: "src"}),
		},
		Sink: SinkSpec{
			Type:   "kafka",
			Config: mustRawConfigForValidation(KafkaSinkSpec{Brokers: []string{"broker:9092"}, Topic: "dst"}),
		},
	}

	t.Run("valid", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type: "cast",
			Config: mustRawConfigForValidation(CastTransformation{
				Spec: map[string]string{
					"id":     "int64",
					"amount": "float64",
					"active": "bool",
					"note":   "string",
					"gone":   "null",
				},
			}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) != 0 {
			t.Fatalf("expected no validation errors, got %v", errs)
		}
	})

	t.Run("valid with JSONPath prefix", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type: "cast",
			Config: mustRawConfigForValidation(CastTransformation{
				Spec: map[string]string{
					"$.row.id": "int64",
				},
			}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) != 0 {
			t.Fatalf("expected no validation errors, got %v", errs)
		}
	})

	t.Run("missing config", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{Type: "cast"}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for missing config")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "cast transformation configuration is required") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})

	t.Run("empty spec", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type:   "cast",
			Config: mustRawConfigForValidation(CastTransformation{}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for empty spec")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "spec is required") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})

	t.Run("unsupported type", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type: "cast",
			Config: mustRawConfigForValidation(CastTransformation{
				Spec: map[string]string{"id": "uint64"},
			}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for unsupported cast type")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "uint64") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})

	t.Run("empty path key", func(t *testing.T) {
		spec := baseSpec
		spec.Transformations = []TransformationSpec{{
			Type: "cast",
			Config: mustRawConfigForValidation(CastTransformation{
				Spec: map[string]string{"$": "int64"},
			}),
		}}
		errs := ValidateDataFlowSpec(&spec)
		if len(errs) == 0 {
			t.Fatal("expected validation error for empty path")
		}
		if !strings.Contains(errs.ToAggregate().Error(), "non-empty JSONPaths") {
			t.Fatalf("unexpected error: %v", errs.ToAggregate())
		}
	})
}
