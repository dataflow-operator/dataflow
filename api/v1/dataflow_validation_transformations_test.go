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
