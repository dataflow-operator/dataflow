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

package validation

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
)

func kafkaSource(cfg dataflowv1.KafkaSourceSpec) dataflowv1.SourceSpec {
	raw, _ := json.Marshal(cfg)
	return dataflowv1.SourceSpec{Type: "kafka", Config: &runtime.RawExtension{Raw: raw}}
}
func kafkaSink(cfg dataflowv1.KafkaSinkSpec) dataflowv1.SinkSpec {
	raw, _ := json.Marshal(cfg)
	return dataflowv1.SinkSpec{Type: "kafka", Config: &runtime.RawExtension{Raw: raw}}
}
func postgresqlSource(cfg dataflowv1.PostgreSQLSourceSpec) dataflowv1.SourceSpec {
	raw, _ := json.Marshal(cfg)
	return dataflowv1.SourceSpec{Type: "postgresql", Config: &runtime.RawExtension{Raw: raw}}
}
func trinoSource(cfg dataflowv1.TrinoSourceSpec) dataflowv1.SourceSpec {
	raw, _ := json.Marshal(cfg)
	return dataflowv1.SourceSpec{Type: "trino", Config: &runtime.RawExtension{Raw: raw}}
}
func timestampTransform(cfg dataflowv1.TimestampTransformation) dataflowv1.TransformationSpec {
	raw, _ := json.Marshal(cfg)
	return dataflowv1.TransformationSpec{Type: "timestamp", Config: &runtime.RawExtension{Raw: raw}}
}
func flattenTransform(cfg dataflowv1.FlattenTransformation) dataflowv1.TransformationSpec {
	raw, _ := json.Marshal(cfg)
	return dataflowv1.TransformationSpec{Type: "flatten", Config: &runtime.RawExtension{Raw: raw}}
}
func snakeCaseTransform(cfg dataflowv1.SnakeCaseTransformation) dataflowv1.TransformationSpec {
	raw, _ := json.Marshal(cfg)
	return dataflowv1.TransformationSpec{Type: "snakeCase", Config: &runtime.RawExtension{Raw: raw}}
}
func routerTransform(cfg dataflowv1.RouterTransformation) dataflowv1.TransformationSpec {
	raw, _ := json.Marshal(cfg)
	return dataflowv1.TransformationSpec{Type: "router", Config: &runtime.RawExtension{Raw: raw}}
}

func TestValidateDataFlowSpec_NilSpec(t *testing.T) {
	errs := ValidateDataFlowSpec(nil)
	if len(errs) != 0 {
		t.Errorf("expected no errors for nil spec, got %d", len(errs))
	}
}

func TestValidateDataFlowSpec_ValidKafkaToKafka(t *testing.T) {
	sourceCfg, _ := json.Marshal(dataflowv1.KafkaSourceSpec{
		Brokers: []string{"localhost:9092"},
		Topic:   "input",
	})
	sinkCfg, _ := json.Marshal(dataflowv1.KafkaSinkSpec{
		Brokers: []string{"localhost:9092"},
		Topic:   "output",
	})
	spec := &dataflowv1.DataFlowSpec{
		Source: dataflowv1.SourceSpec{
			Type:   "kafka",
			Config: &runtime.RawExtension{Raw: sourceCfg},
		},
		Sink: dataflowv1.SinkSpec{
			Type:   "kafka",
			Config: &runtime.RawExtension{Raw: sinkCfg},
		},
	}
	errs := ValidateDataFlowSpec(spec)
	if len(errs) > 0 {
		t.Errorf("expected no errors for valid spec, got: %v", errs.ToAggregate())
	}
}

func TestValidateDataFlowSpec_RawConfigFormat(t *testing.T) {
	kafkaConfig := dataflowv1.KafkaSourceSpec{
		Brokers: []string{"localhost:9092"},
		Topic:   "input",
	}
	configRaw, _ := json.Marshal(kafkaConfig)

	spec := &dataflowv1.DataFlowSpec{
		Source: dataflowv1.SourceSpec{
			Type:   "kafka",
			Config: &runtime.RawExtension{Raw: configRaw},
		},
		Sink: dataflowv1.SinkSpec{
			Type: "kafka",
			Config: &runtime.RawExtension{
				Raw: []byte(`{"brokers":["localhost:9092"],"topic":"output"}`),
			},
		},
	}
	errs := ValidateDataFlowSpec(spec)
	if len(errs) > 0 {
		t.Errorf("expected no errors for valid raw config spec, got: %v", errs.ToAggregate())
	}
}

func TestValidateDataFlowSpec_EmptySource(t *testing.T) {
	spec := &dataflowv1.DataFlowSpec{
		Source: dataflowv1.SourceSpec{},
		Sink:   kafkaSink(dataflowv1.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"}),
	}
	errs := ValidateDataFlowSpec(spec)
	if len(errs) == 0 {
		t.Error("expected errors for empty source type")
	}
	found := false
	for _, e := range errs {
		if e.Field == "spec.source.type" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error on spec.source.type, got: %v", errs)
	}
}

func TestValidateDataFlowSpec_InvalidSourceType(t *testing.T) {
	spec := &dataflowv1.DataFlowSpec{
		Source: dataflowv1.SourceSpec{Type: "invalid"},
		Sink:   kafkaSink(dataflowv1.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"}),
	}
	errs := ValidateDataFlowSpec(spec)
	if len(errs) == 0 {
		t.Error("expected errors for invalid source type")
	}
}

func TestValidateDataFlowSpec_SourceTypeWithoutConfig(t *testing.T) {
	spec := &dataflowv1.DataFlowSpec{
		Source: dataflowv1.SourceSpec{Type: "kafka"}, // Config nil
		Sink:   kafkaSink(dataflowv1.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"}),
	}
	errs := ValidateDataFlowSpec(spec)
	if len(errs) == 0 {
		t.Error("expected errors when source type is kafka but kafka config is nil")
	}
}

func TestValidateDataFlowSpec_KafkaSourceMissingBrokersAndTopic(t *testing.T) {
	spec := &dataflowv1.DataFlowSpec{
		Source: kafkaSource(dataflowv1.KafkaSourceSpec{}), // no brokers, no topic
		Sink:   kafkaSink(dataflowv1.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"}),
	}
	errs := ValidateDataFlowSpec(spec)
	if len(errs) < 2 {
		t.Errorf("expected at least 2 errors (brokers, topic), got %d: %v", len(errs), errs.ToAggregate())
	}
}

func TestValidateDataFlowSpec_InvalidTransformationType(t *testing.T) {
	spec := &dataflowv1.DataFlowSpec{
		Source: kafkaSource(dataflowv1.KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t"}),
		Sink:   kafkaSink(dataflowv1.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"}),
		Transformations: []dataflowv1.TransformationSpec{
			{Type: "unknown"},
		},
	}
	errs := ValidateDataFlowSpec(spec)
	if len(errs) == 0 {
		t.Error("expected error for unsupported transformation type")
	}
}

func TestValidateDataFlowSpec_TransformationTypeWithoutConfig(t *testing.T) {
	spec := &dataflowv1.DataFlowSpec{
		Source: kafkaSource(dataflowv1.KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t"}),
		Sink:   kafkaSink(dataflowv1.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"}),
		Transformations: []dataflowv1.TransformationSpec{
			{Type: "flatten"}, // Config is nil
		},
	}
	errs := ValidateDataFlowSpec(spec)
	if len(errs) == 0 {
		t.Error("expected error when transformation type is flatten but flatten config is nil")
	}
}

func TestValidateDataFlowSpec_ErrorsSinkInvalid(t *testing.T) {
	spec := &dataflowv1.DataFlowSpec{
		Source: kafkaSource(dataflowv1.KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t"}),
		Sink:   kafkaSink(dataflowv1.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"}),
		Errors: &dataflowv1.ErrorSinkSpec{SinkSpec: dataflowv1.SinkSpec{Type: "kafka"}}, // Config nil
	}
	errs := ValidateDataFlowSpec(spec)
	if len(errs) == 0 {
		t.Error("expected error when errors sink type is kafka but kafka config is nil")
	}
}

func TestValidateDataFlowSpec_SecretRefMissingNameOrKey(t *testing.T) {
	spec := &dataflowv1.DataFlowSpec{
		Source: kafkaSource(dataflowv1.KafkaSourceSpec{
			Brokers:        []string{"b"},
			Topic:          "t",
			TopicSecretRef: &dataflowv1.SecretRef{Name: "sec"}, // Key missing
		}),
		Sink: kafkaSink(dataflowv1.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"}),
	}
	errs := ValidateDataFlowSpec(spec)
	if len(errs) == 0 {
		t.Error("expected error when SecretRef has no key")
	}
}

func TestValidateDataFlowSpec_ResourcesNegative(t *testing.T) {
	negQty := resource.MustParse("-100m")
	spec := &dataflowv1.DataFlowSpec{
		Source: kafkaSource(dataflowv1.KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t"}),
		Sink:   kafkaSink(dataflowv1.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"}),
		Resources: &corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: negQty},
		},
	}
	errs := ValidateDataFlowSpec(spec)
	if len(errs) == 0 {
		t.Error("expected error for negative resource quantity")
	}
}

func TestValidateDataFlowSpec_ValidWithTransformations(t *testing.T) {
	spec := &dataflowv1.DataFlowSpec{
		Source: kafkaSource(dataflowv1.KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t"}),
		Sink:   kafkaSink(dataflowv1.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"}),
		Transformations: []dataflowv1.TransformationSpec{
			timestampTransform(dataflowv1.TimestampTransformation{}),
			flattenTransform(dataflowv1.FlattenTransformation{Field: "items"}),
			snakeCaseTransform(dataflowv1.SnakeCaseTransformation{}),
		},
	}
	errs := ValidateDataFlowSpec(spec)
	if len(errs) > 0 {
		t.Errorf("expected no errors, got: %v", errs.ToAggregate())
	}
}

func TestValidateDataFlowSpec_RouterWithInvalidNestedSink(t *testing.T) {
	spec := &dataflowv1.DataFlowSpec{
		Source: kafkaSource(dataflowv1.KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t"}),
		Sink:   kafkaSink(dataflowv1.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"}),
		Transformations: []dataflowv1.TransformationSpec{
			routerTransform(dataflowv1.RouterTransformation{
				Routes: []dataflowv1.RouteRule{
					{Condition: "true", Sink: dataflowv1.SinkSpec{Type: "kafka"}}, // Config nil
				},
			}),
		},
	}
	errs := ValidateDataFlowSpec(spec)
	if len(errs) == 0 {
		t.Error("expected error for router route with invalid nested sink")
	}
}

// Ensure we don't panic on empty path segments (e.g. flatten with empty field)
func TestValidateDataFlowSpec_FlattenEmptyField(t *testing.T) {
	spec := &dataflowv1.DataFlowSpec{
		Source: kafkaSource(dataflowv1.KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t"}),
		Sink:   kafkaSink(dataflowv1.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"}),
		Transformations: []dataflowv1.TransformationSpec{
			flattenTransform(dataflowv1.FlattenTransformation{Field: ""}),
		},
	}
	errs := ValidateDataFlowSpec(spec)
	if len(errs) == 0 {
		t.Error("expected error for flatten with empty field")
	}
}

func TestValidateDataFlowSpec_PostgreSQLSourceValid(t *testing.T) {
	spec := &dataflowv1.DataFlowSpec{
		Source: postgresqlSource(dataflowv1.PostgreSQLSourceSpec{
			ConnectionString: "host=localhost",
			Table:            "t",
		}),
		Sink: kafkaSink(dataflowv1.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"}),
	}
	errs := ValidateDataFlowSpec(spec)
	if len(errs) > 0 {
		t.Errorf("expected no errors: %v", errs.ToAggregate())
	}
}

func TestValidateDataFlowSpec_TrinoSourceValid(t *testing.T) {
	spec := &dataflowv1.DataFlowSpec{
		Source: trinoSource(dataflowv1.TrinoSourceSpec{
			ServerURL: "http://trino:8080",
			Catalog:   "c",
			Schema:    "s",
			Table:     "t",
		}),
		Sink: kafkaSink(dataflowv1.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"}),
	}
	errs := ValidateDataFlowSpec(spec)
	if len(errs) > 0 {
		t.Errorf("expected no errors: %v", errs.ToAggregate())
	}
}
