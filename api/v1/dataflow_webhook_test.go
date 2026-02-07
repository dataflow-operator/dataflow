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
	"context"
	"testing"
)

func TestDataFlow_ValidateCreate_Valid(t *testing.T) {
	df := &DataFlow{}
	df.Spec = DataFlowSpec{
		Source: SourceSpec{
			Type:  "kafka",
			Kafka: &KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t"},
		},
		Sink: SinkSpec{
			Type:  "kafka",
			Kafka: &KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"},
		},
	}
	warnings, err := df.ValidateCreate(context.Background(), df)
	if err != nil {
		t.Errorf("ValidateCreate: unexpected error: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("ValidateCreate: unexpected warnings: %v", warnings)
	}
}

func TestDataFlow_ValidateCreate_Invalid(t *testing.T) {
	df := &DataFlow{}
	df.Spec = DataFlowSpec{
		Source: SourceSpec{Type: "kafka"}, // Kafka config nil
		Sink: SinkSpec{
			Type:  "kafka",
			Kafka: &KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"},
		},
	}
	_, err := df.ValidateCreate(context.Background(), df)
	if err == nil {
		t.Error("ValidateCreate: expected error for invalid spec")
	}
}

func TestDataFlow_ValidateUpdate_Valid(t *testing.T) {
	df := &DataFlow{}
	df.Spec = DataFlowSpec{
		Source: SourceSpec{
			Type:  "kafka",
			Kafka: &KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t"},
		},
		Sink: SinkSpec{
			Type:  "kafka",
			Kafka: &KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"},
		},
	}
	old := &DataFlow{}
	old.Spec = df.Spec
	warnings, err := df.ValidateUpdate(context.Background(), old, df)
	if err != nil {
		t.Errorf("ValidateUpdate: unexpected error: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("ValidateUpdate: unexpected warnings: %v", warnings)
	}
}

func TestDataFlow_ValidateDelete(t *testing.T) {
	df := &DataFlow{}
	warnings, err := df.ValidateDelete(context.Background(), df)
	if err != nil {
		t.Errorf("ValidateDelete: unexpected error: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("ValidateDelete: unexpected warnings: %v", warnings)
	}
}
