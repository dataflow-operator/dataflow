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
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateReplicas_kafkaAllowed(t *testing.T) {
	r := int32(3)
	spec := &DataFlowSpec{
		Replicas: &r,
		Source: SourceSpec{
			Type:   "kafka",
			Config: mustConfig(KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t"}),
		},
		Sink: SinkSpec{
			Type:   "kafka",
			Config: mustConfig(KafkaSinkSpec{Brokers: []string{"b"}, Topic: "out"}),
		},
	}
	errs := ValidateDataFlowSpec(spec)
	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
}

func TestValidateReplicas_postgresqlRejected(t *testing.T) {
	r := int32(2)
	spec := &DataFlowSpec{
		Replicas: &r,
		Source: SourceSpec{
			Type:   "postgresql",
			Config: mustConfig(PostgreSQLSourceSpec{ConnectionString: "postgres://x", Table: "t"}),
		},
		Sink: SinkSpec{
			Type:   "kafka",
			Config: mustConfig(KafkaSinkSpec{Brokers: []string{"b"}, Topic: "out"}),
		},
	}
	errs := ValidateDataFlowSpec(spec)
	if len(errs) == 0 {
		t.Fatal("expected validation error for postgresql replicas > 1")
	}
	found := false
	for _, e := range errs {
		if e.Field == "spec.replicas" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected error on spec.replicas, got %v", errs)
	}
}

func TestValidateTransformWorkers(t *testing.T) {
	base := func() *DataFlowSpec {
		return &DataFlowSpec{
			Source: SourceSpec{
				Type:   "kafka",
				Config: mustConfig(KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t"}),
			},
			Sink: SinkSpec{
				Type:   "kafka",
				Config: mustConfig(KafkaSinkSpec{Brokers: []string{"b"}, Topic: "out"}),
			},
		}
	}

	t.Run("valid", func(t *testing.T) {
		spec := base()
		w := int32(8)
		spec.TransformWorkers = &w
		if errs := ValidateDataFlowSpec(spec); len(errs) != 0 {
			t.Fatalf("expected no errors, got %v", errs)
		}
	})

	t.Run("too low", func(t *testing.T) {
		spec := base()
		w := int32(0)
		spec.TransformWorkers = &w
		errs := ValidateDataFlowSpec(spec)
		found := false
		for _, e := range errs {
			if e.Field == "spec.transformWorkers" {
				found = true
			}
		}
		if !found {
			t.Fatalf("expected error on spec.transformWorkers, got %v", errs)
		}
	})

	t.Run("too high", func(t *testing.T) {
		spec := base()
		w := int32(65)
		spec.TransformWorkers = &w
		errs := ValidateDataFlowSpec(spec)
		found := false
		for _, e := range errs {
			if e.Field == "spec.transformWorkers" {
				found = true
			}
		}
		if !found {
			t.Fatalf("expected error on spec.transformWorkers, got %v", errs)
		}
	})
}

func TestValidateNessieSource_incrementalQueryForbidden(t *testing.T) {
	inc := true
	n := &NessieSourceSpec{
		BaseURL:               "http://nessie:19120",
		Namespace:             "ns",
		Table:                 "t",
		Query:                 "SELECT 1",
		IncrementalBySnapshot: &inc,
	}
	errs := validateNessieSource(n, field.NewPath("config"))
	if len(errs) == 0 {
		t.Fatal("expected error when query set with incrementalBySnapshot")
	}
}

func TestValidateDataFlowCronSpec_replicasRejected(t *testing.T) {
	r := int32(2)
	spec := &DataFlowCronSpec{
		Schedule: "0 * * * *",
		DataFlowSpec: DataFlowSpec{
			Replicas: &r,
			Source: SourceSpec{
				Type:   "kafka",
				Config: mustConfig(KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t"}),
			},
			Sink: SinkSpec{
				Type:   "kafka",
				Config: mustConfig(KafkaSinkSpec{Brokers: []string{"b"}, Topic: "out"}),
			},
		},
	}
	errs := ValidateDataFlowCronSpec(spec)
	if len(errs) == 0 {
		t.Fatal("expected validation error for DataFlowCron replicas > 1")
	}
}

func TestValidateNessieSource_startSnapshotID(t *testing.T) {
	n := &NessieSourceSpec{
		BaseURL:         "http://nessie:19120",
		Namespace:       "ns",
		Table:           "t",
		StartSnapshotID: "not-a-number",
	}
	errs := validateNessieSource(n, field.NewPath("config"))
	if len(errs) == 0 {
		t.Fatal("expected invalid startSnapshotID error")
	}
}
