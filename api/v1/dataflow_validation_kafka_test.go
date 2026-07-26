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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateKafkaConsumerTiming(t *testing.T) {
	path := field.NewPath("spec").Child("source").Child("config")

	t.Run("valid", func(t *testing.T) {
		minBytes := int32(0)
		maxBytes := int32(1024)
		spec := &KafkaSourceSpec{
			ConsumerMaxWait: &metav1.Duration{Duration: 30 * time.Second},
			NetReadTimeout:  &metav1.Duration{Duration: 60 * time.Second},
			FetchMinBytes:   &minBytes,
			FetchMaxBytes:   &maxBytes,
		}
		if errs := validateKafkaConsumerTiming(spec, path); len(errs) != 0 {
			t.Fatalf("expected no errors, got %v", errs)
		}
	})

	t.Run("netReadTimeout must exceed consumerMaxWait", func(t *testing.T) {
		spec := &KafkaSourceSpec{
			ConsumerMaxWait: &metav1.Duration{Duration: 30 * time.Second},
			NetReadTimeout:  &metav1.Duration{Duration: 10 * time.Second},
		}
		errs := validateKafkaConsumerTiming(spec, path)
		if len(errs) == 0 {
			t.Fatal("expected validation error")
		}
	})
}

func TestValidateKafkaProducerTuning(t *testing.T) {
	path := field.NewPath("spec").Child("sink").Child("config")

	t.Run("defaults valid", func(t *testing.T) {
		spec := &KafkaSinkSpec{}
		if errs := validateKafkaProducerTuning(spec, path); len(errs) != 0 {
			t.Fatalf("expected no errors, got %v", errs)
		}
	})

	t.Run("async high throughput profile", func(t *testing.T) {
		idempotent := true
		async := true
		flushMsgs := int32(100)
		spec := &KafkaSinkSpec{
			RequiredAcks:   "all",
			Compression:    "snappy",
			Idempotent:     &idempotent,
			Async:          &async,
			FlushMessages:  &flushMsgs,
			FlushFrequency: &metav1.Duration{Duration: 50 * time.Millisecond},
		}
		if errs := validateKafkaProducerTuning(spec, path); len(errs) != 0 {
			t.Fatalf("expected no errors, got %v", errs)
		}
	})

	t.Run("idempotent rejects local acks", func(t *testing.T) {
		spec := &KafkaSinkSpec{RequiredAcks: "local"}
		errs := validateKafkaProducerTuning(spec, path)
		if len(errs) == 0 {
			t.Fatal("expected validation error for requiredAcks=local with idempotent default")
		}
	})

	t.Run("non-idempotent allows local acks and higher maxOpenRequests", func(t *testing.T) {
		idempotent := false
		maxOpen := int32(5)
		spec := &KafkaSinkSpec{
			Idempotent:      &idempotent,
			RequiredAcks:    "local",
			MaxOpenRequests: &maxOpen,
			Compression:     "lz4",
		}
		if errs := validateKafkaProducerTuning(spec, path); len(errs) != 0 {
			t.Fatalf("expected no errors, got %v", errs)
		}
	})

	t.Run("rejects unknown compression", func(t *testing.T) {
		spec := &KafkaSinkSpec{Compression: "brotli"}
		errs := validateKafkaProducerTuning(spec, path)
		if len(errs) == 0 {
			t.Fatal("expected validation error")
		}
	})
}
