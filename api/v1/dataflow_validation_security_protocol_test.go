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

func TestValidateKafkaSecurityProtocol(t *testing.T) {
	path := field.NewPath("spec").Child("source").Child("config").Child("securityProtocol")
	sasl := &SASLConfig{
		Mechanism: "scram-sha-256",
		Username:  "user",
		Password:  "pass",
	}
	tls := &TLSConfig{InsecureSkipVerify: true}

	tests := []struct {
		name     string
		protocol string
		tls      *TLSConfig
		sasl     *SASLConfig
		wantErr  bool
	}{
		{"empty protocol", "", nil, sasl, false},
		{"SASL_PLAINTEXT valid", "SASL_PLAINTEXT", nil, sasl, false},
		{"sasl-plaintext normalized", "sasl-plaintext", nil, sasl, false},
		{"SASL_SSL valid", "SASL_SSL", tls, sasl, false},
		{"SSL valid", "SSL", tls, nil, false},
		{"PLAINTEXT valid", "PLAINTEXT", nil, nil, false},
		{"unknown protocol", "WSS", nil, nil, true},
		{"SASL_PLAINTEXT without sasl", "SASL_PLAINTEXT", nil, nil, true},
		{"SASL_PLAINTEXT with tls", "SASL_PLAINTEXT", tls, sasl, true},
		{"SASL_SSL without tls", "SASL_SSL", nil, sasl, true},
		{"SASL_SSL without sasl", "SASL_SSL", tls, nil, true},
		{"SSL without tls", "SSL", nil, nil, true},
		{"SSL with sasl", "SSL", tls, sasl, true},
		{"PLAINTEXT with sasl", "PLAINTEXT", nil, sasl, true},
		{"PLAINTEXT with tls", "PLAINTEXT", tls, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validateKafkaSecurityProtocol(tt.protocol, tt.tls, tt.sasl, path)
			if tt.wantErr && len(errs) == 0 {
				t.Fatal("expected validation error")
			}
			if !tt.wantErr && len(errs) != 0 {
				t.Fatalf("expected no errors, got %v", errs)
			}
		})
	}
}

func TestValidateKafkaSource_SecurityProtocol(t *testing.T) {
	path := field.NewPath("spec").Child("source").Child("config")
	spec := &KafkaSourceSpec{
		Brokers:          []string{"broker:9092"},
		Topic:            "t",
		SecurityProtocol: "SASL_PLAINTEXT",
		SASL: &SASLConfig{
			Mechanism: "scram-sha-256",
			Username:  "user",
			Password:  "pass",
		},
	}
	if errs := validateKafkaSource(spec, path); len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
}

func TestValidateKafkaSink_SecurityProtocol(t *testing.T) {
	path := field.NewPath("spec").Child("sink").Child("config")
	spec := &KafkaSinkSpec{
		Brokers:          []string{"broker:9092"},
		Topic:            "t",
		SecurityProtocol: "SASL_PLAINTEXT",
		SASL: &SASLConfig{
			Mechanism: "scram-sha-256",
			Username:  "user",
			Password:  "pass",
		},
	}
	if errs := validateKafkaSink(spec, path); len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
}
