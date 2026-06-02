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

package controller

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestProcessorProgressTimeoutSeconds(t *testing.T) {
	t.Setenv("PROCESSOR_PROGRESS_TIMEOUT_SECONDS", "")
	if got := processorProgressTimeoutSeconds(); got != "3600" {
		t.Fatalf("default = %q, want 3600", got)
	}

	t.Setenv("PROCESSOR_PROGRESS_TIMEOUT_SECONDS", "900")
	if got := processorProgressTimeoutSeconds(); got != "900" {
		t.Fatalf("custom = %q, want 900", got)
	}
}

func TestProcessorLivenessProbe(t *testing.T) {
	probe := processorLivenessProbe()
	if probe == nil {
		t.Fatal("probe is nil")
	}
	if probe.HTTPGet == nil {
		t.Fatal("HTTPGet is nil")
	}
	if probe.HTTPGet.Path != "/livez" {
		t.Fatalf("path = %q, want /livez", probe.HTTPGet.Path)
	}
	if probe.HTTPGet.Port != intstr.FromInt(9090) {
		t.Fatalf("port = %v, want 9090", probe.HTTPGet.Port)
	}
	if probe.PeriodSeconds != 30 {
		t.Fatalf("periodSeconds = %d, want 30", probe.PeriodSeconds)
	}
	if probe.TimeoutSeconds != 5 {
		t.Fatalf("timeoutSeconds = %d, want 5", probe.TimeoutSeconds)
	}
	if probe.FailureThreshold != 3 {
		t.Fatalf("failureThreshold = %d, want 3", probe.FailureThreshold)
	}
}

func TestProcessorStartupProbe(t *testing.T) {
	probe := processorStartupProbe()
	if probe == nil {
		t.Fatal("probe is nil")
	}
	if probe.HTTPGet == nil {
		t.Fatal("HTTPGet is nil")
	}
	if probe.HTTPGet.Path != "/readyz" {
		t.Fatalf("path = %q, want /readyz", probe.HTTPGet.Path)
	}
	if probe.FailureThreshold != 120 {
		t.Fatalf("failureThreshold = %d, want 120", probe.FailureThreshold)
	}
}

func TestProcessorLogLevelFromEnv(t *testing.T) {
	t.Setenv("PROCESSOR_LOG_LEVEL", "")
	if got := processorLogLevel(); got != "info" {
		t.Fatalf("default = %q, want info", got)
	}
	t.Setenv("PROCESSOR_LOG_LEVEL", "debug")
	if got := processorLogLevel(); got != "debug" {
		t.Fatalf("custom = %q, want debug", got)
	}
}
