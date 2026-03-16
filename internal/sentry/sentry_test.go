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

package sentry

import (
	"os"
	"testing"
)

func TestInit_EmptyDSN(t *testing.T) {
	os.Unsetenv(EnvDSN)
	os.Unsetenv(EnvEnvironment)
	os.Unsetenv(EnvTracesSampleRate)
	os.Unsetenv(EnvDebug)
	os.Unsetenv(EnvRelease)

	err := Init()
	if err != nil {
		t.Errorf("Init with empty DSN should return nil, got %v", err)
	}
}

func TestInit_InvalidDSN(t *testing.T) {
	os.Setenv(EnvDSN, "invalid-dsn")
	defer os.Unsetenv(EnvDSN)

	err := Init()
	// Sentry SDK may return error for malformed DSN; we verify Init doesn't panic
	if err != nil {
		t.Logf("Init with invalid DSN returned expected error: %v", err)
	}
}

func TestInit_ValidFormatDSN(t *testing.T) {
	// Use a fake but valid-format DSN; Sentry will reject it but Init may succeed
	// or fail depending on SDK. A typical format: https://key@host/project
	os.Setenv(EnvDSN, "https://abc123@o0.ingest.sentry.io/0")
	defer func() {
		os.Unsetenv(EnvDSN)
		os.Unsetenv(EnvEnvironment)
		os.Unsetenv(EnvTracesSampleRate)
	}()

	// Init may succeed (SDK accepts DSN format) or fail (network/auth).
	// We only verify it doesn't panic.
	_ = Init()
}

func TestEnvConstants(t *testing.T) {
	// Ensure env keys are non-empty for documentation
	if EnvDSN == "" || EnvEnvironment == "" || EnvTracesSampleRate == "" {
		t.Error("Sentry env constants must be non-empty")
	}
}
