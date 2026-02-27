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

package connectors

import (
	"errors"
	"testing"
)

func TestIsCoordinatorUnavailableError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"coordinator is not available", errors.New("kafka server: The coordinator is not available"), true},
		{"CoordinatorNotAvailable", errors.New("CoordinatorNotAvailable"), true},
		{"wrapped", errors.New("error from consumer: kafka server: The coordinator is not available"), true},
		{"other error", errors.New("connection refused"), false},
		{"other kafka", errors.New("kafka server: Topic not found"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isCoordinatorUnavailableError(tt.err)
			if got != tt.want {
				t.Errorf("isCoordinatorUnavailableError() = %v, want %v", got, tt.want)
			}
		})
	}
}
