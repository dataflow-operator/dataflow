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

package errors

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

func TestGetErrorType(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: "unknown",
		},
		{
			name:     "context canceled",
			err:      context.Canceled,
			expected: "context_canceled",
		},
		{
			name:     "wrapped context canceled",
			err:      errors.Join(errors.New("wrap"), context.Canceled),
			expected: "context_canceled",
		},
		{
			name:     "context deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: "timeout",
		},
		{
			name:     "wrapped deadline exceeded",
			err:      errors.Join(errors.New("wrap"), context.DeadlineExceeded),
			expected: "timeout",
		},
		{
			name:     "timeout in message",
			err:      errors.New("connection timeout"),
			expected: "timeout",
		},
		{
			name:     "deadline exceeded in message",
			err:      errors.New("context deadline exceeded"),
			expected: "timeout",
		},
		{
			name:     "i/o timeout in message",
			err:      errors.New("read tcp: i/o timeout"),
			expected: "timeout",
		},
		{
			name:     "PostgreSQL constraint violation",
			err:      &pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint"},
			expected: "constraint_violation",
		},
		{
			name:     "PostgreSQL foreign key violation",
			err:      &pgconn.PgError{Code: "23503", Message: "foreign key violation"},
			expected: "constraint_violation",
		},
		{
			name:     "PostgreSQL not null violation",
			err:      &pgconn.PgError{Code: "23502", Message: "null value in column"},
			expected: "constraint_violation",
		},
		{
			name:     "PostgreSQL syntax error not constraint",
			err:      &pgconn.PgError{Code: "42601", Message: "syntax error"},
			expected: "invalid_data",
		},
		{
			name:     "connection refused",
			err:      errors.New("connection refused"),
			expected: "connection_error",
		},
		{
			name:     "not connected",
			err:      errors.New("not connected, call Connect first"),
			expected: "connection_error",
		},
		{
			name:     "failed to connect",
			err:      errors.New("failed to connect to PostgreSQL: dial tcp: connection refused"),
			expected: "connection_error",
		},
		{
			name:     "connection failure",
			err:      errors.New("connection failure"),
			expected: "connection_error",
		},
		{
			name:     "invalid json",
			err:      errors.New("failed to parse invalid json"),
			expected: "invalid_data",
		},
		{
			name:     "parse error",
			err:      errors.New("failed to parse message JSON"),
			expected: "invalid_data",
		},
		{
			name:     "schema error",
			err:      errors.New("Avro schema is required"),
			expected: "invalid_data",
		},
		{
			name:     "validation error",
			err:      errors.New("validation failed: field required"),
			expected: "invalid_data",
		},
		{
			name:     "syntax error",
			err:      errors.New("syntax error at line 1"),
			expected: "invalid_data",
		},
		{
			name:     "Trino TOO_MANY_REQUESTS_FAILED",
			err:      errors.New("Trino query failed: TOO_MANY_REQUESTS_FAILED"),
			expected: "transient",
		},
		{
			name:     "transient in message",
			err:      errors.New("This is probably a transient issue, please retry"),
			expected: "transient",
		},
		{
			name:     "retry your query",
			err:      errors.New("please retry your query in a few minutes"),
			expected: "transient",
		},
		{
			name:     "worker node",
			err:      errors.New("Encountered too many errors talking to a worker node"),
			expected: "transient",
		},
		{
			name:     "under too much load",
			err:      errors.New("The node may be under too much load"),
			expected: "transient",
		},
		{
			name:     "SASL auth",
			err:      errors.New("SASL username is required but not provided"),
			expected: "auth_error",
		},
		{
			name:     "invalid password",
			err:      errors.New("invalid password"),
			expected: "auth_error",
		},
		{
			name:     "unauthorized",
			err:      errors.New("unauthorized: token expired"),
			expected: "auth_error",
		},
		{
			name:     "authentication failed",
			err:      errors.New("authentication failed"),
			expected: "auth_error",
		},
		{
			name:     "unknown error",
			err:      errors.New("random unexpected error"),
			expected: "unknown",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetErrorType(tt.err)
			assert.Equal(t, tt.expected, got, "GetErrorType(%v) = %q, want %q", tt.err, got, tt.expected)
		})
	}
}
