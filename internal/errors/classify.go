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
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// GetErrorType classifies an error and returns a semantic error type string
// for use in metrics (e.g. TaskStageErrors, TransformerErrors).
// Order of checks matters: first match wins.
func GetErrorType(err error) string {
	if err == nil {
		return "unknown"
	}
	if errors.Is(err, context.Canceled) {
		return "context_canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && len(pgErr.Code) >= 2 && strings.HasPrefix(pgErr.Code, "23") {
		return "constraint_violation"
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "deadline exceeded") ||
		strings.Contains(msg, "i/o timeout") {
		return "timeout"
	}
	if strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "not connected") ||
		strings.Contains(msg, "failed to connect") ||
		strings.Contains(msg, "connection failure") {
		return "connection_error"
	}
	// Check auth_error before invalid_data so "invalid password" is classified as auth_error
	if strings.Contains(msg, "authentication") ||
		strings.Contains(msg, "invalid password") ||
		strings.Contains(msg, "unauthorized") ||
		strings.Contains(msg, "sasl") {
		return "auth_error"
	}
	if strings.Contains(msg, "too_many_requests_failed") ||
		strings.Contains(msg, "transient") ||
		strings.Contains(msg, "retry your query") ||
		strings.Contains(msg, "worker node") ||
		strings.Contains(msg, "under too much load") ||
		strings.Contains(msg, "too many errors") ||
		strings.Contains(msg, "connect timeout") ||
		strings.Contains(msg, "the node may have crashed") {
		return "transient"
	}
	if strings.Contains(msg, "json") ||
		strings.Contains(msg, "parse") ||
		strings.Contains(msg, "schema") ||
		strings.Contains(msg, "validation") ||
		strings.Contains(msg, "invalid") ||
		strings.Contains(msg, "syntax error") {
		return "invalid_data"
	}
	return "unknown"
}
