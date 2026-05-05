package retry

import (
	"context"
	"errors"
	"strings"
	"time"
)

// DefaultMaxAttempts is the default number of retry attempts for timeout errors.
const DefaultMaxAttempts = 3

// DefaultInitialBackoff is the initial delay between retries.
const DefaultInitialBackoff = 500 * time.Millisecond

// NessieAppendMaxAttempts is the number of retry attempts for Nessie batch writes.
const NessieAppendMaxAttempts = 5

// NessieAppendInitialBackoff is the initial backoff for Nessie append retries.
const NessieAppendInitialBackoff = 1 * time.Second

// TrinoMaxAttempts is the number of retry attempts for Trino batch writes (transient worker/load errors).
const TrinoMaxAttempts = 5

// TrinoInitialBackoff is the initial backoff for Trino retries (worker may need time to recover).
const TrinoInitialBackoff = 2 * time.Second

// ClickHouseMaxAttempts is the number of retry attempts for ClickHouse batch writes.
const ClickHouseMaxAttempts = 5

// ClickHouseInitialBackoff is the initial backoff for ClickHouse retries.
const ClickHouseInitialBackoff = 2 * time.Second

// IsTimeoutError returns true if err is or wraps context.DeadlineExceeded,
// or if the error message indicates a timeout (e.g. from drivers).
func IsTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "deadline exceeded") ||
		strings.Contains(msg, "i/o timeout")
}

// IsRetryableTransient returns true for generic transient errors (connection refused, timeout, HTTP 5xx).
// Used by connectWithRetry for all connector types (via IsRetryableForConnect).
func IsRetryableTransient(err error) bool {
	if err == nil {
		return false
	}
	if IsTimeoutError(err) {
		return true
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "connection refused") ||
		strings.Contains(lower, "connect timeout") ||
		strings.Contains(lower, "connection reset") ||
		strings.Contains(lower, "status 503") ||
		strings.Contains(lower, "status 502") ||
		strings.Contains(lower, "internal server error") ||
		strings.Contains(lower, "http/500") ||
		strings.Contains(lower, "bad gateway") ||
		strings.Contains(lower, "service temporarily unavailable") ||
		(strings.Contains(lower, "temporary") && strings.Contains(lower, "unavailable"))
}

// isPermanentConnectAuthFailure returns true for credential / OAuth client misconfiguration errors
// that connectWithRetry must not mask with backoff (wrong password, invalid_grant, etc.).
func isPermanentConnectAuthFailure(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "password authentication failed") ||
		strings.Contains(lower, "sasl authentication failed") ||
		strings.Contains(lower, "invalid password") ||
		strings.Contains(lower, "invalid credentials") ||
		strings.Contains(lower, "invalid_grant") ||
		strings.Contains(lower, "unauthorized_client") ||
		strings.Contains(lower, "client authentication failed")
}

// IsRetryableDelayedAccess returns true when the failure may resolve after IAM, ACL, OAuth role,
// or object privileges propagate — same operational intent as retrying Kafka authorization/coordinator issues.
// Covers PostgreSQL (42501, permission denied), Trino (PERMISSION_DENIED, HTTP 403), ClickHouse
// (privilege / ACCESS_DENIED), Nessie/REST (403, catalog access), and selected OAuth token states.
func IsRetryableDelayedAccess(err error) bool {
	if err == nil || isPermanentConnectAuthFailure(err) {
		return false
	}
	lower := strings.ToLower(err.Error())

	// PostgreSQL: insufficient_privilege / SQLSTATE 42501 (object privileges not yet granted).
	if strings.Contains(lower, "permission denied for") ||
		strings.Contains(lower, "42501") ||
		strings.Contains(lower, "insufficient_privilege") ||
		strings.Contains(lower, "insufficient privilege") {
		return true
	}

	// HTTP 403 / Forbidden (Trino "query failed with status 403", proxies, Nessie REST, Keycloak-protected HTTP).
	if strings.Contains(lower, "status 403") ||
		strings.Contains(lower, "http 403") ||
		strings.Contains(lower, "http/403") {
		return true
	}
	if strings.Contains(lower, "403 forbidden") || strings.Contains(lower, "forbidden: 403") {
		return true
	}

	// Trino engine / HTTP error text.
	if strings.Contains(lower, "access denied") ||
		strings.Contains(lower, "permission_denied") {
		return true
	}

	// ClickHouse: privilege and access control (error messages from clickhouse-go / server).
	if strings.Contains(lower, "not enough privileges") ||
		strings.Contains(lower, "not enough privilege") ||
		strings.Contains(lower, "access_denied") ||
		strings.Contains(lower, "not enough rights") {
		return true
	}

	// REST / Iceberg / Nessie style (also appears in wrapped errors from Apache Iceberg clients).
	if strings.Contains(lower, "not authorized") ||
		strings.Contains(lower, "forbidden to") {
		return true
	}

	// OAuth2 / OIDC: token or role not ready yet (avoid classifying invalid_grant as retryable — see isPermanentConnectAuthFailure).
	if strings.Contains(lower, "invalid_token") ||
		strings.Contains(lower, "token expired") ||
		strings.Contains(lower, "token not active") {
		return true
	}
	// 401 on token or Trino/Keycloak round-trip (transient until role or token is valid).
	if strings.Contains(lower, "status 401") || strings.Contains(lower, "http/401") || strings.Contains(lower, "http 401") {
		if strings.Contains(lower, "unauthorized") ||
			strings.Contains(lower, "oauth") ||
			strings.Contains(lower, "bearer") ||
			strings.Contains(lower, "keycloak") ||
			strings.Contains(lower, "openid") ||
			strings.Contains(lower, "jwt") {
			return true
		}
	}

	return false
}

// IsRetryableForConnect returns true if Connect should be retried by connectWithRetry (transient network,
// HTTP 5xx, or delayed access / privilege / OAuth propagation). Permanent credential failures are excluded.
func IsRetryableForConnect(err error) bool {
	if err == nil {
		return false
	}
	if isPermanentConnectAuthFailure(err) {
		return false
	}
	return IsRetryableTransient(err) || IsRetryableDelayedAccess(err)
}

// IsTransientClickHouseError returns true if err looks like a transient ClickHouse error
// (TOO_MANY_PARTS, memory limit, connection refused, etc.).
func IsTransientClickHouseError(err error) bool {
	if err == nil {
		return false
	}
	if IsRetryableTransient(err) {
		return true
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "too_many_parts") ||
		strings.Contains(lower, "too many parts") ||
		strings.Contains(lower, "memory_limit_exceeded") ||
		strings.Contains(lower, "memory limit") ||
		strings.Contains(lower, "connection reset") ||
		(strings.Contains(lower, "temporary") && strings.Contains(lower, "unavailable"))
}

// IsRetryableForClickHouse returns true if the error is retryable for ClickHouse batch writes.
func IsRetryableForClickHouse(err error) bool {
	return IsTransientClickHouseError(err)
}

// OnRetry retries op with exponential backoff when isRetryable returns true.
func OnRetry(ctx context.Context, maxAttempts int, initialBackoff time.Duration, isRetryable func(error) bool, op func() error) error {
	var lastErr error
	backoff := initialBackoff
	for attempt := 0; attempt < maxAttempts; attempt++ {
		lastErr = op()
		if lastErr == nil {
			return nil
		}
		if !isRetryable(lastErr) {
			return lastErr
		}
		if attempt == maxAttempts-1 {
			return lastErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
		}
	}
	return lastErr
}

// OnRetryableClickHouse runs op and retries when it returns a transient ClickHouse error.
func OnRetryableClickHouse(ctx context.Context, maxAttempts int, initialBackoff time.Duration, op func() error) error {
	return OnRetry(ctx, maxAttempts, initialBackoff, IsRetryableForClickHouse, op)
}

// IsTransientTrinoError returns true if err looks like a transient Trino error
// (TOO_MANY_REQUESTS_FAILED, worker overload/crash, "transient", "retry your query",
// HTTP 500/502/503 from proxy/load balancer, connection refused to backend, etc.).
func IsTransientTrinoError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	lower := strings.ToLower(msg)
	return strings.Contains(msg, "TOO_MANY_REQUESTS_FAILED") ||
		strings.Contains(lower, "transient") ||
		strings.Contains(lower, "worker node") ||
		strings.Contains(lower, "retry your query") ||
		strings.Contains(lower, "too many errors") ||
		strings.Contains(lower, "connect timeout") ||
		(strings.Contains(lower, "connection") && strings.Contains(lower, "refused")) ||
		strings.Contains(lower, "under too much load") ||
		strings.Contains(lower, "the node may have crashed") ||
		strings.Contains(lower, "status 503") ||
		strings.Contains(lower, "status 502") ||
		strings.Contains(lower, "internal server error") ||
		strings.Contains(lower, "http/500") ||
		strings.Contains(lower, "generic_internal_error") ||
		strings.Contains(lower, "service temporarily unavailable") ||
		strings.Contains(lower, "bad gateway")
}

// IsRetryableForTrino returns true if the error is a timeout or a transient Trino error.
func IsRetryableForTrino(err error) bool {
	return IsTimeoutError(err) || IsTransientTrinoError(err)
}

// OnRetryableTrino runs op and retries when it returns a timeout or transient Trino error.
// Use TrinoMaxAttempts and TrinoInitialBackoff for batch inserts.
func OnRetryableTrino(ctx context.Context, maxAttempts int, initialBackoff time.Duration, op func() error) error {
	return OnRetry(ctx, maxAttempts, initialBackoff, IsRetryableForTrino, op)
}

// OnTimeout runs op and retries up to maxAttempts times when op returns a timeout error.
// Backoff doubles after each attempt (initialBackoff, 2*initialBackoff, ...).
// If op returns a non-timeout error, it is returned immediately without retry.
func OnTimeout(ctx context.Context, maxAttempts int, initialBackoff time.Duration, op func() error) error {
	return OnRetry(ctx, maxAttempts, initialBackoff, IsTimeoutError, op)
}
