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

// TrinoMaxAttempts is the number of retry attempts for Trino batch writes (transient worker/load errors).
const TrinoMaxAttempts = 5

// TrinoInitialBackoff is the initial backoff for Trino retries (worker may need time to recover).
const TrinoInitialBackoff = 2 * time.Second

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

// IsTransientTrinoError returns true if err looks like a transient Trino error
// (TOO_MANY_REQUESTS_FAILED, worker overload/crash, "transient", "retry your query").
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
		strings.Contains(lower, "under too much load") ||
		strings.Contains(lower, "the node may have crashed")
}

// IsRetryableForTrino returns true if the error is a timeout or a transient Trino error.
func IsRetryableForTrino(err error) bool {
	return IsTimeoutError(err) || IsTransientTrinoError(err)
}

// OnRetryableTrino runs op and retries when it returns a timeout or transient Trino error.
// Use TrinoMaxAttempts and TrinoInitialBackoff for batch inserts.
func OnRetryableTrino(ctx context.Context, maxAttempts int, initialBackoff time.Duration, op func() error) error {
	var lastErr error
	backoff := initialBackoff
	for attempt := 0; attempt < maxAttempts; attempt++ {
		lastErr = op()
		if lastErr == nil {
			return nil
		}
		if !IsRetryableForTrino(lastErr) {
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

// OnTimeout runs op and retries up to maxAttempts times when op returns a timeout error.
// Backoff doubles after each attempt (initialBackoff, 2*initialBackoff, ...).
// If op returns a non-timeout error, it is returned immediately without retry.
func OnTimeout(ctx context.Context, maxAttempts int, initialBackoff time.Duration, op func() error) error {
	var lastErr error
	backoff := initialBackoff
	for attempt := 0; attempt < maxAttempts; attempt++ {
		lastErr = op()
		if lastErr == nil {
			return nil
		}
		if !IsTimeoutError(lastErr) {
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
