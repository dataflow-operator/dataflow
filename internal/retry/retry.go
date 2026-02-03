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
