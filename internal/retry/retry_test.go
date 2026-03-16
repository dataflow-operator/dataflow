package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestIsTimeoutError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"DeadlineExceeded", context.DeadlineExceeded, true},
		{"wrapped DeadlineExceeded", errors.Join(errors.New("wrap"), context.DeadlineExceeded), true},
		{"timeout in message", errors.New("connection timeout"), true},
		{"Timeout in message", errors.New("connection Timeout"), true},
		{"i/o timeout", errors.New("read tcp: i/o timeout"), true},
		{"deadline exceeded in message", errors.New("context deadline exceeded"), true},
		{"other error", errors.New("something went wrong"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTimeoutError(tt.err); got != tt.want {
				t.Errorf("IsTimeoutError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOnRetry_SuccessFirstTry(t *testing.T) {
	ctx := context.Background()
	calls := 0
	err := OnRetry(ctx, 3, 10*time.Millisecond, func(error) bool { return true }, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Errorf("OnRetry() err = %v, want nil", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestOnRetry_NonRetryableErrorNoRetry(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("permanent error")
	calls := 0
	err := OnRetry(ctx, 3, 10*time.Millisecond, func(err error) bool { return false }, func() error {
		calls++
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Errorf("OnRetry() err = %v, want %v", err, wantErr)
	}
	if calls != 1 {
		t.Errorf("expected 1 call (no retry on non-retryable), got %d", calls)
	}
}

func TestOnRetry_RetryThenSuccess(t *testing.T) {
	ctx := context.Background()
	retryableErr := errors.New("transient")
	calls := 0
	err := OnRetry(ctx, 3, 5*time.Millisecond, func(err error) bool { return err == retryableErr }, func() error {
		calls++
		if calls < 2 {
			return retryableErr
		}
		return nil
	})
	if err != nil {
		t.Errorf("OnRetry() err = %v, want nil", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls (retry then success), got %d", calls)
	}
}

func TestOnRetry_ExhaustRetries(t *testing.T) {
	ctx := context.Background()
	retryableErr := errors.New("transient")
	calls := 0
	err := OnRetry(ctx, 3, 5*time.Millisecond, func(err error) bool { return err == retryableErr }, func() error {
		calls++
		return retryableErr
	})
	if !errors.Is(err, retryableErr) {
		t.Errorf("OnRetry() err = %v, want %v", err, retryableErr)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls (all retries exhausted), got %d", calls)
	}
}

func TestOnRetry_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	retryableErr := errors.New("transient")
	calls := 0
	err := OnRetry(ctx, 3, 100*time.Millisecond, func(err error) bool { return err == retryableErr }, func() error {
		calls++
		return retryableErr
	})
	if err != context.Canceled {
		t.Errorf("OnRetry() err = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call then context cancel, got %d", calls)
	}
}

func TestOnRetry_MixedErrors(t *testing.T) {
	ctx := context.Background()
	retryableErr := errors.New("transient")
	permanentErr := errors.New("permanent")
	calls := 0
	err := OnRetry(ctx, 5, 5*time.Millisecond, func(err error) bool { return err == retryableErr }, func() error {
		calls++
		if calls < 3 {
			return retryableErr
		}
		return permanentErr
	})
	if !errors.Is(err, permanentErr) {
		t.Errorf("OnRetry() err = %v, want %v", err, permanentErr)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls (2 retryable + 1 permanent), got %d", calls)
	}
}

func TestOnTimeout_SuccessFirstTry(t *testing.T) {
	ctx := context.Background()
	calls := 0
	err := OnTimeout(ctx, 3, 10*time.Millisecond, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Errorf("OnTimeout() err = %v, want nil", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestOnTimeout_NonTimeoutErrorNoRetry(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("permanent error")
	calls := 0
	err := OnTimeout(ctx, 3, 10*time.Millisecond, func() error {
		calls++
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Errorf("OnTimeout() err = %v, want %v", err, wantErr)
	}
	if calls != 1 {
		t.Errorf("expected 1 call (no retry on non-timeout), got %d", calls)
	}
}

func TestOnTimeout_RetryThenSuccess(t *testing.T) {
	ctx := context.Background()
	calls := 0
	err := OnTimeout(ctx, 3, 5*time.Millisecond, func() error {
		calls++
		if calls < 2 {
			return context.DeadlineExceeded
		}
		return nil
	})
	if err != nil {
		t.Errorf("OnTimeout() err = %v, want nil", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls (retry then success), got %d", calls)
	}
}

func TestOnTimeout_ExhaustRetries(t *testing.T) {
	ctx := context.Background()
	calls := 0
	err := OnTimeout(ctx, 3, 5*time.Millisecond, func() error {
		calls++
		return context.DeadlineExceeded
	})
	if err != context.DeadlineExceeded {
		t.Errorf("OnTimeout() err = %v, want DeadlineExceeded", err)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls (all retries), got %d", calls)
	}
}

func TestOnTimeout_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	err := OnTimeout(ctx, 3, 100*time.Millisecond, func() error {
		calls++
		return context.DeadlineExceeded
	})
	if err != context.Canceled {
		t.Errorf("OnTimeout() err = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call then context cancel, got %d", calls)
	}
}

func TestIsRetryableTransient(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"timeout", errors.New("i/o timeout"), true},
		{"connection refused", errors.New("connection refused"), true},
		{"connect timeout", errors.New("connect timeout"), true},
		{"connection reset", errors.New("connection reset by peer"), true},
		{"status 503", errors.New("status 503: Service Unavailable"), true},
		{"status 502", errors.New("status 502: Bad Gateway"), true},
		{"internal server error", errors.New("Internal Server Error"), true},
		{"http/500", errors.New("HTTP/500"), true},
		{"bad gateway", errors.New("502 bad gateway"), true},
		{"service temporarily unavailable", errors.New("503 Service Temporarily Unavailable"), true},
		{"permanent error", errors.New("syntax error"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsRetryableTransient(tt.err); got != tt.want {
				t.Errorf("IsRetryableTransient() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsTransientTrinoError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"TOO_MANY_REQUESTS_FAILED", errors.New("Trino query failed: ... (Error: TOO_MANY_REQUESTS_FAILED, Code: 65537)"), true},
		{"transient", errors.New("This is probably a transient issue, so please retry"), true},
		{"worker node", errors.New("Encountered too many errors talking to a worker node"), true},
		{"retry your query", errors.New("please retry your query in a few minutes"), true},
		{"too many errors", errors.New("too many errors talking to a worker"), true},
		{"connect timeout", errors.New("Connect Timeout"), true},
		{"under too much load", errors.New("The node may be under too much load"), true},
		{"node may have crashed", errors.New("The node may have crashed or be under load"), true},
		{"status 503", errors.New("Trino query failed with status 503: <html>503 Service Temporarily Unavailable</html>"), true},
		{"status 502", errors.New("Trino query failed with status 502: Bad Gateway"), true},
		{"connection refused", errors.New("Connection to c-xxx.rw.mdb.yandexcloud.net:6432 refused"), true},
		{"internal server error", errors.New("Internal Server Error (HTTP/500): java.lang.RuntimeException"), true},
		{"http/500", errors.New("Trino query failed: Internal Server Error (HTTP/500)"), true},
		{"generic_internal_error", errors.New("Error: GENERIC_INTERNAL_ERROR, Code: 65536"), true},
		{"service temporarily unavailable", errors.New("503 Service Temporarily Unavailable"), true},
		{"bad gateway", errors.New("502 bad gateway"), true},
		{"permanent error", errors.New("syntax error at line 1"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTransientTrinoError(tt.err); got != tt.want {
				t.Errorf("IsTransientTrinoError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsRetryableForTrino(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"timeout", errors.New("connection timeout"), true},
		{"transient Trino", errors.New("TOO_MANY_REQUESTS_FAILED"), true},
		{"both", errors.New("transient: please retry"), true},
		{"503 from nginx", errors.New("Trino query failed with status 503: <html>503 Service Temporarily Unavailable</html>"), true},
		{"permanent", errors.New("syntax error"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsRetryableForTrino(tt.err); got != tt.want {
				t.Errorf("IsRetryableForTrino() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOnRetryableTrino_TransientThenSuccess(t *testing.T) {
	ctx := context.Background()
	calls := 0
	transientErr := errors.New("Trino: TOO_MANY_REQUESTS_FAILED - transient, please retry your query")
	err := OnRetryableTrino(ctx, 3, 5*time.Millisecond, func() error {
		calls++
		if calls < 2 {
			return transientErr
		}
		return nil
	})
	if err != nil {
		t.Errorf("OnRetryableTrino() err = %v, want nil", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls (retry then success), got %d", calls)
	}
}

func TestOnRetryableTrino_PermanentErrorNoRetry(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("syntax error at line 1")
	calls := 0
	err := OnRetryableTrino(ctx, 3, 10*time.Millisecond, func() error {
		calls++
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Errorf("OnRetryableTrino() err = %v, want %v", err, wantErr)
	}
	if calls != 1 {
		t.Errorf("expected 1 call (no retry on permanent error), got %d", calls)
	}
}

func TestOnRetryableTrino_503ThenSuccess(t *testing.T) {
	ctx := context.Background()
	calls := 0
	err503 := errors.New("Trino query failed with status 503: <html>503 Service Temporarily Unavailable</html>")
	err := OnRetryableTrino(ctx, 5, 5*time.Millisecond, func() error {
		calls++
		if calls < 3 {
			return err503
		}
		return nil
	})
	if err != nil {
		t.Errorf("OnRetryableTrino() err = %v, want nil", err)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls (503 twice then success), got %d", calls)
	}
}

func TestOnRetryableTrino_ConnectionRefusedThenSuccess(t *testing.T) {
	ctx := context.Background()
	calls := 0
	connRefused := errors.New("Connection to c-xxx.rw.mdb.yandexcloud.net:6432 refused. Check that the hostname and port are correct")
	err := OnRetryableTrino(ctx, 5, 5*time.Millisecond, func() error {
		calls++
		if calls < 2 {
			return connRefused
		}
		return nil
	})
	if err != nil {
		t.Errorf("OnRetryableTrino() err = %v, want nil", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls (connection refused then success), got %d", calls)
	}
}

func TestIsTransientClickHouseError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"connection refused", errors.New("connect: connection refused"), true},
		{"connect timeout", errors.New("connect timeout"), true},
		{"TOO_MANY_PARTS", errors.New("DB::Exception: Too many parts"), true},
		{"too many parts", errors.New("too many parts in total"), true},
		{"memory_limit_exceeded", errors.New("Memory limit exceeded"), true},
		{"memory limit", errors.New("Memory limit: would use 1.00 GiB"), true},
		{"connection reset", errors.New("connection reset by peer"), true},
		{"status 503", errors.New("status 503: Service Unavailable"), true},
		{"status 502", errors.New("status 502: Bad Gateway"), true},
		{"timeout", errors.New("i/o timeout"), true},
		{"permanent error", errors.New("syntax error at position 5"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTransientClickHouseError(tt.err); got != tt.want {
				t.Errorf("IsTransientClickHouseError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOnRetryableClickHouse_TransientThenSuccess(t *testing.T) {
	ctx := context.Background()
	calls := 0
	transientErr := errors.New("DB::Exception: Too many parts")
	err := OnRetryableClickHouse(ctx, 3, 5*time.Millisecond, func() error {
		calls++
		if calls < 2 {
			return transientErr
		}
		return nil
	})
	if err != nil {
		t.Errorf("OnRetryableClickHouse() err = %v, want nil", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls (retry then success), got %d", calls)
	}
}

func TestOnRetryableClickHouse_PermanentErrorNoRetry(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("syntax error at position 5")
	calls := 0
	err := OnRetryableClickHouse(ctx, 3, 10*time.Millisecond, func() error {
		calls++
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Errorf("OnRetryableClickHouse() err = %v, want %v", err, wantErr)
	}
	if calls != 1 {
		t.Errorf("expected 1 call (no retry on permanent error), got %d", calls)
	}
}
