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
