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
