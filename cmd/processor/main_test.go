package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIsRetryableProcessorError(t *testing.T) {
	assert.True(t, isRetryableProcessorError(context.DeadlineExceeded))
	assert.True(t, isRetryableProcessorError(errors.New("Trino query failed with status 503")))
	assert.False(t, isRetryableProcessorError(errors.New("permission denied")))
}

func TestWriteLivez(t *testing.T) {
	t.Parallel()

	t.Run("not ready", func(t *testing.T) {
		t.Parallel()
		rr := httptest.NewRecorder()
		writeLivez(rr, func() bool { return false }, func(time.Duration) bool { return false }, time.Minute)
		assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
		assert.Contains(t, rr.Body.String(), "not ready")
	})

	t.Run("stale progress", func(t *testing.T) {
		t.Parallel()
		rr := httptest.NewRecorder()
		writeLivez(rr, func() bool { return true }, func(time.Duration) bool { return true }, time.Minute)
		assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
		assert.Contains(t, rr.Body.String(), "stale")
	})

	t.Run("ok", func(t *testing.T) {
		t.Parallel()
		rr := httptest.NewRecorder()
		writeLivez(rr, func() bool { return true }, func(time.Duration) bool { return false }, time.Minute)
		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Equal(t, "ok\n", rr.Body.String())
	})
}

func TestProgressTimeoutFromEnv(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 10*time.Minute, progressTimeoutFromEnv(""))
	assert.Equal(t, time.Duration(0), progressTimeoutFromEnv("0"))
	assert.Equal(t, 30*time.Minute, progressTimeoutFromEnv("1800"))
}

func TestProcessorSinkErrorMaxRetriesFromEnv(t *testing.T) {
	assert.Equal(t, 0, processorSinkErrorMaxRetriesFromEnv(""))
	assert.Equal(t, 0, processorSinkErrorMaxRetriesFromEnv("-1"))
	assert.Equal(t, 0, processorSinkErrorMaxRetriesFromEnv("abc"))
	assert.Equal(t, 5, processorSinkErrorMaxRetriesFromEnv("5"))
}
