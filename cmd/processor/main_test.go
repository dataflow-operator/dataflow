package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsRetryableProcessorError(t *testing.T) {
	assert.True(t, isRetryableProcessorError(context.DeadlineExceeded))
	assert.True(t, isRetryableProcessorError(errors.New("Trino query failed with status 503")))
	assert.False(t, isRetryableProcessorError(errors.New("permission denied")))
}

func TestProcessorSinkErrorMaxRetriesFromEnv(t *testing.T) {
	assert.Equal(t, 0, processorSinkErrorMaxRetriesFromEnv(""))
	assert.Equal(t, 0, processorSinkErrorMaxRetriesFromEnv("-1"))
	assert.Equal(t, 0, processorSinkErrorMaxRetriesFromEnv("abc"))
	assert.Equal(t, 5, processorSinkErrorMaxRetriesFromEnv("5"))
}
