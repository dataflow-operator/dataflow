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

package connectors

import (
	"context"
	"time"

	"github.com/dataflow-operator/dataflow/internal/constants"
	"github.com/dataflow-operator/dataflow/internal/logkeys"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
)

const (
	// BatchWriteTimeout bounds normal batch IO (long Iceberg/S3 uploads, large INSERTs).
	BatchWriteTimeout = 5 * time.Minute
	// BatchShutdownFlushTimeout bounds the final flush after parent context cancellation (SIGTERM).
	BatchShutdownFlushTimeout = 30 * time.Second
)

// BatchWriteConfig holds batching parameters shared by batch-oriented sink connectors.
type BatchWriteConfig struct {
	MaxBatchSize  int
	FlushInterval time.Duration // 0 disables timer-based flush
}

// NewBatchWriteConfig builds batch settings from sink spec pointers.
// defaultBatchSize is used when batchSize is nil (e.g. 100 for ClickHouse/Nessie, 1 for Trino).
func NewBatchWriteConfig(batchSize, flushIntervalSec *int32, defaultBatchSize int) BatchWriteConfig {
	maxBatchSize := defaultBatchSize
	if batchSize != nil {
		maxBatchSize = int(*batchSize)
	}
	if maxBatchSize == 0 {
		maxBatchSize = constants.MaxBatchSizeWhenTimerOnly
	}

	flushIntervalSecVal := 10
	if flushIntervalSec != nil {
		flushIntervalSecVal = int(*flushIntervalSec)
	}
	var flushInterval time.Duration
	if flushIntervalSecVal > 0 {
		flushInterval = time.Duration(flushIntervalSecVal) * time.Second
	}

	return BatchWriteConfig{
		MaxBatchSize:  maxBatchSize,
		FlushInterval: flushInterval,
	}
}

// ApplyAckGranularity forces single-message batches when collapseBatch is true
// (typically message-ack with collapseBatchOnMessageAck enabled / default).
func ApplyAckGranularity(cfg BatchWriteConfig, collapseBatch bool) BatchWriteConfig {
	if collapseBatch {
		cfg.MaxBatchSize = 1
	}
	return cfg
}

// BatchWriteContext returns a context detached from parent cancellation.
// Parent cancel (SIGTERM) must not abort in-flight batch IO; timeout bounds duration.
// Uses a shorter timeout when parent is already cancelled (shutdown flush).
func BatchWriteContext(parent context.Context) (context.Context, context.CancelFunc) {
	return BatchWriteContextWithTimeout(parent, 0)
}

// BatchWriteContextWithTimeout returns a detached context with optional custom timeout.
// customTimeout <= 0 means BatchWriteTimeout.
func BatchWriteContextWithTimeout(parent context.Context, customTimeout time.Duration) (context.Context, context.CancelFunc) {
	timeout := BatchWriteTimeout
	if customTimeout > 0 {
		timeout = customTimeout
	}
	if parent.Err() != nil {
		timeout = BatchShutdownFlushTimeout
	}
	return context.WithTimeout(context.Background(), timeout)
}

// BatchWriteOptions configures RunBatchWriteLoop.
//
// Connectors with non-standard accumulation (e.g. PostgreSQL pgx.Batch + soft delete)
// should implement their own loop but must use BatchWriteContext for flush IO.
type BatchWriteOptions struct {
	Logger logr.Logger
	// OnFlush performs the sink write. batchCtx is detached from parent cancellation.
	OnFlush func(batchCtx context.Context, msgs []*types.Message) error
	// OnAck is called after a successful OnFlush. If nil, the connector must ack inside OnFlush.
	OnAck func(msgs []*types.Message)
	// OnMessage is called before appending each message. Return false to skip (e.g. unmarshal error).
	OnMessage func(msg *types.Message) bool
	// LogFields are appended to error logs from the loop (e.g. "table", name).
	LogFields []any
	// FlushTimeout overrides default batch write timeout for normal (non-shutdown) flushes.
	// <= 0 uses BatchWriteTimeout.
	FlushTimeout time.Duration
}

// BatchFlushLogFields returns structured log fields for a successful sink batch flush.
func BatchFlushLogFields(batchSize int, duration time.Duration, reason string, totalFlushed int) []any {
	return []any{
		"batchSize", batchSize,
		logkeys.DurationMS, duration.Milliseconds(),
		logkeys.FlushReason, reason,
		"messages_flushed_total", totalFlushed,
	}
}

type batchFlushResult struct {
	err      error
	n        int
	reason   string
	duration time.Duration
}

// RunBatchWriteLoop reads messages, batches them, and flushes on size, timer, shutdown, or channel close.
//
// Double-buffer: at most one OnFlush runs asynchronously while the loop continues accumulating the
// next batch (bounded to one in-flight flush + one active batch). Flush/ack order is preserved.
func RunBatchWriteLoop(
	ctx context.Context,
	messages <-chan *types.Message,
	cfg BatchWriteConfig,
	opts BatchWriteOptions,
) error {
	useTimer := cfg.FlushInterval > 0
	var batch []*types.Message
	var flushTimer *time.Timer
	var totalFlushed int
	var flushDone <-chan batchFlushResult

	stopTimer := func() {
		if flushTimer != nil {
			flushTimer.Stop()
			flushTimer = nil
		}
	}

	logFlushError := func(err error, event string, msg *types.Message, batchSize int) {
		fields := append([]any{}, opts.LogFields...)
		if msg != nil {
			fields = append(fields, logkeys.MessageID, types.MessageID(msg))
		}
		if batchSize > 0 {
			fields = append(fields, "batchSize", batchSize)
		}
		opts.Logger.Error(err, event, fields...)
	}

	applyFlushResult := func(res batchFlushResult) error {
		if res.err != nil {
			return res.err
		}
		totalFlushed += res.n
		fields := append([]any{}, opts.LogFields...)
		fields = append(fields, BatchFlushLogFields(res.n, res.duration, res.reason, totalFlushed)...)
		opts.Logger.Info("Batch flushed", fields...)
		return nil
	}

	waitInFlight := func() error {
		if flushDone == nil {
			return nil
		}
		res := <-flushDone
		flushDone = nil
		return applyFlushResult(res)
	}

	startAsyncFlush := func(toFlush []*types.Message, reason string) {
		stopTimer()
		ch := make(chan batchFlushResult, 1)
		flushDone = ch
		go func(msgs []*types.Message, reason string) {
			flushStart := time.Now()
			batchCtx, cancel := BatchWriteContextWithTimeout(ctx, opts.FlushTimeout)
			defer cancel()
			err := opts.OnFlush(batchCtx, msgs)
			if err == nil && opts.OnAck != nil {
				opts.OnAck(msgs)
			}
			ch <- batchFlushResult{
				err:      err,
				n:        len(msgs),
				reason:   reason,
				duration: time.Since(flushStart),
			}
		}(toFlush, reason)
	}

	// requestFlush waits for any in-flight flush (preserving order), then starts a new async flush.
	requestFlush := func(toFlush []*types.Message, reason string) error {
		if len(toFlush) == 0 {
			return nil
		}
		if err := waitInFlight(); err != nil {
			return err
		}
		startAsyncFlush(toFlush, reason)
		return nil
	}

	// syncFlush drains in-flight work then flushes synchronously (shutdown / channel close).
	syncFlush := func(toFlush []*types.Message, reason string) error {
		if err := waitInFlight(); err != nil {
			return err
		}
		if len(toFlush) == 0 {
			return nil
		}
		stopTimer()
		flushStart := time.Now()
		batchCtx, cancel := BatchWriteContextWithTimeout(ctx, opts.FlushTimeout)
		defer cancel()
		if err := opts.OnFlush(batchCtx, toFlush); err != nil {
			return err
		}
		if opts.OnAck != nil {
			opts.OnAck(toFlush)
		}
		return applyFlushResult(batchFlushResult{
			n:        len(toFlush),
			reason:   reason,
			duration: time.Since(flushStart),
		})
	}

	appendMessage := func(msg *types.Message) error {
		if opts.OnMessage != nil && !opts.OnMessage(msg) {
			return nil
		}
		batch = append(batch, msg)
		if len(batch) < cfg.MaxBatchSize {
			return nil
		}
		toFlush := batch
		batch = nil
		if err := requestFlush(toFlush, "size"); err != nil {
			logFlushError(err, "Failed to write batch", msg, len(toFlush))
			return err
		}
		return nil
	}

	for {
		if useTimer && len(batch) > 0 && flushTimer == nil {
			flushTimer = time.NewTimer(cfg.FlushInterval)
		}

		var timerC <-chan time.Time
		if flushTimer != nil {
			timerC = flushTimer.C
		}

		select {
		case <-ctx.Done():
			stopTimer()
			if err := syncFlush(batch, "shutdown"); err != nil {
				return err
			}
			return ctx.Err()

		case res := <-flushDone:
			flushDone = nil
			if err := applyFlushResult(res); err != nil {
				logFlushError(err, "Failed to write batch", nil, res.n)
				return err
			}

		case <-timerC:
			flushTimer = nil
			if len(batch) == 0 {
				continue
			}
			toFlush := batch
			batch = nil
			if err := requestFlush(toFlush, "timer"); err != nil {
				logFlushError(err, "Failed to write batch on timer", nil, len(toFlush))
				return err
			}

		case msg, ok := <-messages:
			if !ok {
				stopTimer()
				if err := syncFlush(batch, "channel_closed"); err != nil {
					return err
				}
				return nil
			}
			if err := appendMessage(msg); err != nil {
				return err
			}
		}
	}
}
