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

// BatchWriteContext returns a context detached from parent cancellation.
// Parent cancel (SIGTERM) must not abort in-flight batch IO; timeout bounds duration.
// Uses a shorter timeout when parent is already cancelled (shutdown flush).
func BatchWriteContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := BatchWriteTimeout
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
}

// RunBatchWriteLoop reads messages, batches them, and flushes on size, timer, shutdown, or channel close.
func RunBatchWriteLoop(
	ctx context.Context,
	messages <-chan *types.Message,
	cfg BatchWriteConfig,
	opts BatchWriteOptions,
) error {
	useTimer := cfg.FlushInterval > 0
	var batch []*types.Message
	var flushTimer *time.Timer

	stopTimer := func() {
		if flushTimer != nil {
			flushTimer.Stop()
			flushTimer = nil
		}
	}

	doFlush := func(toFlush []*types.Message) error {
		stopTimer()
		if len(toFlush) == 0 {
			return nil
		}
		batchCtx, cancel := BatchWriteContext(ctx)
		defer cancel()
		if err := opts.OnFlush(batchCtx, toFlush); err != nil {
			return err
		}
		if opts.OnAck != nil {
			opts.OnAck(toFlush)
		}
		return nil
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

	for {
		if useTimer && len(batch) > 0 && flushTimer == nil {
			flushTimer = time.NewTimer(cfg.FlushInterval)
		}

		if useTimer && flushTimer != nil {
			select {
			case <-ctx.Done():
				stopTimer()
				if len(batch) > 0 {
					opts.Logger.Info("Context cancelled, flushing batch", append(opts.LogFields, "batchSize", len(batch))...)
					if err := doFlush(batch); err != nil {
						return err
					}
				}
				return ctx.Err()
			case <-flushTimer.C:
				flushTimer = nil
				if len(batch) == 0 {
					continue
				}
				toFlush := batch
				batch = nil
				if err := doFlush(toFlush); err != nil {
					logFlushError(err, "Failed to write batch on timer", nil, len(toFlush))
					return err
				}
			case msg, ok := <-messages:
				if !ok {
					stopTimer()
					if len(batch) > 0 {
						if err := doFlush(batch); err != nil {
							return err
						}
					}
					return nil
				}
				if opts.OnMessage != nil && !opts.OnMessage(msg) {
					continue
				}
				batch = append(batch, msg)
				if len(batch) >= cfg.MaxBatchSize {
					toFlush := batch
					batch = nil
					if err := doFlush(toFlush); err != nil {
						logFlushError(err, "Failed to write batch", msg, len(toFlush))
						return err
					}
				}
			}
		} else {
			select {
			case <-ctx.Done():
				stopTimer()
				if len(batch) > 0 {
					opts.Logger.Info("Context cancelled, flushing batch", append(opts.LogFields, "batchSize", len(batch))...)
					if err := doFlush(batch); err != nil {
						return err
					}
				}
				return ctx.Err()
			case msg, ok := <-messages:
				if !ok {
					stopTimer()
					if len(batch) > 0 {
						if err := doFlush(batch); err != nil {
							return err
						}
					}
					return nil
				}
				if opts.OnMessage != nil && !opts.OnMessage(msg) {
					continue
				}
				batch = append(batch, msg)
				if len(batch) >= cfg.MaxBatchSize {
					toFlush := batch
					batch = nil
					if err := doFlush(toFlush); err != nil {
						logFlushError(err, "Failed to write batch", msg, len(toFlush))
						return err
					}
				}
			}
		}
	}
}
