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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchWriteContext(t *testing.T) {
	t.Run("active parent", func(t *testing.T) {
		parent := context.Background()
		ctx, cancel := BatchWriteContext(parent)
		defer cancel()

		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		assert.InDelta(t, time.Now().Add(BatchWriteTimeout).Unix(), deadline.Unix(), 2)
	})

	t.Run("cancelled parent", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(context.Background())
		parentCancel()

		ctx, cancel := BatchWriteContext(parent)
		defer cancel()

		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		assert.InDelta(t, time.Now().Add(BatchShutdownFlushTimeout).Unix(), deadline.Unix(), 2)
	})

	t.Run("custom timeout", func(t *testing.T) {
		parent := context.Background()
		ctx, cancel := BatchWriteContextWithTimeout(parent, 45*time.Second)
		defer cancel()

		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		assert.InDelta(t, time.Now().Add(45*time.Second).Unix(), deadline.Unix(), 2)
	})
}

func TestApplyAckGranularity(t *testing.T) {
	cfg := BatchWriteConfig{MaxBatchSize: 100, FlushInterval: 5 * time.Second}
	assert.Equal(t, 100, cfg.MaxBatchSize)

	collapsed := ApplyAckGranularity(cfg, true)
	assert.Equal(t, 1, collapsed.MaxBatchSize)
	assert.Equal(t, 5*time.Second, collapsed.FlushInterval)

	preserved := ApplyAckGranularity(cfg, false)
	assert.Equal(t, 100, preserved.MaxBatchSize)
}

func TestNewBatchWriteConfig(t *testing.T) {
	batchSizeVal := int32(50)
	flushSecVal := int32(5)
	cfg := NewBatchWriteConfig(&batchSizeVal, &flushSecVal, 100)
	assert.Equal(t, 50, cfg.MaxBatchSize)
	assert.Equal(t, 5*time.Second, cfg.FlushInterval)

	zero := int32(0)
	ten := int32(10)
	cfgTimerOnly := NewBatchWriteConfig(&zero, &ten, 100)
	assert.Equal(t, 10000, cfgTimerOnly.MaxBatchSize)
}

func TestBatchFlushLogFields(t *testing.T) {
	fields := BatchFlushLogFields(100, 250*time.Millisecond, "size", 500)
	fieldMap := make(map[string]any)
	for i := 0; i < len(fields); i += 2 {
		fieldMap[fields[i].(string)] = fields[i+1]
	}
	assert.Equal(t, 100, fieldMap["batchSize"])
	assert.Equal(t, int64(250), fieldMap["duration_ms"])
	assert.Equal(t, "size", fieldMap["flush_reason"])
	assert.Equal(t, 500, fieldMap["messages_flushed_total"])
}

func TestRunBatchWriteLoop_BatchSizeFlush(t *testing.T) {
	ctx := context.Background()
	ch := make(chan *types.Message, 4)
	ch <- types.NewMessage([]byte(`{"a":1}`))
	ch <- types.NewMessage([]byte(`{"a":2}`))
	close(ch)

	var flushCount atomic.Int32
	err := RunBatchWriteLoop(ctx, ch, BatchWriteConfig{MaxBatchSize: 2, FlushInterval: 0}, BatchWriteOptions{
		Logger: logr.Discard(),
		OnFlush: func(batchCtx context.Context, msgs []*types.Message) error {
			assert.NoError(t, batchCtx.Err())
			flushCount.Add(1)
			assert.Len(t, msgs, 2)
			return nil
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), flushCount.Load())
}

func TestRunBatchWriteLoop_ShutdownFlush(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// Buffered channel: message must be queued before the loop starts (unbuffered send blocks forever).
	ch := make(chan *types.Message, 1)
	ch <- types.NewMessage([]byte(`{"a":1}`))

	var flushed atomic.Bool
	batchReady := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- RunBatchWriteLoop(ctx, ch, BatchWriteConfig{MaxBatchSize: 100, FlushInterval: time.Hour}, BatchWriteOptions{
			Logger: logr.Discard(),
			OnMessage: func(*types.Message) bool {
				select {
				case batchReady <- struct{}{}:
				default:
				}
				return true
			},
			OnFlush: func(batchCtx context.Context, msgs []*types.Message) error {
				flushed.Store(true)
				assert.NoError(t, batchCtx.Err())
				assert.Len(t, msgs, 1)
				return nil
			},
		})
	}()

	select {
	case <-batchReady:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message to enter batch")
	}
	err := <-done
	require.ErrorIs(t, err, context.Canceled)
	assert.True(t, flushed.Load())
}

func TestRunBatchWriteLoop_InFlightCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan *types.Message, 1)
	ch <- types.NewMessage([]byte(`{"a":1}`))

	var gotDetached atomic.Bool
	batchReady := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- RunBatchWriteLoop(ctx, ch, BatchWriteConfig{MaxBatchSize: 100, FlushInterval: time.Hour}, BatchWriteOptions{
			Logger: logr.Discard(),
			OnMessage: func(*types.Message) bool {
				select {
				case batchReady <- struct{}{}:
				default:
				}
				return true
			},
			OnFlush: func(batchCtx context.Context, msgs []*types.Message) error {
				cancel() // simulate SIGTERM during upload
				assert.NoError(t, batchCtx.Err(), "flush must use detached context")
				gotDetached.Store(true)
				return nil
			},
		})
	}()

	select {
	case <-batchReady:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message to enter batch")
	}
	err := <-done
	require.ErrorIs(t, err, context.Canceled)
	assert.True(t, gotDetached.Load())
}

func TestRunBatchWriteLoop_OnAck_NotifiesProgress(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ch := make(chan *types.Message, 1)
	ch <- types.NewMessage([]byte(`{}`))
	close(ch)

	var progressCalls int
	sink := &progressRecorder{}
	sink.SetProgressCallback(func() {
		progressCalls++
	})

	err := RunBatchWriteLoop(ctx, ch, BatchWriteConfig{MaxBatchSize: 1, FlushInterval: 0}, BatchWriteOptions{
		Logger: logr.Discard(),
		OnFlush: func(_ context.Context, _ []*types.Message) error {
			return nil
		},
		OnAck: func(msgs []*types.Message) {
			sink.AckMessagesAndNotifyProgress(msgs)
		},
	})
	if err != nil {
		t.Fatalf("RunBatchWriteLoop: %v", err)
	}
	if progressCalls != 1 {
		t.Fatalf("progressCalls = %d, want 1", progressCalls)
	}
}

func TestRunBatchWriteLoop_OnAck(t *testing.T) {
	ctx := context.Background()
	ch := make(chan *types.Message, 1)
	msg := types.NewMessage([]byte(`{}`))
	ch <- msg
	close(ch)

	var acked atomic.Bool
	err := RunBatchWriteLoop(ctx, ch, BatchWriteConfig{MaxBatchSize: 1, FlushInterval: 0}, BatchWriteOptions{
		Logger:  logr.Discard(),
		OnFlush: func(context.Context, []*types.Message) error { return nil },
		OnAck: func(msgs []*types.Message) {
			assert.Len(t, msgs, 1)
			acked.Store(true)
		},
	})
	require.NoError(t, err)
	assert.True(t, acked.Load())
}

func TestRunBatchWriteLoop_OnMessageSkip(t *testing.T) {
	ctx := context.Background()
	ch := make(chan *types.Message, 2)
	ch <- types.NewMessage([]byte(`skip`))
	ch <- types.NewMessage([]byte(`keep`))
	close(ch)

	err := RunBatchWriteLoop(ctx, ch, BatchWriteConfig{MaxBatchSize: 1, FlushInterval: 0}, BatchWriteOptions{
		Logger: logr.Discard(),
		OnMessage: func(msg *types.Message) bool {
			return string(msg.Data) != "skip"
		},
		OnFlush: func(_ context.Context, msgs []*types.Message) error {
			require.Len(t, msgs, 1)
			assert.Equal(t, []byte("keep"), msgs[0].Data)
			return nil
		},
	})
	require.NoError(t, err)
}

func TestRunBatchWriteLoop_FlushError(t *testing.T) {
	ctx := context.Background()
	ch := make(chan *types.Message, 1)
	ch <- types.NewMessage([]byte(`{}`))
	close(ch)

	wantErr := errors.New("sink failed")
	err := RunBatchWriteLoop(ctx, ch, BatchWriteConfig{MaxBatchSize: 1, FlushInterval: 0}, BatchWriteOptions{
		Logger: logr.Discard(),
		OnFlush: func(context.Context, []*types.Message) error {
			return wantErr
		},
	})
	require.ErrorIs(t, err, wantErr)
}

func TestRunBatchWriteLoop_DoubleBufferOverlap(t *testing.T) {
	ctx := context.Background()
	ch := make(chan *types.Message, 8)

	flushGate := make(chan struct{})
	firstFlushStarted := make(chan struct{})
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	var received atomic.Int32
	var flushOrder []int
	var flushMu sync.Mutex
	var ackOrder []int

	for i := 1; i <= 4; i++ {
		ch <- types.NewMessage([]byte(fmt.Sprintf(`{"n":%d}`, i)))
	}
	close(ch)

	done := make(chan error, 1)
	go func() {
		done <- RunBatchWriteLoop(ctx, ch, BatchWriteConfig{MaxBatchSize: 2, FlushInterval: 0}, BatchWriteOptions{
			Logger: logr.Discard(),
			OnMessage: func(*types.Message) bool {
				received.Add(1)
				return true
			},
			OnFlush: func(_ context.Context, msgs []*types.Message) error {
				cur := inFlight.Add(1)
				for {
					old := maxInFlight.Load()
					if cur <= old || maxInFlight.CompareAndSwap(old, cur) {
						break
					}
				}
				defer inFlight.Add(-1)

				n := len(msgs)
				flushMu.Lock()
				flushOrder = append(flushOrder, n)
				first := len(flushOrder) == 1
				flushMu.Unlock()

				if first {
					close(firstFlushStarted)
					<-flushGate
				}
				return nil
			},
			OnAck: func(msgs []*types.Message) {
				flushMu.Lock()
				ackOrder = append(ackOrder, len(msgs))
				flushMu.Unlock()
			},
		})
	}()

	select {
	case <-firstFlushStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first flush")
	}

	// While first flush is blocked, the loop should keep reading and fill the next batch.
	require.Eventually(t, func() bool {
		return received.Load() >= 4
	}, 2*time.Second, 5*time.Millisecond, "second batch must accumulate during in-flight flush")
	close(flushGate)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for loop exit")
	}

	assert.Equal(t, int32(1), maxInFlight.Load(), "at most one OnFlush in flight")
	flushMu.Lock()
	defer flushMu.Unlock()
	assert.Equal(t, []int{2, 2}, flushOrder)
	assert.Equal(t, []int{2, 2}, ackOrder, "OnAck order follows flush order")
}

func TestRunBatchWriteLoop_DoubleBufferFlushErrorStops(t *testing.T) {
	ctx := context.Background()
	ch := make(chan *types.Message, 4)
	ch <- types.NewMessage([]byte(`1`))
	ch <- types.NewMessage([]byte(`2`))
	ch <- types.NewMessage([]byte(`3`))
	close(ch)

	wantErr := errors.New("boom")
	var flushes atomic.Int32
	err := RunBatchWriteLoop(ctx, ch, BatchWriteConfig{MaxBatchSize: 1, FlushInterval: 0}, BatchWriteOptions{
		Logger: logr.Discard(),
		OnFlush: func(_ context.Context, _ []*types.Message) error {
			if flushes.Add(1) == 1 {
				return wantErr
			}
			return nil
		},
	})
	require.ErrorIs(t, err, wantErr)
	assert.Equal(t, int32(1), flushes.Load())
}
