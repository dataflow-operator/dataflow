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

package checkpoint

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingStore struct {
	NoopStore
	flushCount atomic.Int32
	flushBlock chan struct{} // if non-nil, Flush waits until closed
	flushErr   error
	entered    chan struct{} // closed once Flush is entered (optional)
}

func (s *recordingStore) Flush(context.Context) error {
	if s.entered != nil {
		select {
		case <-s.entered:
		default:
			close(s.entered)
		}
	}
	if s.flushBlock != nil {
		<-s.flushBlock
	}
	s.flushCount.Add(1)
	return s.flushErr
}

func TestSyncStore_FlushAfterBatchAck_Disabled(t *testing.T) {
	t.Parallel()

	inner := &recordingStore{}
	syncStore := NewSyncStore(inner, false, time.Second)

	err := syncStore.FlushAfterBatchAck(context.Background())
	require.NoError(t, err)
	syncStore.Stop()
	assert.Equal(t, int32(0), inner.flushCount.Load())
}

func TestSyncStore_FlushAfterBatchAck_EnabledAsync(t *testing.T) {
	t.Parallel()

	block := make(chan struct{})
	entered := make(chan struct{})
	inner := &recordingStore{flushBlock: block, entered: entered}
	syncStore := NewSyncStore(inner, true, 0)
	defer syncStore.Stop()

	err := syncStore.FlushAfterBatchAck(context.Background())
	require.NoError(t, err)

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("background flush did not start")
	}
	assert.Equal(t, int32(0), inner.flushCount.Load(), "must not complete Flush before returning from ack path")

	close(block)
	require.Eventually(t, func() bool {
		return inner.flushCount.Load() >= 1
	}, 2*time.Second, 5*time.Millisecond)
}

func TestSyncStore_FlushAfterBatchAck_DoesNotBlockOnSlowFlush(t *testing.T) {
	t.Parallel()

	block := make(chan struct{})
	entered := make(chan struct{})
	inner := &recordingStore{flushBlock: block, entered: entered}
	syncStore := NewSyncStore(inner, true, 0)
	defer syncStore.Stop()

	require.NoError(t, syncStore.FlushAfterBatchAck(context.Background()))

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("background flush did not start")
	}

	// Second ack while first flush is blocked must return without waiting.
	ackDone := make(chan struct{})
	go func() {
		require.NoError(t, syncStore.FlushAfterBatchAck(context.Background()))
		close(ackDone)
	}()
	select {
	case <-ackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("FlushAfterBatchAck blocked on slow Store.Flush")
	}

	close(block)
	require.Eventually(t, func() bool {
		return inner.flushCount.Load() >= 1
	}, 2*time.Second, 5*time.Millisecond)
}

func TestSyncStore_FlushAfterBatchAck_ErrorHandler(t *testing.T) {
	t.Parallel()

	inner := &recordingStore{flushErr: errors.New("k8s patch failed")}
	var got atomic.Value
	var wg sync.WaitGroup
	wg.Add(1)
	syncStore := NewSyncStore(inner, true, 0, WithFlushErrorHandler(func(err error) {
		got.Store(err)
		wg.Done()
	}))
	defer syncStore.Stop()

	require.NoError(t, syncStore.FlushAfterBatchAck(context.Background()))
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("error handler not called")
	}
	err, ok := got.Load().(error)
	require.True(t, ok)
	assert.EqualError(t, err, "k8s patch failed")
}

func TestSyncStore_FlushAfterBatchAck_CoalesceSkipsWithinInterval(t *testing.T) {
	t.Parallel()

	inner := &recordingStore{}
	syncStore := NewSyncStore(inner, true, 40*time.Millisecond)
	defer syncStore.Stop()

	require.NoError(t, syncStore.FlushAfterBatchAck(context.Background()))
	require.Eventually(t, func() bool {
		return inner.flushCount.Load() >= 1
	}, 2*time.Second, 5*time.Millisecond)

	require.NoError(t, syncStore.FlushAfterBatchAck(context.Background()))
	require.NoError(t, syncStore.FlushAfterBatchAck(context.Background()))

	require.Eventually(t, func() bool {
		return inner.flushCount.Load() >= 2
	}, 2*time.Second, 5*time.Millisecond)

	assert.LessOrEqual(t, inner.flushCount.Load(), int32(3))
}

func TestSyncStore_Flush_WakesCoalesceWait(t *testing.T) {
	t.Parallel()

	inner := &recordingStore{}
	syncStore := NewSyncStore(inner, true, time.Hour)
	defer syncStore.Stop()

	require.NoError(t, syncStore.FlushAfterBatchAck(context.Background()))
	require.Eventually(t, func() bool {
		return inner.flushCount.Load() >= 1
	}, 2*time.Second, 5*time.Millisecond)

	// Second ack would wait up to 1h; Sync Flush must not block on that timer.
	require.NoError(t, syncStore.FlushAfterBatchAck(context.Background()))
	done := make(chan error, 1)
	go func() {
		done <- syncStore.Flush(context.Background())
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Flush blocked on coalesce interval")
	}
}
