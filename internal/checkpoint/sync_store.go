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
	"sync"
	"time"
)

// BatchAckSyncer flushes pending checkpoint data after a successful sink batch ack.
type BatchAckSyncer interface {
	FlushAfterBatchAck(ctx context.Context) error
}

// FlushErrorHandler receives asynchronous checkpoint flush failures.
// The write/ack path must not wait on Kubernetes API; errors surface here instead.
type FlushErrorHandler func(err error)

// SyncStore wraps a Store with optional flush-after-ack behavior.
// FlushAfterBatchAck never blocks on the underlying Store.Flush (K8s Get/Patch);
// it only marks pending work for a coalesced background writer.
type SyncStore struct {
	inner       Store
	syncOnAck   bool
	minInterval time.Duration

	mu            sync.Mutex
	lastFlush     time.Time
	dirty         bool
	workerRunning bool
	stopped       bool
	onFlushErr    FlushErrorHandler

	flushMu sync.Mutex // serializes inner.Flush (ack worker vs Sync Flush)

	wakeCh    chan struct{}
	flushDone *sync.Cond
}

// SyncStoreOption configures SyncStore.
type SyncStoreOption func(*SyncStore)

// WithFlushErrorHandler sets the callback for background flush failures.
func WithFlushErrorHandler(h FlushErrorHandler) SyncStoreOption {
	return func(s *SyncStore) {
		s.onFlushErr = h
	}
}

// NewSyncStore wraps inner with sync-on-ack behavior.
// minInterval coalesces ack-triggered flushes when syncOnAck is true (0 = flush ASAP on worker).
func NewSyncStore(inner Store, syncOnAck bool, minInterval time.Duration, opts ...SyncStoreOption) *SyncStore {
	s := &SyncStore{
		inner:       inner,
		syncOnAck:   syncOnAck,
		minInterval: minInterval,
		wakeCh:      make(chan struct{}, 1),
	}
	s.flushDone = sync.NewCond(&s.mu)
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *SyncStore) Load(ctx context.Context, sourceType string) ([]byte, error) {
	return s.inner.Load(ctx, sourceType)
}

func (s *SyncStore) Save(ctx context.Context, sourceType string, data []byte) error {
	return s.inner.Save(ctx, sourceType, data)
}

// Flush forces an immediate write of pending checkpoint data (e.g. on shutdown).
// Wakes any coalesce wait so shutdown is not blocked by minInterval.
func (s *SyncStore) Flush(ctx context.Context) error {
	s.mu.Lock()
	s.dirty = false
	s.mu.Unlock()
	s.signalWake()
	s.waitForWorker()
	return s.doFlush(ctx)
}

func (s *SyncStore) Clear(ctx context.Context, sourceType string) error {
	return s.inner.Clear(ctx, sourceType)
}

// Stop prevents new background flush workers and wakes coalesce waits.
// Safe to call multiple times. Does not Flush — call Flush before Stop on shutdown.
func (s *SyncStore) Stop() {
	s.mu.Lock()
	s.stopped = true
	s.dirty = false
	s.mu.Unlock()
	s.signalWake()
	s.waitForWorker()
}

// FlushAfterBatchAck queues a checkpoint flush when sync-on-ack is enabled.
// Returns immediately without calling the Kubernetes API; coalesce by minInterval.
func (s *SyncStore) FlushAfterBatchAck(ctx context.Context) error {
	if !s.syncOnAck {
		return nil
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.dirty = true
	if !s.workerRunning {
		s.workerRunning = true
		go s.flushWorker()
	}
	s.mu.Unlock()
	return nil
}

func (s *SyncStore) signalWake() {
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

func (s *SyncStore) waitForWorker() {
	s.mu.Lock()
	for s.workerRunning {
		s.flushDone.Wait()
	}
	s.mu.Unlock()
}

func (s *SyncStore) doFlush(ctx context.Context) error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	return s.inner.Flush(ctx)
}

func (s *SyncStore) flushWorker() {
	var failed bool
	defer func() {
		s.mu.Lock()
		s.workerRunning = false
		s.flushDone.Broadcast()
		// Cover the race where ack sets dirty after we observed !dirty but
		// before workerRunning is cleared. Do not restart after a flush error
		// (avoids tight loops); the next FlushAfterBatchAck or Sync Flush retries.
		if s.dirty && !s.stopped && !failed {
			s.workerRunning = true
			s.mu.Unlock()
			go s.flushWorker()
			return
		}
		s.mu.Unlock()
	}()

	for {
		s.mu.Lock()
		if s.stopped || !s.dirty {
			s.mu.Unlock()
			return
		}

		wait := time.Duration(0)
		if s.minInterval > 0 && !s.lastFlush.IsZero() {
			elapsed := time.Since(s.lastFlush)
			if elapsed < s.minInterval {
				wait = s.minInterval - elapsed
			}
		}

		if wait > 0 {
			s.mu.Unlock()
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-s.wakeCh:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}
			continue
		}

		s.dirty = false
		handler := s.onFlushErr
		s.mu.Unlock()

		err := s.doFlush(context.Background())

		s.mu.Lock()
		if err == nil {
			s.lastFlush = time.Now()
			s.mu.Unlock()
			continue
		}
		s.dirty = true
		failed = true
		s.mu.Unlock()
		if handler != nil {
			handler(err)
		}
		return
	}
}
