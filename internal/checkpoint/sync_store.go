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

// SyncStore wraps a Store with optional immediate flush after sink batch ack.
type SyncStore struct {
	inner       Store
	syncOnAck   bool
	minInterval time.Duration
	mu          sync.Mutex
	lastFlush   time.Time
}

// NewSyncStore wraps inner with sync-on-ack behavior.
// minInterval coalesces sync flushes when syncOnAck is true (0 disables coalescing).
func NewSyncStore(inner Store, syncOnAck bool, minInterval time.Duration) *SyncStore {
	return &SyncStore{
		inner:       inner,
		syncOnAck:   syncOnAck,
		minInterval: minInterval,
	}
}

func (s *SyncStore) Load(ctx context.Context, sourceType string) ([]byte, error) {
	return s.inner.Load(ctx, sourceType)
}

func (s *SyncStore) Save(ctx context.Context, sourceType string, data []byte) error {
	return s.inner.Save(ctx, sourceType, data)
}

func (s *SyncStore) Flush(ctx context.Context) error {
	return s.inner.Flush(ctx)
}

func (s *SyncStore) Clear(ctx context.Context, sourceType string) error {
	return s.inner.Clear(ctx, sourceType)
}

// FlushAfterBatchAck persists pending checkpoint when sync-on-ack is enabled.
func (s *SyncStore) FlushAfterBatchAck(ctx context.Context) error {
	if !s.syncOnAck {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.minInterval > 0 && !s.lastFlush.IsZero() && time.Since(s.lastFlush) < s.minInterval {
		return nil
	}
	err := s.inner.Flush(ctx)
	if err == nil {
		s.lastFlush = time.Now()
	}
	return err
}
