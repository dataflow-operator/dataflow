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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingStore struct {
	NoopStore
	flushCount atomic.Int32
}

func (s *recordingStore) Flush(context.Context) error {
	s.flushCount.Add(1)
	return nil
}

func TestSyncStore_FlushAfterBatchAck_Disabled(t *testing.T) {
	t.Parallel()

	inner := &recordingStore{}
	syncStore := NewSyncStore(inner, false, time.Second)

	err := syncStore.FlushAfterBatchAck(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int32(0), inner.flushCount.Load())
}

func TestSyncStore_FlushAfterBatchAck_Enabled(t *testing.T) {
	t.Parallel()

	inner := &recordingStore{}
	syncStore := NewSyncStore(inner, true, 0)

	err := syncStore.FlushAfterBatchAck(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int32(1), inner.flushCount.Load())
}

func TestSyncStore_FlushAfterBatchAck_Coalesces(t *testing.T) {
	t.Parallel()

	inner := &recordingStore{}
	syncStore := NewSyncStore(inner, true, time.Hour)

	require.NoError(t, syncStore.FlushAfterBatchAck(context.Background()))
	require.NoError(t, syncStore.FlushAfterBatchAck(context.Background()))
	assert.Equal(t, int32(1), inner.flushCount.Load())
}
