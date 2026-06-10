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
	"sync/atomic"
	"testing"

	"github.com/dataflow-operator/dataflow/internal/checkpoint"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestAckMessagesAndNotifyProgress(t *testing.T) {
	t.Parallel()

	var progressCalls atomic.Int32
	var ackCalls atomic.Int32

	rec := &progressRecorder{}
	rec.SetProgressCallback(func() {
		progressCalls.Add(1)
	})

	msg := types.NewMessage([]byte(`{}`))
	msg.Ack = func() { ackCalls.Add(1) }

	rec.AckMessagesAndNotifyProgress([]*types.Message{msg})

	assert.Equal(t, int32(1), ackCalls.Load())
	assert.Equal(t, int32(1), progressCalls.Load())
}

func TestAckMessages_NoAckCallback(t *testing.T) {
	t.Parallel()

	var progressCalls atomic.Int32
	rec := &progressRecorder{}
	rec.SetProgressCallback(func() {
		progressCalls.Add(1)
	})

	rec.AckMessagesAndNotifyProgress([]*types.Message{types.NewMessage([]byte(`{}`))})
	assert.Equal(t, int32(1), progressCalls.Load())
}

type mockBatchAckSyncer struct {
	calls atomic.Int32
}

func (m *mockBatchAckSyncer) FlushAfterBatchAck(context.Context) error {
	m.calls.Add(1)
	return nil
}

func TestAckMessagesAndNotifyProgress_FlushesCheckpoint(t *testing.T) {
	t.Parallel()

	syncer := &mockBatchAckSyncer{}
	rec := &progressRecorder{}
	rec.SetCheckpointBatchAckSyncer(syncer)

	msg := types.NewMessage([]byte(`{}`))
	msg.Ack = func() {}

	rec.AckMessagesAndNotifyProgress([]*types.Message{msg})
	assert.Equal(t, int32(1), syncer.calls.Load())
}

func TestAckMessageAndNotifyProgress_FlushesCheckpoint(t *testing.T) {
	t.Parallel()

	syncer := &mockBatchAckSyncer{}
	rec := &progressRecorder{}
	rec.SetCheckpointBatchAckSyncer(syncer)

	msg := types.NewMessage([]byte(`{}`))
	msg.Ack = func() {}

	rec.AckMessageAndNotifyProgress(msg)
	assert.Equal(t, int32(1), syncer.calls.Load())
}

func TestAckMessagesAndNotifyProgress_NoSyncer(t *testing.T) {
	t.Parallel()

	rec := &progressRecorder{}
	msg := types.NewMessage([]byte(`{}`))
	assert.NotPanics(t, func() {
		rec.AckMessagesAndNotifyProgress([]*types.Message{msg})
	})
}

func TestProgressRecorder_SyncerInterface(t *testing.T) {
	t.Parallel()

	var _ checkpoint.BatchAckSyncer = (*mockBatchAckSyncer)(nil)
}
