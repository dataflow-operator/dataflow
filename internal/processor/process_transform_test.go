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

package processor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/constants"
	"github.com/dataflow-operator/dataflow/internal/transformers"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAttachAckBarrier_WaitsForAllDerived(t *testing.T) {
	var parentAcked atomic.Int32
	parent := types.NewMessage([]byte(`{}`))
	parent.Ack = func() { parentAcked.Add(1) }

	a := types.NewMessage([]byte(`a`))
	b := types.NewMessage([]byte(`b`))
	attachAckBarrier(parent, []*types.Message{a, b})

	a.Ack()
	assert.Equal(t, int32(0), parentAcked.Load())
	b.Ack()
	assert.Equal(t, int32(1), parentAcked.Load())
}

func TestAttachAckBarrier_EmptyDerivedAcksImmediately(t *testing.T) {
	var parentAcked atomic.Int32
	parent := types.NewMessage([]byte(`{}`))
	parent.Ack = func() { parentAcked.Add(1) }

	attachAckBarrier(parent, nil)
	assert.Equal(t, int32(1), parentAcked.Load())
}

func TestTransformWorkers_DefaultAndCap(t *testing.T) {
	p := &Processor{spec: &v1.DataFlowSpec{}}
	assert.Equal(t, constants.DefaultTransformWorkers, p.transformWorkers())

	w := int32(4)
	p.spec.TransformWorkers = &w
	assert.Equal(t, 4, p.transformWorkers())

	w = int32(1000)
	p.spec.TransformWorkers = &w
	assert.Equal(t, constants.MaxTransformWorkers, p.transformWorkers())
}

type delayPassthrough struct {
	delay time.Duration
}

func (d *delayPassthrough) Transform(_ context.Context, message *types.Message) ([]*types.Message, error) {
	if d.delay > 0 {
		time.Sleep(d.delay)
	}
	return []*types.Message{message}, nil
}

var _ transformers.Transformer = (*delayPassthrough)(nil)

func TestProcessMessagesParallel_PreservesOrder(t *testing.T) {
	workers := int32(4)
	p := &Processor{
		spec: &v1.DataFlowSpec{
			Source:           v1.SourceSpec{Type: "kafka"},
			Sink:             v1.SinkSpec{Type: "kafka"},
			TransformWorkers: &workers,
		},
		transformers: []transformers.Transformer{&delayPassthrough{delay: 5 * time.Millisecond}},
		namespace:    "ns",
		name:         "flow",
	}

	const n = 20
	in := make(chan *types.Message, n)
	out := make(chan *types.Message, n)
	for i := 0; i < n; i++ {
		in <- types.NewMessage([]byte{byte(i)})
	}
	close(in)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.processMessages(ctx, in, out)
	}()

	var got []byte
	for msg := range out {
		require.Len(t, msg.Data, 1)
		got = append(got, msg.Data[0])
	}
	wg.Wait()

	want := make([]byte, n)
	for i := 0; i < n; i++ {
		want[i] = byte(i)
	}
	assert.Equal(t, want, got)
}
