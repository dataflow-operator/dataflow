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
	"encoding/json"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
)

// benchmarkSourceConnector produces messages for benchmarking.
type benchmarkSourceConnector struct {
	messageCount int
	messageSize  int
}

func (b *benchmarkSourceConnector) Connect(ctx context.Context) error {
	return nil
}

func (b *benchmarkSourceConnector) Read(ctx context.Context) (<-chan *types.Message, error) {
	ch := make(chan *types.Message, 1024)
	msgData := make([]byte, b.messageSize)
	for i := 0; i < b.messageSize; i++ {
		msgData[i] = 'x'
	}

	go func() {
		defer close(ch)
		for i := 0; i < b.messageCount; i++ {
			msg := &types.Message{
				Data:      append([]byte(nil), msgData...),
				Metadata:  make(map[string]interface{}),
				Timestamp: time.Now(),
			}
			select {
			case ch <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

func (b *benchmarkSourceConnector) Close() error {
	return nil
}

// benchmarkSinkConnector consumes messages for benchmarking.
type benchmarkSinkConnector struct {
	mu       sync.Mutex
	received int
}

func (b *benchmarkSinkConnector) Connect(ctx context.Context) error {
	return nil
}

func (b *benchmarkSinkConnector) Write(ctx context.Context, messages <-chan *types.Message) error {
	for msg := range messages {
		b.mu.Lock()
		b.received++
		b.mu.Unlock()
		if msg.Ack != nil {
			msg.Ack()
		}
	}
	return nil
}

func (b *benchmarkSinkConnector) Close() error {
	return nil
}

func newBenchmarkProcessor(messageCount, messageSize int) *Processor {
	source := &benchmarkSourceConnector{messageCount: messageCount, messageSize: messageSize}
	sink := &benchmarkSinkConnector{}
	sourceCfg, _ := json.Marshal(v1.KafkaSourceSpec{
		Brokers:       []string{"localhost:9092"},
		Topic:         "bench-topic",
		ConsumerGroup: "bench-group",
	})
	sinkCfg, _ := json.Marshal(v1.KafkaSinkSpec{
		Brokers: []string{"localhost:9092"},
		Topic:   "bench-out",
	})
	spec := &v1.DataFlowSpec{
		Source: v1.SourceSpec{
			Type:   "kafka",
			Config: &runtime.RawExtension{Raw: sourceCfg},
		},
		Sink: v1.SinkSpec{
			Type:   "kafka",
			Config: &runtime.RawExtension{Raw: sinkCfg},
		},
	}
	return &Processor{
		source:       source,
		sink:         sink,
		errorSink:    nil,
		transformers: nil,
		routerSinks:  make(map[string]v1.SinkSpec),
		logger:       logr.Discard(),
		namespace:    "bench",
		name:         "bench",
		spec:         spec,
	}
}

func BenchmarkProcessor_HighLoad_1K_1KB(b *testing.B) {
	benchmarkProcessorThroughput(b, 1000, 1024)
}

func BenchmarkProcessor_HighLoad_10K_1KB(b *testing.B) {
	benchmarkProcessorThroughput(b, 10000, 1024)
}

func BenchmarkProcessor_HighLoad_100K_256B(b *testing.B) {
	benchmarkProcessorThroughput(b, 100000, 256)
}

func benchmarkProcessorThroughput(b *testing.B, messageCount, messageSize int) {
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proc := newBenchmarkProcessor(messageCount, messageSize)
		runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		err := proc.Start(runCtx)
		cancel()
		if err != nil {
			b.Fatalf("Start failed: %v", err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(messageCount*b.N)/b.Elapsed().Seconds(), "msgs/sec")
}
