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
	"testing"
	"time"

	"github.com/IBM/sarama"
	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
)

// testConsumerGroup is a minimal sarama.ConsumerGroup for Read tests (Consume is overridden via testConsumeFunc).
type testConsumerGroup struct {
	errorsCh <-chan error
}

func (testConsumerGroup) Consume(context.Context, []string, sarama.ConsumerGroupHandler) error {
	panic("testConsumerGroup.Consume must be replaced by KafkaSourceConnector.testConsumeFunc in tests")
}

func (t testConsumerGroup) Errors() <-chan error    { return t.errorsCh }
func (testConsumerGroup) Pause(map[string][]int32)  {}
func (testConsumerGroup) Resume(map[string][]int32) {}
func (testConsumerGroup) PauseAll()                 {}
func (testConsumerGroup) ResumeAll()                {}
func (testConsumerGroup) Close() error              { return nil }

func TestKafkaRead_ConsumeFatalErrorBeforeSetup_ReturnsError(t *testing.T) {
	closedErrCh := make(chan error)
	close(closedErrCh)

	k := NewKafkaSourceConnector(&v1.KafkaSourceSpec{Topic: "t", Brokers: []string{"localhost:9092"}})
	k.consumer = testConsumerGroup{errorsCh: closedErrCh}
	k.testConsumeFunc = func(context.Context, []string, sarama.ConsumerGroupHandler) error {
		return errors.New("unknown topic or partition")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	readDone := make(chan error, 1)
	go func() {
		_, err := k.Read(ctx)
		readDone <- err
	}()

	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("expected Read error for fatal Consume failure before Setup")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Read blocked; expected quick return on fatal Consume error before Setup")
	}
}

func TestKafkaRead_AuthorizationErrorRetriesThenReady(t *testing.T) {
	closedErrCh := make(chan error)
	close(closedErrCh)

	k := NewKafkaSourceConnector(&v1.KafkaSourceSpec{Topic: "t", Brokers: []string{"localhost:9092"}})
	k.consumer = testConsumerGroup{errorsCh: closedErrCh}
	k.consumeRetryDelay = 5 * time.Millisecond

	var calls int
	k.testConsumeFunc = func(cctx context.Context, topics []string, h sarama.ConsumerGroupHandler) error {
		calls++
		if calls == 1 {
			return fmt.Errorf("broker: %w", sarama.ErrTopicAuthorizationFailed)
		}
		hh, ok := h.(*kafkaConsumerGroupHandler)
		if !ok {
			return fmt.Errorf("unexpected handler type %T", h)
		}
		if err := hh.Setup(&mockConsumerGroupSession{ctx: cctx}); err != nil {
			return err
		}
		<-cctx.Done()
		return context.Canceled
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readDone := make(chan struct {
		ch  <-chan *types.Message
		err error
	}, 1)
	go func() {
		msgCh, err := k.Read(ctx)
		readDone <- struct {
			ch  <-chan *types.Message
			err error
		}{ch: msgCh, err: err}
	}()

	var msgCh <-chan *types.Message
	select {
	case res := <-readDone:
		if res.err != nil {
			t.Fatalf("Read: %v", res.err)
		}
		msgCh = res.ch
	case <-time.After(5 * time.Second):
		t.Fatal("Read blocked; expected success after authorization retry and Setup")
	}

	if msgCh == nil {
		t.Fatal("expected non-nil message channel")
	}
	if calls != 2 {
		t.Fatalf("Consume calls = %d, want 2 (one auth failure, one success)", calls)
	}

	cancel()
}

func TestIsCoordinatorUnavailableError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"coordinator is not available", errors.New("kafka server: The coordinator is not available"), true},
		{"CoordinatorNotAvailable", errors.New("CoordinatorNotAvailable"), true},
		{"wrapped", errors.New("error from consumer: kafka server: The coordinator is not available"), true},
		{"other error", errors.New("connection refused"), false},
		{"other kafka", errors.New("kafka server: Topic not found"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isCoordinatorUnavailableError(tt.err)
			if got != tt.want {
				t.Errorf("isCoordinatorUnavailableError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsKafkaAuthorizationError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"KError topic", sarama.ErrTopicAuthorizationFailed, true},
		{"KError group", sarama.ErrGroupAuthorizationFailed, true},
		{"KError cluster", sarama.ErrClusterAuthorizationFailed, true},
		{"wrapped KError", fmt.Errorf("upstream: %w", sarama.ErrTopicAuthorizationFailed), true},
		{"not authorized string", errors.New("not authorized to access this topic"), true},
		{"TOPIC_AUTHORIZATION_FAILED text", errors.New("TOPIC_AUTHORIZATION_FAILED"), true},
		{"other", errors.New("connection refused"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isKafkaAuthorizationError(tt.err); got != tt.want {
				t.Errorf("isKafkaAuthorizationError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsKafkaConsumeRetriableError(t *testing.T) {
	authWrapped := fmt.Errorf("wrap: %w", sarama.ErrGroupAuthorizationFailed)
	if !isKafkaConsumeRetriableError(authWrapped) {
		t.Error("authorization error should be retriable")
	}
	if !isKafkaConsumeRetriableError(errors.New("kafka server: The coordinator is not available")) {
		t.Error("coordinator error should be retriable")
	}
	if isKafkaConsumeRetriableError(errors.New("some permanent failure")) {
		t.Error("non-retriable error should be false")
	}
}

// mockConsumerGroupSession implements sarama.ConsumerGroupSession for testing.
type mockConsumerGroupSession struct {
	ctx context.Context
}

func (m *mockConsumerGroupSession) Claims() map[string][]int32                  { return nil }
func (m *mockConsumerGroupSession) MemberID() string                            { return "test" }
func (m *mockConsumerGroupSession) GenerationID() int32                         { return 1 }
func (m *mockConsumerGroupSession) MarkOffset(string, int32, int64, string)     {}
func (m *mockConsumerGroupSession) Commit()                                     {}
func (m *mockConsumerGroupSession) ResetOffset(string, int32, int64, string)    {}
func (m *mockConsumerGroupSession) MarkMessage(*sarama.ConsumerMessage, string) {}
func (m *mockConsumerGroupSession) Context() context.Context                    { return m.ctx }

// mockConsumerGroupClaim implements sarama.ConsumerGroupClaim for testing.
type mockConsumerGroupClaim struct {
	ch chan *sarama.ConsumerMessage
}

func (m *mockConsumerGroupClaim) Topic() string                            { return "test-topic" }
func (m *mockConsumerGroupClaim) Partition() int32                         { return 0 }
func (m *mockConsumerGroupClaim) InitialOffset() int64                     { return 0 }
func (m *mockConsumerGroupClaim) HighWaterMarkOffset() int64               { return 0 }
func (m *mockConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage { return m.ch }

func TestConsumeClaim_SetsTimestampMetadata(t *testing.T) {
	kafkaTimestamp := time.Date(2024, 2, 27, 10, 13, 20, 0, time.UTC)

	claim := &mockConsumerGroupClaim{
		ch: make(chan *sarama.ConsumerMessage, 1),
	}
	claim.ch <- &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 3,
		Offset:    42,
		Key:       []byte("key-1"),
		Value:     []byte(`{"id":1}`),
		Timestamp: kafkaTimestamp,
	}
	close(claim.ch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &mockConsumerGroupSession{ctx: ctx}

	msgChan := make(chan *types.Message, 1)
	handler := &kafkaConsumerGroupHandler{
		connector: &KafkaSourceConnector{
			config:          &v1.KafkaSourceSpec{},
			connectorLogger: connectorLogger{logger: logr.Discard()},
		},
		msgChan:   msgChan,
		ready:     make(chan bool),
		readyOnce: sync.Once{},
	}

	err := handler.ConsumeClaim(session, claim)
	if err != nil {
		t.Fatalf("ConsumeClaim returned error: %v", err)
	}

	select {
	case msg := <-msgChan:
		ts, ok := msg.Metadata["timestamp"].(string)
		if !ok {
			t.Fatal("timestamp not found in metadata or not a string")
		}
		want := "2024-02-27T10:13:20.000Z"
		if ts != want {
			t.Errorf("timestamp = %q, want %q", ts, want)
		}

		if topic, _ := msg.Metadata["topic"].(string); topic != "test-topic" {
			t.Errorf("topic = %q, want %q", topic, "test-topic")
		}
		if partition, _ := msg.Metadata["partition"].(int32); partition != 3 {
			t.Errorf("partition = %v, want 3", partition)
		}
		if offset, _ := msg.Metadata["offset"].(int64); offset != 42 {
			t.Errorf("offset = %v, want 42", offset)
		}
		if key, _ := msg.Metadata["key"].(string); key != "key-1" {
			t.Errorf("key = %q, want %q", key, "key-1")
		}
	default:
		t.Fatal("no message received from msgChan")
	}
}

func TestConsumeClaim_TimestampFormatRFC3339Milli(t *testing.T) {
	tests := []struct {
		name      string
		timestamp time.Time
		want      string
	}{
		{
			name:      "zero millis",
			timestamp: time.Date(2024, 1, 15, 8, 0, 0, 0, time.UTC),
			want:      "2024-01-15T08:00:00.000Z",
		},
		{
			name:      "with millis",
			timestamp: time.Date(2024, 6, 1, 12, 30, 45, 123000000, time.UTC),
			want:      "2024-06-01T12:30:45.123Z",
		},
		{
			name:      "non-UTC converted to UTC",
			timestamp: time.Date(2024, 3, 10, 15, 0, 0, 0, time.FixedZone("EST", -5*3600)),
			want:      "2024-03-10T20:00:00.000Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			claim := &mockConsumerGroupClaim{
				ch: make(chan *sarama.ConsumerMessage, 1),
			}
			claim.ch <- &sarama.ConsumerMessage{
				Value:     []byte(`{}`),
				Timestamp: tt.timestamp,
			}
			close(claim.ch)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			session := &mockConsumerGroupSession{ctx: ctx}

			msgChan := make(chan *types.Message, 1)
			handler := &kafkaConsumerGroupHandler{
				connector: &KafkaSourceConnector{
					config:          &v1.KafkaSourceSpec{},
					connectorLogger: connectorLogger{logger: logr.Discard()},
				},
				msgChan:   msgChan,
				ready:     make(chan bool),
				readyOnce: sync.Once{},
			}

			if err := handler.ConsumeClaim(session, claim); err != nil {
				t.Fatalf("ConsumeClaim returned error: %v", err)
			}

			msg := <-msgChan
			ts, ok := msg.Metadata["timestamp"].(string)
			if !ok {
				t.Fatal("timestamp not found in metadata or not a string")
			}
			if ts != tt.want {
				t.Errorf("timestamp = %q, want %q", ts, tt.want)
			}
		})
	}
}
