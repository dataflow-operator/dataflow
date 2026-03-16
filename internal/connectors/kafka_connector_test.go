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
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
)

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
