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

package types

import (
	"testing"
)

func TestMessageID(t *testing.T) {
	tests := []struct {
		name     string
		msg      *Message
		expected string
	}{
		{
			name:     "nil message",
			msg:      nil,
			expected: "",
		},
		{
			name:     "nil metadata",
			msg:      &Message{Data: []byte("{}")},
			expected: "",
		},
		{
			name: "empty metadata",
			msg: &Message{
				Data:     []byte("{}"),
				Metadata: make(map[string]interface{}),
			},
			expected: "",
		},
		{
			name: "message_id present",
			msg: &Message{
				Data: []byte("{}"),
				Metadata: map[string]interface{}{
					"message_id": "abc-123",
				},
			},
			expected: "abc-123",
		},
		{
			name: "id present",
			msg: &Message{
				Data: []byte("{}"),
				Metadata: map[string]interface{}{
					"id": "xyz-456",
				},
			},
			expected: "xyz-456",
		},
		{
			name: "message_id takes precedence over id",
			msg: &Message{
				Data: []byte("{}"),
				Metadata: map[string]interface{}{
					"message_id": "first",
					"id":         "second",
				},
			},
			expected: "first",
		},
		{
			name: "kafka partition and offset",
			msg: &Message{
				Data: []byte("{}"),
				Metadata: map[string]interface{}{
					"kafka_partition": 2,
					"kafka_offset":    100,
				},
			},
			expected: "2/100",
		},
		{
			name: "kafka_offset only",
			msg: &Message{
				Data: []byte("{}"),
				Metadata: map[string]interface{}{
					"kafka_offset": 42,
				},
			},
			expected: "42",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MessageID(tt.msg)
			if got != tt.expected {
				t.Errorf("MessageID() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestMessage_AckCalledAfterSinkWrite(t *testing.T) {
	// Ack is used by sources (e.g. Kafka) to commit offset only after the message is written to the sink.
	// This test verifies that Message can carry an Ack callback and it can be invoked.
	acked := 0
	msg := NewMessage([]byte(`{"id":1}`))
	msg.Ack = func() { acked++ }

	if acked != 0 {
		t.Fatalf("Ack should not be called yet, got acked=%d", acked)
	}
	msg.Ack()
	if acked != 1 {
		t.Errorf("Ack() should have been called once, got acked=%d", acked)
	}
	msg.Ack()
	msg.Ack()
	if acked != 3 {
		t.Errorf("Ack() can be called multiple times, got acked=%d", acked)
	}
}
