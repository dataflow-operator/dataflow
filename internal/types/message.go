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
	"encoding/json"
	"fmt"
	"time"
)

// Message represents a data message flowing through the system
type Message struct {
	// Data is the raw message data (typically JSON)
	Data []byte

	// Metadata contains additional information about the message
	Metadata map[string]interface{}

	// Timestamp when the message was created
	Timestamp time.Time

	// Ack is an optional callback to be invoked after the message has been successfully written to the sink.
	// Used by sources (e.g. Kafka) to commit offset only after sink write, ensuring at-least-once delivery.
	Ack func()
}

// NewMessage creates a new message from data
func NewMessage(data []byte) *Message {
	return &Message{
		Data:      data,
		Metadata:  make(map[string]interface{}),
		Timestamp: time.Now(),
	}
}

// ToJSON converts message data to JSON object
func (m *Message) ToJSON() (map[string]interface{}, error) {
	var result map[string]interface{}
	if err := json.Unmarshal(m.Data, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// FromJSON creates a message from a JSON object
func FromJSON(data map[string]interface{}) (*Message, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return NewMessage(jsonData), nil
}

// MessageID returns a short string identifier for the message for use in logs.
// It checks Metadata for "message_id", "id", or Kafka-style "kafka_partition"/"kafka_offset".
// Returns empty string if no identifier is available.
func MessageID(msg *Message) string {
	if msg == nil || msg.Metadata == nil {
		return ""
	}
	if v, ok := msg.Metadata["message_id"]; ok {
		return fmt.Sprint(v)
	}
	if v, ok := msg.Metadata["id"]; ok {
		return fmt.Sprint(v)
	}
	part, hasPart := msg.Metadata["kafka_partition"]
	off, hasOff := msg.Metadata["kafka_offset"]
	if hasPart && hasOff {
		return fmt.Sprintf("%v/%v", part, off)
	}
	if hasOff {
		return fmt.Sprint(off)
	}
	return ""
}
