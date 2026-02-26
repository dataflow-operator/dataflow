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
	"encoding/json"
	"testing"
	"time"

	"github.com/IBM/sarama"
	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaSourceConnector_BuildRawModeMessage_JSONValue(t *testing.T) {
	rawMode := true
	k := NewKafkaSourceConnector(&v1.KafkaSourceSpec{RawMode: &rawMode})

	value := []byte(`{"id": 1, "event": "test"}`)
	msg := &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 2,
		Offset:    100,
		Key:       []byte("key-1"),
		Value:     value,
		Timestamp: time.UnixMilli(1709000000000),
	}

	data, err := k.buildRawModeMessage(value, msg)
	require.NoError(t, err)

	var parsed map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &parsed))

	assert.Contains(t, parsed, "value")
	assert.Contains(t, parsed, "_metadata")

	meta := parsed["_metadata"].(map[string]interface{})
	assert.Equal(t, "test-topic", meta["topic"])
	assert.Equal(t, float64(2), meta["partition"])
	assert.Equal(t, float64(100), meta["offset"])
	assert.Equal(t, float64(1709000000000), meta["timestamp"])
	assert.Equal(t, "key-1", meta["key"])

	val := parsed["value"].(map[string]interface{})
	assert.Equal(t, float64(1), val["id"])
	assert.Equal(t, "test", val["event"])
}

func TestKafkaSourceConnector_BuildRawModeMessage_NonJSONValue(t *testing.T) {
	rawMode := true
	k := NewKafkaSourceConnector(&v1.KafkaSourceSpec{RawMode: &rawMode})

	value := []byte("plain text value")
	msg := &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    0,
		Key:       nil,
		Value:     value,
		Timestamp: time.Time{},
	}

	data, err := k.buildRawModeMessage(value, msg)
	require.NoError(t, err)

	var parsed map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &parsed))

	assert.Equal(t, "plain text value", parsed["value"])
	meta := parsed["_metadata"].(map[string]interface{})
	assert.Equal(t, float64(0), meta["timestamp"])
	assert.Equal(t, "", meta["key"])
}
