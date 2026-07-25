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

package v1

const (
	// AckGranularityBatch commits source offsets after each sink batch flush (default).
	AckGranularityBatch = "batch"
	// AckGranularityMessage commits source offsets after each message is successfully written.
	AckGranularityMessage = "message"
)

// AckGranularityOrDefault returns the configured ack granularity (default batch).
func AckGranularityOrDefault(spec *DataFlowSpec) string {
	if spec != nil && spec.AckGranularity == AckGranularityMessage {
		return AckGranularityMessage
	}
	return AckGranularityBatch
}

// AckGranularityIsMessage reports whether per-message ack is enabled.
func AckGranularityIsMessage(spec *DataFlowSpec) bool {
	return AckGranularityOrDefault(spec) == AckGranularityMessage
}

// CollapseBatchOnMessageAckOrDefault reports whether message-ack must force MaxBatchSize=1.
// Default true preserves legacy coupling; false keeps sink batchSize with per-message source commit.
func CollapseBatchOnMessageAckOrDefault(spec *DataFlowSpec) bool {
	if spec != nil && spec.CollapseBatchOnMessageAck != nil {
		return *spec.CollapseBatchOnMessageAck
	}
	return true
}

// ShouldCollapseSinkBatch reports whether sinks must force MaxBatchSize=1 for the given spec.
func ShouldCollapseSinkBatch(spec *DataFlowSpec) bool {
	return AckGranularityIsMessage(spec) && CollapseBatchOnMessageAckOrDefault(spec)
}
