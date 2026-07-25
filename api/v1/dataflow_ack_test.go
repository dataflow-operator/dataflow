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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAckGranularityOrDefault(t *testing.T) {
	t.Parallel()

	assert.Equal(t, AckGranularityBatch, AckGranularityOrDefault(nil))
	assert.Equal(t, AckGranularityBatch, AckGranularityOrDefault(&DataFlowSpec{}))
	assert.Equal(t, AckGranularityBatch, AckGranularityOrDefault(&DataFlowSpec{AckGranularity: AckGranularityBatch}))
	assert.Equal(t, AckGranularityMessage, AckGranularityOrDefault(&DataFlowSpec{AckGranularity: AckGranularityMessage}))
}

func TestAckGranularityIsMessage(t *testing.T) {
	t.Parallel()

	assert.False(t, AckGranularityIsMessage(nil))
	assert.False(t, AckGranularityIsMessage(&DataFlowSpec{}))
	assert.True(t, AckGranularityIsMessage(&DataFlowSpec{AckGranularity: AckGranularityMessage}))
}

func TestCollapseBatchOnMessageAckOrDefault(t *testing.T) {
	t.Parallel()

	assert.True(t, CollapseBatchOnMessageAckOrDefault(nil))
	assert.True(t, CollapseBatchOnMessageAckOrDefault(&DataFlowSpec{}))

	f := false
	assert.False(t, CollapseBatchOnMessageAckOrDefault(&DataFlowSpec{CollapseBatchOnMessageAck: &f}))
	tr := true
	assert.True(t, CollapseBatchOnMessageAckOrDefault(&DataFlowSpec{CollapseBatchOnMessageAck: &tr}))
}

func TestShouldCollapseSinkBatch(t *testing.T) {
	t.Parallel()

	assert.False(t, ShouldCollapseSinkBatch(nil))
	assert.False(t, ShouldCollapseSinkBatch(&DataFlowSpec{}))

	f := false
	assert.True(t, ShouldCollapseSinkBatch(&DataFlowSpec{AckGranularity: AckGranularityMessage}))
	assert.False(t, ShouldCollapseSinkBatch(&DataFlowSpec{
		AckGranularity:            AckGranularityMessage,
		CollapseBatchOnMessageAck: &f,
	}))
	assert.False(t, ShouldCollapseSinkBatch(&DataFlowSpec{
		AckGranularity:            AckGranularityBatch,
		CollapseBatchOnMessageAck: &f,
	}))
}
