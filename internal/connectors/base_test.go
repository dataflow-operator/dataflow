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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildRawModeJSON(t *testing.T) {
	value := map[string]interface{}{"id": 1, "name": "test"}
	metadata := map[string]interface{}{"table": "users", "offset": int64(42)}

	data, err := buildRawModeJSON(value, metadata)
	require.NoError(t, err)

	var parsed map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &parsed))

	assert.Contains(t, parsed, "value")
	assert.Contains(t, parsed, "_metadata")

	meta := parsed["_metadata"].(map[string]interface{})
	assert.Equal(t, "users", meta["table"])
	assert.Equal(t, float64(42), meta["offset"])

	val := parsed["value"].(map[string]interface{})
	assert.Equal(t, float64(1), val["id"])
	assert.Equal(t, "test", val["name"])
}

func TestBaseConnector_GuardConnect_WhenNotClosed(t *testing.T) {
	var b baseConnector
	ok := b.guardConnect()
	require.True(t, ok)
	b.Unlock()
}

func TestBaseConnector_GuardConnect_WhenClosed(t *testing.T) {
	var b baseConnector
	b.closed = true
	ok := b.guardConnect()
	require.False(t, ok)
}

func TestBaseConnector_GuardClose_WhenNotClosed(t *testing.T) {
	var b baseConnector
	alreadyClosed := b.guardClose()
	require.False(t, alreadyClosed)
	assert.True(t, b.closed)
	b.Unlock()
}

func TestBaseConnector_GuardClose_WhenAlreadyClosed(t *testing.T) {
	var b baseConnector
	b.closed = true
	alreadyClosed := b.guardClose()
	require.True(t, alreadyClosed)
}

func TestBaseConnector_LockUnlock(t *testing.T) {
	var b baseConnector
	b.Lock()
	// No deadlock - we hold the lock
	b.Unlock()
}

func TestBaseConnectorRWMutex_GuardConnect_WhenNotClosed(t *testing.T) {
	var b baseConnectorRWMutex
	ok := b.guardConnect()
	require.True(t, ok)
	b.Unlock()
}

func TestBaseConnectorRWMutex_GuardConnect_WhenClosed(t *testing.T) {
	var b baseConnectorRWMutex
	b.closed = true
	ok := b.guardConnect()
	require.False(t, ok)
}

func TestBaseConnectorRWMutex_GuardClose_WhenNotClosed(t *testing.T) {
	var b baseConnectorRWMutex
	alreadyClosed := b.guardClose()
	require.False(t, alreadyClosed)
	assert.True(t, b.closed)
	b.Unlock()
}

func TestBaseConnectorRWMutex_RLock_Closed(t *testing.T) {
	var b baseConnectorRWMutex
	b.RLock()
	assert.False(t, b.Closed())
	b.RUnlock()

	// Set closed under write lock
	b.guardClose()
	b.Unlock()

	b.RLock()
	assert.True(t, b.Closed())
	b.RUnlock()
}

func TestBaseConnectorRWMutex_ConcurrentRLock(t *testing.T) {
	var b baseConnectorRWMutex
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.RLock()
			_ = b.Closed()
			b.RUnlock()
		}()
	}
	wg.Wait()
}
