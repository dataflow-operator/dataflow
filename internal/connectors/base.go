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
)

// buildRawModeJSON wraps value and metadata into JSON: {"value": ..., "_metadata": {...}}
func buildRawModeJSON(value interface{}, metadata map[string]interface{}) ([]byte, error) {
	raw := map[string]interface{}{
		"value":     value,
		"_metadata": metadata,
	}
	return json.Marshal(raw)
}

// baseConnector provides common Connect/Close synchronization for connectors.
// Embed it in source and sink connectors to avoid duplicating mutex and closed-state logic.
//
// Usage in Connect:
//
//	if !c.guardConnect() {
//	    return fmt.Errorf("connector is closed")
//	}
//	defer c.Unlock()
//	// ... connection logic
//
// Usage in Close:
//
//	if c.guardClose() {
//	    return nil // already closed
//	}
//	defer c.Unlock()
//	// ... close underlying connection
type baseConnector struct {
	mu     sync.Mutex
	closed bool
}

// guardConnect acquires the lock and returns false if the connector is already closed.
// If it returns true, the caller holds the lock and must call Unlock() when done (typically via defer).
func (b *baseConnector) guardConnect() bool {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return false
	}
	return true
}

// guardClose acquires the lock and returns true if the connector was already closed (idempotent).
// If it returns false, the caller holds the lock, closed is set to true, and the caller must call Unlock() when done.
func (b *baseConnector) guardClose() bool {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return true
	}
	b.closed = true
	return false
}

// Unlock releases the lock. Call after guardConnect or guardClose when they indicate the caller should proceed.
func (b *baseConnector) Unlock() {
	b.mu.Unlock()
}

// Lock acquires the lock. Use when the connector needs to hold the lock for custom operations (e.g. readRows).
func (b *baseConnector) Lock() {
	b.mu.Lock()
}

// baseConnectorRWMutex provides Connect/Close synchronization with RWMutex for connectors
// that need RLock in read paths (e.g. readRows) to avoid blocking Connect/Close during long queries.
// Use this instead of baseConnector when the connector has concurrent read operations that only
// read conn/closed and should not block Connect/Close.
type baseConnectorRWMutex struct {
	mu     sync.RWMutex
	closed bool
}

// guardConnect acquires the write lock and returns false if the connector is already closed.
func (b *baseConnectorRWMutex) guardConnect() bool {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return false
	}
	return true
}

// guardClose acquires the write lock and returns true if the connector was already closed.
func (b *baseConnectorRWMutex) guardClose() bool {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return true
	}
	b.closed = true
	return false
}

// Unlock releases the write lock.
func (b *baseConnectorRWMutex) Unlock() {
	b.mu.Unlock()
}

// RLock acquires a read lock. Use in read paths (e.g. readRows) that only read conn/closed.
func (b *baseConnectorRWMutex) RLock() {
	b.mu.RLock()
}

// RUnlock releases the read lock.
func (b *baseConnectorRWMutex) RUnlock() {
	b.mu.RUnlock()
}

// Closed returns whether the connector is closed. Must be called while holding at least RLock.
func (b *baseConnectorRWMutex) Closed() bool {
	return b.closed
}
