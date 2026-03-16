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
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/dataflow-operator/dataflow/internal/constants"
	"github.com/dataflow-operator/dataflow/internal/metrics"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
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

// connectorLogger provides SetLogger for connectors. Embed in connectors that need logging.
type connectorLogger struct {
	logger logr.Logger
}

// SetLogger sets the logger for the connector.
func (c *connectorLogger) SetLogger(logger logr.Logger) {
	c.logger = logger
}

// connectorMetadata provides SetMetadata for connectors. Embed in connectors that need metrics (namespace, name).
type connectorMetadata struct {
	namespace     string
	name          string
	connectorType string // e.g. "kafka", "postgresql", "trino", "clickhouse", "nessie"
	connectorRole string // "source" or "sink"
}

// SetMetadata sets the metadata for metrics.
func (c *connectorMetadata) SetMetadata(namespace, name string) {
	c.namespace = namespace
	c.name = name
}

// SetConnectorInfo sets the connector type and role for metrics.
func (c *connectorMetadata) SetConnectorInfo(connectorType, role string) {
	c.connectorType = connectorType
	c.connectorRole = role
}

func (c *connectorMetadata) hasMetadata() bool {
	return c.namespace != "" && c.name != ""
}

// RecordError records a connector error metric if metadata is set.
func (c *connectorMetadata) RecordError(operation, errorType string) {
	if c.hasMetadata() {
		metrics.RecordConnectorError(c.namespace, c.name, c.connectorType, c.connectorRole, operation, errorType)
	}
}

// SetConnectionStatus records the connection status metric if metadata is set.
func (c *connectorMetadata) SetConnectionStatus(connected bool) {
	if c.hasMetadata() {
		metrics.SetConnectorConnectionStatus(c.namespace, c.name, c.connectorType, c.connectorRole, connected)
	}
}

// RecordMessageRead records a message read metric if metadata is set.
func (c *connectorMetadata) RecordMessageRead() {
	if c.hasMetadata() {
		metrics.RecordConnectorMessageRead(c.namespace, c.name, c.connectorType, c.connectorRole)
	}
}

// RecordMessageWritten records a message written metric if metadata is set.
func (c *connectorMetadata) RecordMessageWritten(route string) {
	if c.hasMetadata() {
		metrics.RecordConnectorMessageWritten(c.namespace, c.name, c.connectorType, c.connectorRole, route)
	}
}

// rawModeConfig provides the rawMode() method. Embed in sink connectors that support raw mode.
type rawModeConfig struct {
	RawMode *bool
}

func (r *rawModeConfig) rawMode() bool {
	return r.RawMode != nil && *r.RawMode
}

// ParseTableRef splits "schema.table" into schema and table name for information_schema queries.
// If no dot is present, returns "public" as schema and the full string as table name.
func ParseTableRef(table string) (schema, name string) {
	if i := strings.LastIndex(table, "."); i >= 0 {
		return table[:i], table[i+1:]
	}
	return "public", table
}

// quotePostgreSQLIdentifier quotes a PostgreSQL identifier (table, column, index name).
// Required when identifier contains hyphens, spaces, or other special chars.
func quotePostgreSQLIdentifier(id string) string {
	return `"` + strings.ReplaceAll(id, `"`, `""`) + `"`
}

// QuotePostgreSQLTableRef returns a properly quoted schema.table ref for use in SQL.
// E.g. "kafka-to-postgres-raw-events" -> "public"."kafka-to-postgres-raw-events"
func QuotePostgreSQLTableRef(table string) string {
	schema, name := ParseTableRef(table)
	return quotePostgreSQLIdentifier(schema) + "." + quotePostgreSQLIdentifier(name)
}

// runPollingRead creates a message channel and starts a goroutine that calls readFn
// on a ticker, returning the channel. Used by polling-based source connectors.
// bufferSize: channel buffer size; 0 uses DefaultChannelBufferSize.
func runPollingRead(ctx context.Context, pollInterval time.Duration, readFn func(ctx context.Context, ch chan *types.Message), bufferSize int) <-chan *types.Message {
	if bufferSize <= 0 {
		bufferSize = constants.DefaultChannelBufferSize
	}
	msgChan := make(chan *types.Message, bufferSize)
	go func() {
		defer close(msgChan)
		readFn(ctx, msgChan)
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				readFn(ctx, msgChan)
			}
		}
	}()
	return msgChan
}
