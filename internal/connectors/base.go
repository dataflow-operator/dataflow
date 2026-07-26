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
	"errors"
	"strings"
	"sync"
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/checkpoint"
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

// progressRecorder is an optional callback invoked after successful pipeline progress (e.g. sink flush + ack).
type progressRecorder struct {
	onProgress                func()
	batchAckSyncer            checkpoint.BatchAckSyncer
	ackGranularity            string
	collapseBatchOnMessageAck *bool // nil → true (legacy) when message-ack
	checkpointReporter        checkpointSaveReporter
}

func (p *progressRecorder) setReporter(r checkpointSaveReporter) {
	p.checkpointReporter = r
}

// SetProgressCallback registers a callback for liveness/progress probes.
func (p *progressRecorder) SetProgressCallback(fn func()) {
	p.onProgress = fn
}

// SetCheckpointBatchAckSyncer registers a callback to flush checkpoint after sink batch ack.
func (p *progressRecorder) SetCheckpointBatchAckSyncer(syncer checkpoint.BatchAckSyncer) {
	p.batchAckSyncer = syncer
}

// SetAckGranularity configures per-batch or per-message source offset commits.
func (p *progressRecorder) SetAckGranularity(granularity string) {
	if granularity == v1.AckGranularityMessage {
		p.ackGranularity = v1.AckGranularityMessage
		return
	}
	p.ackGranularity = v1.AckGranularityBatch
}

// SetCollapseBatchOnMessageAck controls whether message-ack forces MaxBatchSize=1.
// Wire from CRD via processor (default true when unset on the spec).
func (p *progressRecorder) SetCollapseBatchOnMessageAck(collapse bool) {
	p.collapseBatchOnMessageAck = &collapse
}

func (p *progressRecorder) ackGranularityIsMessage() bool {
	return p.ackGranularity == v1.AckGranularityMessage
}

// shouldCollapseBatchForAck reports whether sink batching must be forced to 1.
func (p *progressRecorder) shouldCollapseBatchForAck() bool {
	if !p.ackGranularityIsMessage() {
		return false
	}
	if p.collapseBatchOnMessageAck != nil {
		return *p.collapseBatchOnMessageAck
	}
	return true
}

func (p *progressRecorder) notifyProgress() {
	if p.onProgress != nil {
		p.onProgress()
	}
}

// AckMessages calls Ack on each message (commits source offsets when configured).
func AckMessages(msgs []*types.Message) {
	for _, m := range msgs {
		if m.Ack != nil {
			m.Ack()
		}
	}
}

// AckMessageAndNotifyProgress commits one message and updates liveness progress after a successful write.
func (p *progressRecorder) AckMessageAndNotifyProgress(msg *types.Message) {
	if msg != nil && msg.Ack != nil {
		msg.Ack()
	}
	p.flushCheckpointAfterBatchAck()
	p.notifyProgress()
}

// AckMessagesAndNotifyProgress commits offsets and updates liveness progress after a successful batch.
func (p *progressRecorder) AckMessagesAndNotifyProgress(msgs []*types.Message) {
	AckMessages(msgs)
	p.flushCheckpointAfterBatchAck()
	p.notifyProgress()
}

// AckAfterSuccessfulWrite commits source offsets using the configured ack granularity.
func (p *progressRecorder) AckAfterSuccessfulWrite(msgs []*types.Message) {
	if len(msgs) == 0 {
		return
	}
	if p.ackGranularityIsMessage() {
		for _, m := range msgs {
			p.AckMessageAndNotifyProgress(m)
		}
		return
	}
	p.AckMessagesAndNotifyProgress(msgs)
}

func (p *progressRecorder) flushCheckpointAfterBatchAck() {
	if p.batchAckSyncer != nil {
		err := p.batchAckSyncer.FlushAfterBatchAck(context.Background())
		p.checkpointReporter.report(err, checkpointOpFlush)
	}
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

// SetSourcePollHealthy records the last polling read outcome for source connectors.
func (c *connectorMetadata) SetSourcePollHealthy(healthy bool) {
	if c.hasMetadata() {
		metrics.SetConnectorSourcePollHealthy(c.namespace, c.name, c.connectorType, c.connectorRole, healthy)
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

// SetChannelFillRatio records buffered channel occupancy if metadata is set.
func (c *connectorMetadata) SetChannelFillRatio(channel string, fillRatio float64) {
	if c.hasMetadata() {
		metrics.SetChannelFillRatio(c.namespace, c.name, channel, fillRatio)
	}
}

// rawModeConfig provides rawMode and flatten metadata helpers. Embed in sink connectors that support raw mode.
type rawModeConfig struct {
	RawMode                      *bool
	FlattenMetadataColumns       *bool
	FlattenMetadataColumnsPrefix string
}

func (r *rawModeConfig) rawMode() bool {
	return r.RawMode != nil && *r.RawMode
}

func (r *rawModeConfig) flattenMetadataColumns() bool {
	return r.FlattenMetadataColumns != nil && *r.FlattenMetadataColumns
}

func (r *rawModeConfig) flattenMetadataPrefix() string {
	return r.FlattenMetadataColumnsPrefix
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

const maxPollingReadBackoff = 5 * time.Minute

// ErrSourceExhausted signals that source has no more data for this run.
// runPollingRead treats this as successful terminal state and closes output channel.
var ErrSourceExhausted = errors.New("source exhausted")

// pollFailureWait returns how long to wait before the next poll after repeated failures.
// consecutiveFailures counts failures in the current streak (>=1).
func pollFailureWait(base time.Duration, consecutiveFailures int) time.Duration {
	if consecutiveFailures <= 1 {
		return base
	}
	shift := consecutiveFailures - 1
	if shift > 12 {
		shift = 12
	}
	d := base * (1 << uint(shift))
	if d > maxPollingReadBackoff {
		return maxPollingReadBackoff
	}
	return d
}

func shouldLogPollFailure(now, lastLog time.Time, consecutiveFailures int, pollInterval time.Duration) bool {
	if consecutiveFailures == 1 {
		return true
	}
	if consecutiveFailures%10 == 0 {
		return true
	}
	minGap := 30 * time.Second
	if g := 5 * pollInterval; g > minGap {
		minGap = g
	}
	return now.Sub(lastLog) >= minGap
}

// pollingReadOpts configures logging and metrics for runPollingRead. All fields are optional.
type pollingReadOpts struct {
	logger logr.Logger
	meta   *connectorMetadata
}

// runPollingRead creates a message channel and starts a goroutine that calls readFn
// on a schedule, returning the channel. Used by polling-based source connectors.
// readFn returns nil on success; a non-nil error triggers exponential backoff between attempts
// (capped) and throttled error logging when opts is set.
// If readFn returns ErrSourceExhausted, polling stops and output channel is closed.
// bufferSize: channel buffer size; 0 uses DefaultChannelBufferSize.
func runPollingRead(ctx context.Context, pollInterval time.Duration, readFn func(ctx context.Context, ch chan *types.Message) error, bufferSize int, opts *pollingReadOpts) <-chan *types.Message {
	if bufferSize <= 0 {
		bufferSize = constants.DefaultChannelBufferSize
	}
	msgChan := make(chan *types.Message, bufferSize)
	go func() {
		defer close(msgChan)
		var consecutiveFailures int
		var lastFailLog time.Time
		first := true
		for {
			if !first {
				wait := pollInterval
				if consecutiveFailures > 0 {
					wait = pollFailureWait(pollInterval, consecutiveFailures)
				}
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
			first = false

			err := readFn(ctx, msgChan)
			if err != nil {
				if errors.Is(err, ErrSourceExhausted) {
					if opts != nil {
						opts.logger.V(1).Info("Source poll exhausted, stopping read loop")
					}
					if opts != nil && opts.meta != nil {
						opts.meta.SetSourcePollHealthy(true)
					}
					return
				}
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				consecutiveFailures++
				if opts != nil && opts.meta != nil {
					opts.meta.SetSourcePollHealthy(false)
				}
				if opts != nil {
					now := time.Now()
					if shouldLogPollFailure(now, lastFailLog, consecutiveFailures, pollInterval) {
						opts.logger.Error(err, "source poll read failed",
							"consecutiveFailures", consecutiveFailures,
							"nextWait", pollFailureWait(pollInterval, consecutiveFailures))
						lastFailLog = now
					}
				}
				continue
			}
			consecutiveFailures = 0
			if opts != nil && opts.meta != nil {
				opts.meta.SetSourcePollHealthy(true)
			}
		}
	}()
	return msgChan
}
