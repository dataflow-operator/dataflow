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

package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/checkpoint"
	"github.com/dataflow-operator/dataflow/internal/connectors"
	"github.com/dataflow-operator/dataflow/internal/constants"
	errclass "github.com/dataflow-operator/dataflow/internal/errors"
	"github.com/dataflow-operator/dataflow/internal/logkeys"
	"github.com/dataflow-operator/dataflow/internal/metrics"
	"github.com/dataflow-operator/dataflow/internal/retry"
	"github.com/dataflow-operator/dataflow/internal/transformers"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
)

// Processor orchestrates data flow from source through transformations to sink
type Processor struct {
	source          connectors.SourceConnector
	sink            connectors.SinkConnector
	errorSink       connectors.SinkConnector
	transformers    []transformers.Transformer
	routerSinks     map[string]v1.SinkSpec
	processedCount  int64
	errorCount      int64
	logger          logr.Logger
	namespace       string
	name            string
	spec            *v1.DataFlowSpec
	checkpointStore checkpoint.Store // for graceful shutdown flush
	// ready is set to true after source.Read succeeds (pipeline is consuming).
	ready atomic.Bool
	// lastProgressUnixNano is updated when the pipeline makes forward progress (e.g. sink flush + ack).
	lastProgressUnixNano atomic.Int64
}

// NewProcessor creates a new processor
func NewProcessor(spec *v1.DataFlowSpec) (*Processor, error) {
	return NewProcessorWithLoggerAndMetadata(spec, logr.Discard(), "", "")
}

// NewProcessorWithLogger creates a new processor with logger
func NewProcessorWithLogger(spec *v1.DataFlowSpec, logger logr.Logger) (*Processor, error) {
	return NewProcessorWithLoggerAndMetadata(spec, logger, "", "")
}

// NewProcessorWithLoggerAndMetadata creates a new processor with logger and metadata
func NewProcessorWithLoggerAndMetadata(spec *v1.DataFlowSpec, logger logr.Logger, namespace, name string) (*Processor, error) {
	return NewProcessorWithOptions(spec, logger, namespace, name)
}

// NewProcessorWithOptions creates a new processor with optional checkpoint store
func NewProcessorWithOptions(spec *v1.DataFlowSpec, logger logr.Logger, namespace, name string, opts ...ProcessorOption) (*Processor, error) {
	options := &ProcessorOptions{}
	for _, opt := range opts {
		opt(options)
	}

	ctx := context.Background()
	sourceOpts := buildSourceConnectorOptions(ctx, spec.Source.Type, options.CheckpointStore, spec.ChannelBufferSize)

	// Create source connector
	source, err := connectors.CreateSourceConnector(&spec.Source, sourceOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create source connector: %w", err)
	}

	// Create sink connector
	sink, err := connectors.CreateSinkConnector(&spec.Sink)
	if err != nil {
		return nil, fmt.Errorf("failed to create sink connector: %w", err)
	}

	// Create error sink connector if specified
	var errorSink connectors.SinkConnector
	if spec.Errors != nil {
		errorSink, err = connectors.CreateSinkConnector(&spec.Errors.SinkSpec)
		if err != nil {
			return nil, fmt.Errorf("failed to create error sink connector: %w", err)
		}
	}

	// Create transformers
	transformerList := make([]transformers.Transformer, 0, len(spec.Transformations))
	routerSinks := make(map[string]v1.SinkSpec)

	for _, t := range spec.Transformations {
		transformer, err := transformers.CreateTransformer(&t)
		if err != nil {
			return nil, fmt.Errorf("failed to create transformer %s: %w", t.Type, err)
		}

		// Check if this is a router transformer
		if t.Type == "router" {
			routerCfg, err := t.GetRouterConfig()
			if err == nil && routerCfg != nil {
				for _, route := range routerCfg.Routes {
					routerSinks[route.Condition] = route.Sink
				}
			}
		}

		transformerList = append(transformerList, transformer)
	}

	p := &Processor{
		source:       source,
		sink:         sink,
		errorSink:    errorSink,
		transformers: transformerList,
		routerSinks:  routerSinks,
		logger:       logger,
		namespace:    namespace,
		name:         name,
		spec:         spec,
	}
	if options.CheckpointStore != nil {
		p.checkpointStore = options.CheckpointStore
	}
	p.initConnector(source, logger.WithValues(logkeys.ConnectorType, spec.Source.Type+"-source"))
	p.initConnector(sink, logger.WithValues(logkeys.ConnectorType, spec.Sink.Type+"-sink"))
	if errorSink != nil {
		p.initConnector(errorSink, logger.WithValues(logkeys.ConnectorType, spec.Errors.Type+"-sink"))
	}
	for _, transformer := range transformerList {
		p.initConnector(transformer, logger)
	}
	return p, nil
}

// Ready reports whether the processor has completed startup through source.Read
// (connect + read loop running). Used by HTTP /readyz for Kubernetes probes.
func (p *Processor) Ready() bool {
	return p.ready.Load()
}

// RecordProgress updates the last-progress timestamp for liveness probes.
func (p *Processor) RecordProgress() {
	p.lastProgressUnixNano.Store(time.Now().UnixNano())
}

// ProgressStale reports whether no progress was recorded within maxAge.
// When maxAge <= 0, progress checking is disabled and this always returns false.
func (p *Processor) ProgressStale(maxAge time.Duration) bool {
	if maxAge <= 0 {
		return false
	}
	ts := p.lastProgressUnixNano.Load()
	if ts == 0 {
		return false
	}
	return time.Since(time.Unix(0, ts)) > maxAge
}

// FlushCheckpoint persists any pending checkpoint to storage. Call before shutdown.
func (p *Processor) FlushCheckpoint(ctx context.Context) error {
	if p.checkpointStore != nil {
		return p.checkpointStore.Flush(ctx)
	}
	return nil
}

// Start starts processing messages
func (p *Processor) Start(ctx context.Context) error {
	p.logger.Info("Starting processor")

	// Connect to source with retry on transient errors (connection refused, etc.)
	if err := connectWithRetry(ctx, p.source, "source", 0, 30*time.Second, p.logger); err != nil {
		p.logger.Error(err, "Failed to connect to source")
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	defer p.source.Close()

	// Connect to main sink with retry on transient errors (connection refused, HTTP 500, etc.)
	if err := connectWithRetry(ctx, p.sink, "sink", 0, 30*time.Second, p.logger); err != nil {
		p.logger.Error(err, "Failed to connect to sink")
		return fmt.Errorf("failed to connect to sink: %w", err)
	}
	defer p.sink.Close()

	// Connect to error sink if specified (with retry on transient errors)
	if p.errorSink != nil {
		if err := connectWithRetry(ctx, p.errorSink, "error-sink", 0, 30*time.Second, p.logger); err != nil {
			p.logger.Error(err, "Failed to connect to error sink")
			return fmt.Errorf("failed to connect to error sink: %w", err)
		}
		defer p.errorSink.Close()
	}

	// Router sinks will be connected dynamically when needed

	// Read messages from source
	msgChan, err := p.source.Read(ctx)
	if err != nil {
		p.logger.Error(err, "Failed to read from source")
		return fmt.Errorf("failed to read from source: %w", err)
	}
	p.ready.Store(true)
	p.RecordProgress()
	p.logger.Info("Started reading from source")

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	fatalSourceErr := make(chan error, constants.DefaultSingleValueChannelBufferSize)
	if re, ok := p.source.(connectors.SourceReadErrors); ok {
		if readErrCh := re.ReadErrors(); readErrCh != nil {
			go func() {
				select {
				case <-runCtx.Done():
				case err, ok := <-readErrCh:
					if ok && err != nil {
						p.logger.Error(err, "Fatal error from source read")
						select {
						case fatalSourceErr <- err:
						default:
						}
						runCancel()
					}
				}
			}()
		}
	}

	// Process messages
	processedChan := make(chan *types.Message, p.channelBufferSize())
	go p.processMessages(runCtx, msgChan, processedChan)

	// Write messages to sink(s)
	p.logger.Info("Starting to write messages to sink")
	writeErr := p.writeMessages(runCtx, processedChan)

	select {
	case err := <-fatalSourceErr:
		return fmt.Errorf("source read error: %w", err)
	default:
	}

	return writeErr
}

// Connectable represents any connector that can establish a connection.
type Connectable interface {
	Connect(ctx context.Context) error
}

// connectWithRetry connects to a connector, retrying on transient errors until success,
// maxRetries exhausted, or context cancellation. Pass maxRetries <= 0 for unlimited retries.
func connectWithRetry(ctx context.Context, connector Connectable, connectorName string, maxRetries int, initialBackoff time.Duration, logger logr.Logger) error {
	const maxBackoff = 5 * time.Minute
	backoff := initialBackoff
	attempt := 0
	connectStart := time.Now()
	for {
		err := connector.Connect(ctx)
		if err == nil {
			fields := []any{"connector", connectorName, logkeys.DurationMS, time.Since(connectStart).Milliseconds()}
			if attempt > 0 {
				fields = append(fields, logkeys.Attempt, attempt+1)
			}
			logger.Info("Connected to connector", fields...)
			return nil
		}
		if !retry.IsRetryableForConnect(err) {
			return err
		}
		attempt++
		if maxRetries > 0 && attempt >= maxRetries {
			return fmt.Errorf("connector %s: max retries (%d) exceeded: %w", connectorName, maxRetries, err)
		}
		logger.Info("Transient connection error, retrying later",
			"connector", connectorName,
			logkeys.Attempt, attempt,
			"error", err.Error(),
			"backoff", backoff.String())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			if backoff < maxBackoff {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
		}
	}
}

// channelBufferSize returns the configured channel buffer size for the processor (default: constants.DefaultChannelBufferSize).
func (p *Processor) channelBufferSize() int {
	if p.spec != nil && p.spec.ChannelBufferSize != nil && *p.spec.ChannelBufferSize > 0 {
		return int(*p.spec.ChannelBufferSize)
	}
	return constants.DefaultChannelBufferSize
}

// initConnector sets logger, metadata, progress, and checkpoint sync on a connector.
func (p *Processor) initConnector(connector interface{}, logger logr.Logger) {
	if lc, ok := connector.(interface{ SetLogger(logr.Logger) }); ok {
		lc.SetLogger(logger)
	}
	if mc, ok := connector.(interface{ SetMetadata(string, string) }); ok {
		mc.SetMetadata(p.namespace, p.name)
	}
	connectors.WireCheckpointSaveReporting(connector, logger)
	if pc, ok := connector.(interface{ SetProgressCallback(func()) }); ok {
		pc.SetProgressCallback(p.RecordProgress)
	}
	if syncer, ok := p.checkpointStore.(checkpoint.BatchAckSyncer); ok {
		if sc, ok := connector.(interface {
			SetCheckpointBatchAckSyncer(checkpoint.BatchAckSyncer)
		}); ok {
			sc.SetCheckpointBatchAckSyncer(syncer)
		}
	}
	if ag, ok := connector.(interface{ SetAckGranularity(string) }); ok {
		ag.SetAckGranularity(v1.AckGranularityOrDefault(p.spec))
	}
	if cb, ok := connector.(interface{ SetCollapseBatchOnMessageAck(bool) }); ok {
		cb.SetCollapseBatchOnMessageAck(v1.CollapseBatchOnMessageAckOrDefault(p.spec))
	}
}

// writeMessages writes messages to appropriate sink(s)
func (p *Processor) writeMessages(ctx context.Context, messages <-chan *types.Message) error {
	// Check if we have router sinks
	if len(p.routerSinks) > 0 {
		// Route messages to different sinks
		routerChans := make(map[string]chan *types.Message)
		for condition := range p.routerSinks {
			routerChans[condition] = make(chan *types.Message, p.channelBufferSize())
		}
		defaultChan := make(chan *types.Message, p.channelBufferSize())

		// Route messages
		go func() {
			defer func() {
				for _, ch := range routerChans {
					close(ch)
				}
				close(defaultChan)
			}()

			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-messages:
					if !ok {
						return
					}

					// Track queue size before routing
					queueWaitStart := time.Now()
					metrics.SetTaskQueueSize(p.namespace, p.name, "routing", len(messages))
					if cap(messages) > 0 {
						metrics.SetChannelFillRatio(p.namespace, p.name, "routing", float64(len(messages))/float64(cap(messages)))
					}

					// Check if message has routing metadata
					if routedCondition, ok := msg.Metadata["routed_condition"].(string); ok {
						if log := p.logger.V(1); log.Enabled() {
							log.Info("Message has routing condition", "condition", routedCondition, "message", payloadPreview(msg.Data))
						}
						// Find matching router sink by condition
						if ch, ok := routerChans[routedCondition]; ok {
							p.logger.V(1).Info("Routing message to condition sink", "condition", routedCondition)
							// Record routing queue wait time
							metrics.RecordTaskQueueWaitTime(p.namespace, p.name, "routing", time.Since(queueWaitStart).Seconds())
							metrics.SetTaskQueueSize(p.namespace, p.name, routedCondition, len(ch))
							select {
							case ch <- msg:
							case <-ctx.Done():
								return
							}
						} else {
							// Condition not found, send to default
							availableConditions := make([]string, 0, len(routerChans))
							for cond := range routerChans {
								availableConditions = append(availableConditions, cond)
							}
							p.logger.V(1).Info("Condition not found in router sinks, sending to default", "condition", routedCondition, "available", availableConditions)
							metrics.RecordTaskQueueWaitTime(p.namespace, p.name, "routing", time.Since(queueWaitStart).Seconds())
							metrics.SetTaskQueueSize(p.namespace, p.name, "default", len(defaultChan))
							select {
							case defaultChan <- msg:
							case <-ctx.Done():
								return
							}
						}
					} else {
						if log := p.logger.V(1); log.Enabled() {
							log.Info("Message has no routing condition, sending to default", "message", payloadPreview(msg.Data))
						}
						metrics.RecordTaskQueueWaitTime(p.namespace, p.name, "routing", time.Since(queueWaitStart).Seconds())
						metrics.SetTaskQueueSize(p.namespace, p.name, "default", len(defaultChan))
						select {
						case defaultChan <- msg:
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}()

		// Write to router sinks
		var wg sync.WaitGroup
		for condition, sinkSpec := range p.routerSinks {
			if ch, ok := routerChans[condition]; ok {
				wg.Add(1)
				go func(cond string, spec v1.SinkSpec, msgChan <-chan *types.Message) {
					defer wg.Done()

					// Create connector for this route
					routeSink, err := connectors.CreateSinkConnector(&spec)
					if err != nil {
						p.logger.Error(err, "Failed to create route sink connector", "condition", cond)
						return
					}

					p.initConnector(routeSink, p.logger.WithValues(logkeys.ConnectorType, spec.Type+"-sink"))

					if err := connectWithRetry(ctx, routeSink, "route-sink-"+cond, 0, 30*time.Second, p.logger); err != nil {
						p.logger.Error(err, "Failed to connect to route sink", "condition", cond)
						return
					}
					defer routeSink.Close()

					// Use error handling for route sinks too
					if err := p.writeMessagesWithErrorHandling(ctx, msgChan, routeSink); err != nil {
						p.logger.Error(err, "Failed to write messages to route sink", "condition", cond)
					}
				}(condition, sinkSpec, ch)
			}
		}

		// Write to default sink
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.writeMessagesWithErrorHandling(ctx, defaultChan, p.sink); err != nil {
				p.logger.Error(err, "Failed to write messages to default sink")
			}
		}()

		wg.Wait()
		return nil
	}

	// No router, write to main sink
	p.logger.Info("Writing messages to main sink")
	// Track queue size before write
	metrics.SetTaskQueueSize(p.namespace, p.name, "output", len(messages))
	return p.writeMessagesWithErrorHandling(ctx, messages, p.sink)
}

// writeMessagesWithErrorHandling writes messages to sink and handles errors by sending failed messages to error sink.
//
// Limitations (due to SinkConnector.Write returning a single error per call, not per message):
//   - When the main sink returns an error, it is received asynchronously. The message sent to the error sink
//     is the one we were attempting to write at the time the error was observed; the actual failed message
//     may be an earlier one. Thus the error sink may receive an approximate message (see metadata
//     "error_message_approximate").
//   - Only one error is guaranteed to be reported per Write call. If multiple messages fail in one session,
//     only one error is sent to writeErrChan; the rest are lost. At most one message is forwarded to the
//     error sink per Write invocation in case of failure.
func (p *Processor) writeMessagesWithErrorHandling(ctx context.Context, messages <-chan *types.Message, sink connectors.SinkConnector) error {
	// If error sink is not configured, use standard write
	if p.errorSink == nil {
		if err := sink.Write(ctx, messages); err != nil {
			p.logger.Error(err, "Failed to write messages to sink")
			return err
		}
		p.logger.Info("Successfully completed writing messages to sink",
			logkeys.ProcessedCount, atomic.LoadInt64(&p.processedCount),
			logkeys.ErrorCount, atomic.LoadInt64(&p.errorCount),
		)
		return nil
	}

	// Process messages with error handling
	// Since Write interface doesn't allow per-message error handling,
	// we'll use a wrapper that processes messages individually
	errorChan := make(chan *types.Message, p.channelBufferSize())
	var wg sync.WaitGroup
	hasErrors := false
	var hasErrorsMu sync.Mutex

	// Start error sink writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := p.errorSink.Write(ctx, errorChan); err != nil {
			p.logger.Error(err, "Failed to write messages to error sink")
		}
	}()

	// Process messages individually to catch errors
	// We'll use a buffered channel approach to handle errors
	mainSinkChan := make(chan *types.Message, p.channelBufferSize())
	writeErrChan := make(chan error, constants.DefaultSingleValueChannelBufferSize)

	// Start main sink writer in goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(writeErrChan)
		if err := sink.Write(ctx, mainSinkChan); err != nil {
			writeErrChan <- err
			p.logger.Error(err, "Error writing to main sink")
		}
	}()

	// Route messages and monitor for errors
	go func() {
		defer close(mainSinkChan)
		defer close(errorChan)

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-messages:
				if !ok {
					// All messages processed, check for errors
					select {
					case err := <-writeErrChan:
						if err != nil {
							p.logger.Error(err, "Error occurred during write, but messages were already processed")
							hasErrorsMu.Lock()
							hasErrors = true
							hasErrorsMu.Unlock()
						}
					default:
						// No error
					}
					return
				}

				// Try to send to main sink
				writeStart := time.Now()
				select {
				case mainSinkChan <- msg:
					// Successfully queued for main sink
					writeDuration := time.Since(writeStart).Seconds()
					metrics.RecordTaskStageDuration(p.namespace, p.name, "sink_write", writeDuration)
					route := getRouteFromMessage(msg)
					metrics.RecordMessageSent(p.namespace, p.name, p.spec.Sink.Type, route)
					metrics.RecordTaskOperation(p.namespace, p.name, "sink_write", "success")
				case err := <-writeErrChan:
					// Error occurred - send message to error sink
					if err != nil {
						writeDuration := time.Since(writeStart).Seconds()
						metrics.RecordTaskStageDuration(p.namespace, p.name, "sink_write", writeDuration)
						metrics.RecordTaskStageError(p.namespace, p.name, "sink_write", errclass.GetErrorType(err))
						metrics.RecordTaskOperation(p.namespace, p.name, "sink_write", "error")

						p.logger.Error(err, "Failed to write message to sink, sending to error sink",
							"message", string(msg.Data))
						atomic.AddInt64(&p.errorCount, 1)

						// Create error message with error information embedded in the data.
						// Message is approximate: sink returns one error per Write, so the real failed message may be earlier.
						errorMsg := p.createErrorMessage(msg, err, true)

						// Send to error sink
						errorSinkStart := time.Now()
						if v1.ShouldAckOnErrorSink(p.spec.Errors) {
							errorMsg.Ack = msg.Ack
						}
						select {
						case errorChan <- errorMsg:
							errorSinkDuration := time.Since(errorSinkStart).Seconds()
							metrics.RecordTaskStageDuration(p.namespace, p.name, "error_sink_write", errorSinkDuration)
							metrics.RecordMessageSent(p.namespace, p.name, p.spec.Errors.Type, "error")
							metrics.RecordTaskOperation(p.namespace, p.name, "error_sink_write", "success")
							hasErrorsMu.Lock()
							hasErrors = true
							hasErrorsMu.Unlock()
						case <-ctx.Done():
							metrics.RecordTaskOperation(p.namespace, p.name, "error_sink_write", "cancelled")
							return
						}
					}
				case <-ctx.Done():
					metrics.RecordTaskOperation(p.namespace, p.name, "sink_write", "cancelled")
					return
				}
			}
		}
	}()

	// Wait for all writers to finish
	wg.Wait()

	hasErrorsMu.Lock()
	defer hasErrorsMu.Unlock()
	if hasErrors {
		p.logger.Info("Some messages were sent to error sink",
			logkeys.ProcessedCount, atomic.LoadInt64(&p.processedCount),
			logkeys.ErrorCount, atomic.LoadInt64(&p.errorCount),
		)
		// Don't return error if error sink is configured - errors are handled
		return nil
	}

	p.logger.Info("Successfully completed writing messages to sink",
		logkeys.ProcessedCount, atomic.LoadInt64(&p.processedCount),
		logkeys.ErrorCount, atomic.LoadInt64(&p.errorCount),
	)
	return nil
}

// GetStats returns processing statistics
func (p *Processor) GetStats() (processedCount, errorCount int64) {
	return atomic.LoadInt64(&p.processedCount), atomic.LoadInt64(&p.errorCount)
}

// getTransformerType returns the transformer type by index.
func getTransformerType(spec *v1.DataFlowSpec, index int) string {
	if index < len(spec.Transformations) {
		return spec.Transformations[index].Type
	}
	return "unknown"
}

// getRouteFromMessage extracts the route from message metadata.
func getRouteFromMessage(msg *types.Message) string {
	if route, ok := msg.Metadata["routed_condition"].(string); ok {
		return route
	}
	return "default"
}

// createErrorMessage creates an error message with error information embedded in the data.
// If approximate is true, the failed message might not be originalMsg (e.g. when sink returns one error per Write call).
func (p *Processor) createErrorMessage(originalMsg *types.Message, err error, approximate bool) *types.Message {
	originalSink := "unknown"
	if p.spec != nil {
		originalSink = p.spec.Sink.Type
	}

	// Try to parse original message as JSON
	var originalData map[string]interface{}
	if err := json.Unmarshal(originalMsg.Data, &originalData); err != nil {
		// If original message is not JSON, wrap it
		originalData = map[string]interface{}{
			"original_data": string(originalMsg.Data),
		}
	}

	// Create error message structure
	errorData := map[string]interface{}{
		"error": map[string]interface{}{
			"message":       err.Error(),
			"timestamp":     time.Now().Format(time.RFC3339),
			"original_sink": originalSink,
		},
		"original_message": originalData,
	}

	// Add metadata from original message if present
	if originalMsg.Metadata != nil {
		if errorData["error"].(map[string]interface{})["metadata"] == nil {
			errorData["error"].(map[string]interface{})["metadata"] = make(map[string]interface{})
		}
		for k, v := range originalMsg.Metadata {
			errorData["error"].(map[string]interface{})["metadata"].(map[string]interface{})[k] = v
		}
	}

	// Marshal error message to JSON
	errorDataBytes, errMarshal := json.Marshal(errorData)
	if errMarshal != nil {
		// Fallback: create simple error message
		fallbackData := map[string]interface{}{
			"error":           err.Error(),
			"error_timestamp": time.Now().Format(time.RFC3339),
			"original_sink":   originalSink,
			"original_data":   string(originalMsg.Data),
		}
		errorDataBytes, _ = json.Marshal(fallbackData)
	}

	// Create new message with error information
	errorMsg := &types.Message{
		Data:      errorDataBytes,
		Metadata:  make(map[string]interface{}),
		Timestamp: originalMsg.Timestamp,
	}

	// Copy original metadata
	if originalMsg.Metadata != nil {
		for k, v := range originalMsg.Metadata {
			errorMsg.Metadata[k] = v
		}
	}

	// Add error metadata
	errorMsg.Metadata["error"] = err.Error()
	errorMsg.Metadata["error_timestamp"] = time.Now().Format(time.RFC3339)
	errorMsg.Metadata["original_sink"] = originalSink
	errorMsg.Metadata["is_error_message"] = true
	if approximate {
		errorMsg.Metadata["error_message_approximate"] = true
	}

	return errorMsg
}
