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
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/connectors"
	"github.com/dataflow-operator/dataflow/internal/metrics"
	"github.com/dataflow-operator/dataflow/internal/transformers"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
)

// Processor orchestrates data flow from source through transformations to sink
type Processor struct {
	source         connectors.SourceConnector
	sink           connectors.SinkConnector
	errorSink      connectors.SinkConnector
	transformers   []transformers.Transformer
	routerSinks    map[string]v1.SinkSpec
	processedCount int64
	errorCount     int64
	mu             sync.RWMutex
	logger         logr.Logger
	namespace      string
	name           string
	spec           *v1.DataFlowSpec
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

	// Create source connector
	source, err := connectors.CreateSourceConnector(&spec.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to create source connector: %w", err)
	}

	// Set logger and metadata if connector supports it
	if loggerConnector, ok := source.(interface{ SetLogger(logr.Logger) }); ok {
		loggerConnector.SetLogger(logger)
	}
	if metadataConnector, ok := source.(interface{ SetMetadata(string, string) }); ok {
		metadataConnector.SetMetadata(namespace, name)
	}

	// Create sink connector
	sink, err := connectors.CreateSinkConnector(&spec.Sink)
	if err != nil {
		return nil, fmt.Errorf("failed to create sink connector: %w", err)
	}

	// Set logger and metadata if connector supports it
	if loggerConnector, ok := sink.(interface{ SetLogger(logr.Logger) }); ok {
		loggerConnector.SetLogger(logger)
	}
	if metadataConnector, ok := sink.(interface{ SetMetadata(string, string) }); ok {
		metadataConnector.SetMetadata(namespace, name)
	}

	// Create error sink connector if specified
	var errorSink connectors.SinkConnector
	if spec.Errors != nil {
		errorSink, err = connectors.CreateSinkConnector(spec.Errors)
		if err != nil {
			return nil, fmt.Errorf("failed to create error sink connector: %w", err)
		}

		// Set logger and metadata if connector supports it
		if loggerConnector, ok := errorSink.(interface{ SetLogger(logr.Logger) }); ok {
			loggerConnector.SetLogger(logger)
		}
		if metadataConnector, ok := errorSink.(interface{ SetMetadata(string, string) }); ok {
			metadataConnector.SetMetadata(namespace, name)
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

		// Set logger if transformer supports it
		if loggerTransformer, ok := transformer.(interface{ SetLogger(logr.Logger) }); ok {
			loggerTransformer.SetLogger(logger)
		}

		// Check if this is a router transformer
		if t.Type == "router" && t.Router != nil {
			// Store sink specs for each route
			for _, route := range t.Router.Routes {
				// Use condition as key for routing
				routerSinks[route.Condition] = route.Sink
			}
		}

		transformerList = append(transformerList, transformer)
	}

	return &Processor{
		source:       source,
		sink:         sink,
		errorSink:    errorSink,
		transformers: transformerList,
		routerSinks:  routerSinks,
		logger:       logger,
		namespace:    namespace,
		name:         name,
		spec:         spec,
	}, nil
}

// Start starts processing messages
func (p *Processor) Start(ctx context.Context) error {
	p.logger.Info("Starting processor")

	// Connect to source
	if err := p.source.Connect(ctx); err != nil {
		p.logger.Error(err, "Failed to connect to source")
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	defer p.source.Close()
	p.logger.Info("Connected to source")

	// Connect to main sink
	if err := p.sink.Connect(ctx); err != nil {
		p.logger.Error(err, "Failed to connect to sink")
		return fmt.Errorf("failed to connect to sink: %w", err)
	}
	defer p.sink.Close()
	p.logger.Info("Connected to sink")

	// Connect to error sink if specified
	if p.errorSink != nil {
		if err := p.errorSink.Connect(ctx); err != nil {
			p.logger.Error(err, "Failed to connect to error sink")
			return fmt.Errorf("failed to connect to error sink: %w", err)
		}
		defer p.errorSink.Close()
		p.logger.Info("Connected to error sink")
	}

	// Router sinks will be connected dynamically when needed

	// Read messages from source
	msgChan, err := p.source.Read(ctx)
	if err != nil {
		p.logger.Error(err, "Failed to read from source")
		return fmt.Errorf("failed to read from source: %w", err)
	}
	p.logger.Info("Started reading from source")

	// Process messages
	processedChan := make(chan *types.Message, 100)
	go p.processMessages(ctx, msgChan, processedChan)

	// Write messages to sink(s)
	p.logger.Info("Starting to write messages to sink")
	return p.writeMessages(ctx, processedChan)
}

// processMessages applies transformations to messages
func (p *Processor) processMessages(ctx context.Context, input <-chan *types.Message, output chan<- *types.Message) {
	defer close(output)

	// Метрики для отслеживания пропускной способности
	var messageCount int64
	var lastThroughputUpdate time.Time
	throughputWindow := 10 * time.Second

	// Отслеживание активных сообщений
	activeMessages := 0

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-input:
			if !ok {
				return
			}

			// Увеличиваем счетчик активных сообщений
			activeMessages++
			metrics.SetTaskActiveMessages(p.namespace, p.name, activeMessages)

			// Записываем размер входящего сообщения
			metrics.RecordTaskMessageSize(p.namespace, p.name, "input", len(msg.Data))

			// Record message received
			metrics.RecordMessageReceived(p.namespace, p.name, p.spec.Source.Type)
			messageReceivedTime := time.Now()
			startTime := messageReceivedTime

			// Записываем время этапа чтения
			readStageStart := time.Now()
			metrics.RecordTaskStageDuration(p.namespace, p.name, "read", time.Since(readStageStart).Seconds())

			// Apply transformations
			transformationStart := time.Now()
			messages := []*types.Message{msg}
			transformationStageStart := transformationStart

			for i, transformer := range p.transformers {
				transformerType := getTransformerType(p.spec, i)
				newMessages := make([]*types.Message, 0)
				inputCount := len(messages)
				metrics.RecordTransformerMessagesIn(p.namespace, p.name, transformerType, i, inputCount)

				// Записываем задержку между трансформерами
				if i > 0 {
					prevStage := fmt.Sprintf("transformer_%d", i-1)
					currStage := fmt.Sprintf("transformer_%d", i)
					metrics.RecordTaskStageLatency(p.namespace, p.name, prevStage, currStage, time.Since(transformationStageStart).Seconds())
				}
				transformationStageStart = time.Now()

				for _, m := range messages {
					msgStart := time.Now()
					p.logger.V(1).Info("Applying transformer",
						"transformerIndex", i,
						"inputMessageSize", len(m.Data),
						"inputMessagePreview", string(m.Data)[:min(200, len(m.Data))])

					// Записываем размер сообщения перед трансформацией
					metrics.RecordTaskMessageSize(p.namespace, p.name, fmt.Sprintf("transformer_%d_input", i), len(m.Data))

					transformed, err := transformer.Transform(ctx, m)
					transformationDuration := time.Since(msgStart).Seconds()

					if err != nil {
						p.logger.Error(err, "Transformation failed",
							"transformerIndex", i,
							"message", string(m.Data))
						metrics.RecordTransformerError(p.namespace, p.name, transformerType, i, getErrorType(err))
						metrics.RecordTaskStageError(p.namespace, p.name, fmt.Sprintf("transformer_%d", i), getErrorType(err))
						metrics.RecordTaskOperation(p.namespace, p.name, "transform", "error")
						p.mu.Lock()
						p.errorCount++
						p.mu.Unlock()
						continue
					}

					// Записываем время выполнения трансформации
					metrics.RecordTaskStageDuration(p.namespace, p.name, fmt.Sprintf("transformer_%d", i), transformationDuration)
					metrics.RecordTransformerExecution(p.namespace, p.name, transformerType, i)
					metrics.RecordTransformerDuration(p.namespace, p.name, transformerType, i, transformationDuration)
					metrics.RecordTaskOperation(p.namespace, p.name, "transform", "success")

					// Log transformation results
					for j, tmsg := range transformed {
						// Записываем размер выходного сообщения
						metrics.RecordTaskMessageSize(p.namespace, p.name, fmt.Sprintf("transformer_%d_output", i), len(tmsg.Data))

						p.logger.V(1).Info("Transformation result",
							"transformerIndex", i,
							"outputMessageIndex", j,
							"outputMessageSize", len(tmsg.Data),
							"outputMessagePreview", string(tmsg.Data)[:min(200, len(tmsg.Data))])

						// Log routing metadata after router transformation
						if routedCond, ok := tmsg.Metadata["routed_condition"].(string); ok {
							p.logger.Info("Router set routed_condition",
								"condition", routedCond,
								"message", string(tmsg.Data))
						}
					}

					newMessages = append(newMessages, transformed...)
				}

				// Log transformation summary
				if len(newMessages) != inputCount {
					p.logger.V(1).Info("Transformation changed message count",
						"transformerIndex", i,
						"inputMessages", inputCount,
						"outputMessages", len(newMessages))
				}

				messages = newMessages
				metrics.RecordTransformerMessagesOut(p.namespace, p.name, transformerType, i, len(newMessages))
			}

			// Записываем время этапа трансформации
			transformationDuration := time.Since(transformationStart).Seconds()
			metrics.RecordTaskStageDuration(p.namespace, p.name, "transformation", transformationDuration)

			// Записываем задержку между трансформацией и записью
			writeStageStart := time.Now()
			metrics.RecordTaskStageLatency(p.namespace, p.name, "transformation", "write", time.Since(writeStageStart).Seconds())

			if len(messages) > 0 {
				p.logger.V(1).Info("Processed message", "inputMessages", 1, "outputMessages", len(messages))
			}

			// Record processing duration
			processingDuration := time.Since(startTime).Seconds()
			metrics.DataFlowProcessingDuration.WithLabelValues(p.namespace, p.name).Observe(processingDuration)

			// Записываем end-to-end latency
			endToEndLatency := time.Since(messageReceivedTime).Seconds()
			metrics.RecordTaskEndToEndLatency(p.namespace, p.name, endToEndLatency)

			// Send transformed messages
			writeStart := time.Now()
			for _, m := range messages {
				// Записываем размер сообщения перед записью
				metrics.RecordTaskMessageSize(p.namespace, p.name, "output", len(m.Data))

				select {
				case output <- m:
					// Записываем время этапа записи
					writeDuration := time.Since(writeStart).Seconds()
					metrics.RecordTaskStageDuration(p.namespace, p.name, "write", writeDuration)

					p.mu.Lock()
					p.processedCount++
					messageCount++
					p.mu.Unlock()

					// Record message sent (route will be determined in writeMessages)
					route := getRouteFromMessage(m)
					metrics.RecordMessageSent(p.namespace, p.name, p.spec.Sink.Type, route)
					metrics.RecordTaskOperation(p.namespace, p.name, "write", "success")

					// Уменьшаем счетчик активных сообщений
					activeMessages--
					metrics.SetTaskActiveMessages(p.namespace, p.name, activeMessages)
				case <-ctx.Done():
					metrics.RecordTaskOperation(p.namespace, p.name, "write", "cancelled")
					activeMessages--
					metrics.SetTaskActiveMessages(p.namespace, p.name, activeMessages)
					return
				}
			}

			// Обновляем пропускную способность каждые 10 секунд
			now := time.Now()
			if now.Sub(lastThroughputUpdate) >= throughputWindow {
				p.mu.RLock()
				throughput := float64(messageCount) / throughputWindow.Seconds()
				p.mu.RUnlock()
				metrics.SetTaskThroughput(p.namespace, p.name, throughput)
				messageCount = 0
				lastThroughputUpdate = now

				// Обновляем процент успешных операций
				p.mu.RLock()
				total := p.processedCount + p.errorCount
				var successRate float64
				if total > 0 {
					successRate = float64(p.processedCount) / float64(total)
				}
				p.mu.RUnlock()
				metrics.SetTaskSuccessRate(p.namespace, p.name, successRate)
			}
		}
	}
}

// writeMessages writes messages to appropriate sink(s)
func (p *Processor) writeMessages(ctx context.Context, messages <-chan *types.Message) error {
	// Check if we have router sinks
	if len(p.routerSinks) > 0 {
		// Route messages to different sinks
		routerChans := make(map[string]chan *types.Message)
		for condition := range p.routerSinks {
			routerChans[condition] = make(chan *types.Message, 100)
		}
		defaultChan := make(chan *types.Message, 100)

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

					// Отслеживаем размер очереди перед маршрутизацией
					queueWaitStart := time.Now()
					metrics.SetTaskQueueSize(p.namespace, p.name, "routing", len(messages))

					// Check if message has routing metadata
					if routedCondition, ok := msg.Metadata["routed_condition"].(string); ok {
						p.logger.V(1).Info("Message has routing condition", "condition", routedCondition, "message", string(msg.Data))
						// Find matching router sink by condition
						if ch, ok := routerChans[routedCondition]; ok {
							p.logger.V(1).Info("Routing message to condition sink", "condition", routedCondition)
							// Записываем время ожидания в очереди маршрутизации
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
						p.logger.V(1).Info("Message has no routing condition, sending to default", "message", string(msg.Data))
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

					// Set logger and metadata if connector supports it
					if loggerConnector, ok := routeSink.(interface{ SetLogger(logr.Logger) }); ok {
						loggerConnector.SetLogger(p.logger)
					}
					if metadataConnector, ok := routeSink.(interface{ SetMetadata(string, string) }); ok {
						metadataConnector.SetMetadata(p.namespace, p.name)
					}

					if err := routeSink.Connect(ctx); err != nil {
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
	// Отслеживаем размер очереди перед записью
	metrics.SetTaskQueueSize(p.namespace, p.name, "output", len(messages))
	return p.writeMessagesWithErrorHandling(ctx, messages, p.sink)
}

// writeMessagesWithErrorHandling writes messages to sink and handles errors by sending failed messages to error sink
func (p *Processor) writeMessagesWithErrorHandling(ctx context.Context, messages <-chan *types.Message, sink connectors.SinkConnector) error {
	// If error sink is not configured, use standard write
	if p.errorSink == nil {
		if err := sink.Write(ctx, messages); err != nil {
			p.logger.Error(err, "Failed to write messages to sink")
			return err
		}
		p.logger.Info("Successfully completed writing messages to sink")
		return nil
	}

	// Process messages with error handling
	// Since Write interface doesn't allow per-message error handling,
	// we'll use a wrapper that processes messages individually
	errorChan := make(chan *types.Message, 100)
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
	mainSinkChan := make(chan *types.Message, 100)
	writeErrChan := make(chan error, 1)

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
						metrics.RecordTaskStageError(p.namespace, p.name, "sink_write", getErrorType(err))
						metrics.RecordTaskOperation(p.namespace, p.name, "sink_write", "error")

						p.logger.Error(err, "Failed to write message to sink, sending to error sink",
							"message", string(msg.Data))
						p.mu.Lock()
						p.errorCount++
						p.mu.Unlock()

						// Create error message with error information embedded in the data
						errorMsg := p.createErrorMessage(msg, err)

						// Send to error sink
						errorSinkStart := time.Now()
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
		p.logger.Info("Some messages were sent to error sink")
		// Don't return error if error sink is configured - errors are handled
		return nil
	}

	p.logger.Info("Successfully completed writing messages to sink")
	return nil
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// GetStats returns processing statistics
func (p *Processor) GetStats() (processedCount, errorCount int64) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.processedCount, p.errorCount
}

// getTransformerType возвращает тип трансформера по индексу
func getTransformerType(spec *v1.DataFlowSpec, index int) string {
	if index < len(spec.Transformations) {
		return spec.Transformations[index].Type
	}
	return "unknown"
}

// getRouteFromMessage извлекает маршрут из метаданных сообщения
func getRouteFromMessage(msg *types.Message) string {
	if route, ok := msg.Metadata["routed_condition"].(string); ok {
		return route
	}
	return "default"
}

// getErrorType извлекает тип ошибки из ошибки
func getErrorType(err error) string {
	if err == nil {
		return "unknown"
	}
	// Можно расширить логику для более детальной классификации ошибок
	return "transformation_error"
}

// createErrorMessage creates an error message with error information embedded in the data
func (p *Processor) createErrorMessage(originalMsg *types.Message, err error) *types.Message {
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
			"original_sink": p.spec.Sink.Type,
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
	errorDataBytes, err := json.Marshal(errorData)
	if err != nil {
		// Fallback: create simple error message
		fallbackData := map[string]interface{}{
			"error":           err.Error(),
			"error_timestamp": time.Now().Format(time.RFC3339),
			"original_sink":   p.spec.Sink.Type,
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
	errorMsg.Metadata["original_sink"] = p.spec.Sink.Type
	errorMsg.Metadata["is_error_message"] = true

	return errorMsg
}
