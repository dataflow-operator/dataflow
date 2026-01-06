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
	"fmt"
	"sync"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/connectors"
	"github.com/dataflow-operator/dataflow/internal/transformers"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
)

// Processor orchestrates data flow from source through transformations to sink
type Processor struct {
	source         connectors.SourceConnector
	sink           connectors.SinkConnector
	transformers   []transformers.Transformer
	routerSinks    map[string]v1.SinkSpec
	processedCount int64
	errorCount     int64
	mu             sync.RWMutex
	logger         logr.Logger
}

// NewProcessor creates a new processor
func NewProcessor(spec *v1.DataFlowSpec) (*Processor, error) {
	return NewProcessorWithLogger(spec, logr.Discard())
}

// NewProcessorWithLogger creates a new processor with logger
func NewProcessorWithLogger(spec *v1.DataFlowSpec, logger logr.Logger) (*Processor, error) {
	// Create source connector
	source, err := connectors.CreateSourceConnector(&spec.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to create source connector: %w", err)
	}

	// Create sink connector
	sink, err := connectors.CreateSinkConnector(&spec.Sink)
	if err != nil {
		return nil, fmt.Errorf("failed to create sink connector: %w", err)
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
		transformers: transformerList,
		routerSinks:  routerSinks,
		logger:       logger,
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

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-input:
			if !ok {
				return
			}

			// Apply transformations
			messages := []*types.Message{msg}
			for i, transformer := range p.transformers {
				newMessages := make([]*types.Message, 0)
				for _, m := range messages {
					p.logger.V(1).Info("Applying transformer", "index", i, "message", string(m.Data))
					transformed, err := transformer.Transform(ctx, m)
					if err != nil {
						p.logger.Error(err, "Transformation failed", "message", string(m.Data))
						p.mu.Lock()
						p.errorCount++
						p.mu.Unlock()
						continue
					}
					// Log routing metadata after router transformation
					for _, tmsg := range transformed {
						if routedCond, ok := tmsg.Metadata["routed_condition"].(string); ok {
							p.logger.Info("Router set routed_condition", "condition", routedCond, "message", string(tmsg.Data))
						}
					}
					newMessages = append(newMessages, transformed...)
				}
				messages = newMessages
			}

			if len(messages) > 0 {
				p.logger.V(1).Info("Processed message", "inputMessages", 1, "outputMessages", len(messages))
			}

			// Send transformed messages
			for _, m := range messages {
				select {
				case output <- m:
					p.mu.Lock()
					p.processedCount++
					p.mu.Unlock()
				case <-ctx.Done():
					return
				}
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

					// Check if message has routing metadata
					if routedCondition, ok := msg.Metadata["routed_condition"].(string); ok {
						p.logger.V(1).Info("Message has routing condition", "condition", routedCondition, "message", string(msg.Data))
						// Find matching router sink by condition
						if ch, ok := routerChans[routedCondition]; ok {
							p.logger.V(1).Info("Routing message to condition sink", "condition", routedCondition)
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
							select {
							case defaultChan <- msg:
							case <-ctx.Done():
								return
							}
						}
					} else {
						p.logger.V(1).Info("Message has no routing condition, sending to default", "message", string(msg.Data))
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
						return
					}

					if err := routeSink.Connect(ctx); err != nil {
						return
					}
					defer routeSink.Close()

					routeSink.Write(ctx, msgChan)
				}(condition, sinkSpec, ch)
			}
		}

		// Write to default sink
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.sink.Write(ctx, defaultChan)
		}()

		wg.Wait()
		return nil
	}

	// No router, write to main sink
	p.logger.Info("Writing messages to main sink")
	if err := p.sink.Write(ctx, messages); err != nil {
		p.logger.Error(err, "Failed to write messages to sink")
		return err
	}
	p.logger.Info("Successfully completed writing messages to sink")
	return nil
}

// GetStats returns processing statistics
func (p *Processor) GetStats() (processedCount, errorCount int64) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.processedCount, p.errorCount
}
