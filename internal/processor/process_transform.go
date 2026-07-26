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
	"sync/atomic"
	"time"

	"github.com/dataflow-operator/dataflow/internal/constants"
	errclass "github.com/dataflow-operator/dataflow/internal/errors"
	"github.com/dataflow-operator/dataflow/internal/logkeys"
	"github.com/dataflow-operator/dataflow/internal/metrics"
	"github.com/dataflow-operator/dataflow/internal/types"
)

// transformWorkers returns configured parallel transform workers (default 1, capped at MaxTransformWorkers).
func (p *Processor) transformWorkers() int {
	if p.spec != nil && p.spec.TransformWorkers != nil && *p.spec.TransformWorkers > 0 {
		w := int(*p.spec.TransformWorkers)
		if w > constants.MaxTransformWorkers {
			return constants.MaxTransformWorkers
		}
		return w
	}
	return constants.DefaultTransformWorkers
}

// attachAckBarrier commits the source ack only after every derived message is acked.
// Zero derived messages (filter drop) ack immediately — the input was intentionally consumed.
func attachAckBarrier(parent *types.Message, derived []*types.Message) {
	if parent == nil || parent.Ack == nil {
		return
	}
	if len(derived) == 0 {
		parent.Ack()
		return
	}
	parentAck := parent.Ack
	remaining := int32(len(derived))
	for _, out := range derived {
		out.Ack = func() {
			if atomic.AddInt32(&remaining, -1) == 0 {
				parentAck()
			}
		}
	}
}

// applyTransformations runs the transformer chain on one input message.
func (p *Processor) applyTransformations(ctx context.Context, msg *types.Message) []*types.Message {
	messages := []*types.Message{msg}
	transformationStageStart := time.Now()
	debug := p.logger.V(1)

	for i, transformer := range p.transformers {
		transformerType := getTransformerType(p.spec, i)
		newMessages := make([]*types.Message, 0)
		inputCount := len(messages)
		metrics.RecordTransformerMessagesIn(p.namespace, p.name, transformerType, i, inputCount)

		sampleHist := metrics.ShouldSampleHotPathHistogram()
		if sampleHist && i > 0 {
			prevStage := fmt.Sprintf("transformer_%d", i-1)
			currStage := fmt.Sprintf("transformer_%d", i)
			metrics.RecordTaskStageLatency(p.namespace, p.name, prevStage, currStage, time.Since(transformationStageStart).Seconds())
		}
		transformationStageStart = time.Now()

		for _, m := range messages {
			msgStart := time.Now()
			if debug.Enabled() {
				debug.Info("Applying transformer",
					"transformerIndex", i,
					"inputMessageSize", len(m.Data),
					"inputMessagePreview", payloadPreview(m.Data))
			}

			if sampleHist {
				metrics.RecordTaskMessageSize(p.namespace, p.name, fmt.Sprintf("transformer_%d_input", i), len(m.Data))
			}

			transformed, err := transformer.Transform(ctx, m)
			transformationDuration := time.Since(msgStart).Seconds()

			if err != nil {
				p.logger.Error(err, "Transformation failed",
					logkeys.MessageID, types.MessageID(m),
					"transformerIndex", i,
					"message", string(m.Data))
				metrics.RecordTransformerError(p.namespace, p.name, transformerType, i, errclass.GetErrorType(err))
				metrics.RecordTaskStageError(p.namespace, p.name, fmt.Sprintf("transformer_%d", i), errclass.GetErrorType(err))
				metrics.RecordTaskOperation(p.namespace, p.name, "transform", "error")
				atomic.AddInt64(&p.errorCount, 1)
				continue
			}

			if sampleHist {
				metrics.RecordTaskStageDuration(p.namespace, p.name, fmt.Sprintf("transformer_%d", i), transformationDuration)
				metrics.RecordTransformerDuration(p.namespace, p.name, transformerType, i, transformationDuration)
			}
			metrics.RecordTransformerExecution(p.namespace, p.name, transformerType, i)
			metrics.RecordTaskOperation(p.namespace, p.name, "transform", "success")

			if debug.Enabled() {
				for j, tmsg := range transformed {
					if sampleHist {
						metrics.RecordTaskMessageSize(p.namespace, p.name, fmt.Sprintf("transformer_%d_output", i), len(tmsg.Data))
					}
					debug.Info("Transformation result",
						"transformerIndex", i,
						"outputMessageIndex", j,
						"outputMessageSize", len(tmsg.Data),
						"outputMessagePreview", payloadPreview(tmsg.Data))

					if routedCond, ok := tmsg.Metadata["routed_condition"].(string); ok {
						debug.Info("Router set routed_condition",
							logkeys.MessageID, types.MessageID(tmsg),
							"condition", routedCond,
							"message", string(tmsg.Data))
					}
				}
			} else if sampleHist {
				for _, tmsg := range transformed {
					metrics.RecordTaskMessageSize(p.namespace, p.name, fmt.Sprintf("transformer_%d_output", i), len(tmsg.Data))
				}
			}

			newMessages = append(newMessages, transformed...)
		}

		if len(newMessages) != inputCount {
			p.logger.V(1).Info("Transformation changed message count",
				"transformerIndex", i,
				"inputMessages", inputCount,
				"outputMessages", len(newMessages))
		}

		messages = newMessages
		metrics.RecordTransformerMessagesOut(p.namespace, p.name, transformerType, i, len(newMessages))
	}

	attachAckBarrier(msg, messages)
	return messages
}

func payloadPreview(data []byte) string {
	const maxPreview = 200
	if len(data) <= maxPreview {
		return string(data)
	}
	return string(data[:maxPreview])
}

type transformJob struct {
	seq        int64
	msg        *types.Message
	receivedAt time.Time
}

type transformOutcome struct {
	seq              int64
	out              []*types.Message
	receivedAt       time.Time
	transformStarted time.Time
}

// processMessages applies transformations then sends results to output.
// With transformWorkers > 1, messages are transformed in parallel and reordered before emit
// so sink write order and source ack order match input order.
func (p *Processor) processMessages(ctx context.Context, input <-chan *types.Message, output chan<- *types.Message) {
	defer close(output)

	workers := p.transformWorkers()
	if workers > 1 {
		p.processMessagesParallel(ctx, input, output, workers)
		return
	}
	p.processMessagesSerial(ctx, input, output)
}

func (p *Processor) processMessagesSerial(ctx context.Context, input <-chan *types.Message, output chan<- *types.Message) {
	var messageCount int64
	var lastThroughputUpdate time.Time
	throughputWindow := 10 * time.Second
	activeMessages := 0
	firstMessageLogged := false

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-input:
			if !ok {
				return
			}

			activeMessages++
			metrics.SetTaskActiveMessages(p.namespace, p.name, activeMessages)

			metrics.RecordTaskMessageSize(p.namespace, p.name, "input", len(msg.Data))
			metrics.RecordMessageReceived(p.namespace, p.name, p.spec.Source.Type)
			if !firstMessageLogged {
				firstMessageLogged = true
				p.logger.Info("First message received from source", logkeys.MessageID, types.MessageID(msg))
			}
			messageReceivedTime := time.Now()
			startTime := messageReceivedTime

			transformationStart := time.Now()
			messages := p.applyTransformations(ctx, msg)
			if metrics.ShouldSampleHotPathHistogram() {
				metrics.RecordTaskStageDuration(p.namespace, p.name, "transformation", time.Since(transformationStart).Seconds())
				metrics.DataFlowProcessingDuration.WithLabelValues(p.namespace, p.name).Observe(time.Since(startTime).Seconds())
				metrics.RecordTaskEndToEndLatency(p.namespace, p.name, time.Since(messageReceivedTime).Seconds())
			}

			if len(messages) > 0 {
				p.logger.V(1).Info("Processed message", "inputMessages", 1, "outputMessages", len(messages))
			}

			if !p.emitTransformed(ctx, output, messages, &activeMessages, &messageCount) {
				return
			}

			now := time.Now()
			if now.Sub(lastThroughputUpdate) >= throughputWindow {
				p.recordThroughput(messageCount, throughputWindow)
				messageCount = 0
				lastThroughputUpdate = now
			}
		}
	}
}

func (p *Processor) processMessagesParallel(ctx context.Context, input <-chan *types.Message, output chan<- *types.Message, workers int) {
	jobCh := make(chan transformJob, workers*2)
	resultCh := make(chan transformOutcome, workers*2)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				if ctx.Err() != nil {
					return
				}
				started := time.Now()
				out := p.applyTransformations(ctx, job.msg)
				outcome := transformOutcome{
					seq:              job.seq,
					out:              out,
					receivedAt:       job.receivedAt,
					transformStarted: started,
				}
				select {
				case resultCh <- outcome:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	var dispatcherDone sync.WaitGroup
	dispatcherDone.Add(1)
	go func() {
		defer dispatcherDone.Done()
		defer close(jobCh)
		var seq int64
		firstMessageLogged := false
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-input:
				if !ok {
					return
				}
				metrics.RecordTaskMessageSize(p.namespace, p.name, "input", len(msg.Data))
				metrics.RecordMessageReceived(p.namespace, p.name, p.spec.Source.Type)
				if !firstMessageLogged {
					firstMessageLogged = true
					p.logger.Info("First message received from source", logkeys.MessageID, types.MessageID(msg))
				}
				job := transformJob{
					seq:        seq,
					msg:        msg,
					receivedAt: time.Now(),
				}
				seq++
				select {
				case jobCh <- job:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	go func() {
		dispatcherDone.Wait()
		wg.Wait()
		close(resultCh)
	}()

	pending := make(map[int64]transformOutcome)
	var nextSeq int64
	var messageCount int64
	var lastThroughputUpdate time.Time
	throughputWindow := 10 * time.Second
	activeMessages := 0

	for {
		select {
		case <-ctx.Done():
			return
		case outcome, ok := <-resultCh:
			if !ok {
				return
			}
			pending[outcome.seq] = outcome
			for {
				ready, exists := pending[nextSeq]
				if !exists {
					break
				}
				delete(pending, nextSeq)
				nextSeq++

				activeMessages++
				metrics.SetTaskActiveMessages(p.namespace, p.name, activeMessages)

				if metrics.ShouldSampleHotPathHistogram() {
					metrics.RecordTaskStageDuration(p.namespace, p.name, "transformation", time.Since(ready.transformStarted).Seconds())
					metrics.DataFlowProcessingDuration.WithLabelValues(p.namespace, p.name).Observe(time.Since(ready.receivedAt).Seconds())
					metrics.RecordTaskEndToEndLatency(p.namespace, p.name, time.Since(ready.receivedAt).Seconds())
				}

				if len(ready.out) > 0 {
					p.logger.V(1).Info("Processed message", "inputMessages", 1, "outputMessages", len(ready.out))
				}

				if !p.emitTransformed(ctx, output, ready.out, &activeMessages, &messageCount) {
					return
				}

				now := time.Now()
				if now.Sub(lastThroughputUpdate) >= throughputWindow {
					p.recordThroughput(messageCount, throughputWindow)
					messageCount = 0
					lastThroughputUpdate = now
				}
			}
		}
	}
}

func (p *Processor) emitTransformed(
	ctx context.Context,
	output chan<- *types.Message,
	messages []*types.Message,
	activeMessages *int,
	messageCount *int64,
) bool {
	defer func() {
		*activeMessages--
		if *activeMessages < 0 {
			*activeMessages = 0
		}
		metrics.SetTaskActiveMessages(p.namespace, p.name, *activeMessages)
	}()

	if len(messages) == 0 {
		return true
	}

	writeStart := time.Now()
	sampleHist := metrics.ShouldSampleHotPathHistogram()
	for _, m := range messages {
		if sampleHist {
			metrics.RecordTaskMessageSize(p.namespace, p.name, "output", len(m.Data))
		}

		select {
		case output <- m:
			if sampleHist {
				metrics.RecordTaskStageDuration(p.namespace, p.name, "write", time.Since(writeStart).Seconds())
			}

			atomic.AddInt64(&p.processedCount, 1)
			*messageCount++

			route := getRouteFromMessage(m)
			metrics.RecordMessageSent(p.namespace, p.name, p.spec.Sink.Type, route)
			metrics.RecordTaskOperation(p.namespace, p.name, "write", "success")
		case <-ctx.Done():
			metrics.RecordTaskOperation(p.namespace, p.name, "write", "cancelled")
			return false
		}
	}
	return true
}

func (p *Processor) recordThroughput(messageCount int64, throughputWindow time.Duration) {
	throughput := float64(messageCount) / throughputWindow.Seconds()
	processed := atomic.LoadInt64(&p.processedCount)
	errors := atomic.LoadInt64(&p.errorCount)
	total := processed + errors
	var successRate float64
	if total > 0 {
		successRate = float64(processed) / float64(total)
	}

	metrics.SetTaskThroughput(p.namespace, p.name, throughput)
	metrics.SetTaskSuccessRate(p.namespace, p.name, successRate)
	p.logger.Info("Pipeline progress",
		logkeys.ProcessedCount, processed,
		logkeys.ErrorCount, errors,
		logkeys.Throughput, throughput,
	)
}
