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

package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// DataFlowMessagesReceived — messages received per manifest
	DataFlowMessagesReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_messages_received_total",
			Help: "Total number of messages received from source by dataflow manifest",
		},
		[]string{"namespace", "name", "source_type"},
	)

	// DataFlowMessagesSent — messages sent per manifest
	DataFlowMessagesSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_messages_sent_total",
			Help: "Total number of messages sent to sink by dataflow manifest",
		},
		[]string{"namespace", "name", "sink_type", "route"},
	)

	// DataFlowProcessingDuration — message processing time
	DataFlowProcessingDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dataflow_processing_duration_seconds",
			Help:    "Time spent processing messages",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 10), // 1ms to ~1s
		},
		[]string{"namespace", "name"},
	)

	// ConnectorMessagesRead — messages read from source connector
	ConnectorMessagesRead = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_connector_messages_read_total",
			Help: "Total number of messages read from source connector",
		},
		[]string{"namespace", "name", "connector_type", "connector_name"},
	)

	// ConnectorMessagesWritten — messages written to sink connector
	ConnectorMessagesWritten = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_connector_messages_written_total",
			Help: "Total number of messages written to sink connector",
		},
		[]string{"namespace", "name", "connector_type", "connector_name", "route"},
	)

	// ConnectorErrors — errors in connectors
	ConnectorErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_connector_errors_total",
			Help: "Total number of errors in connectors",
		},
		[]string{"namespace", "name", "connector_type", "connector_name", "operation", "error_type"},
	)

	// ConnectorConnectionStatus — connector connection status
	ConnectorConnectionStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dataflow_connector_connection_status",
			Help: "Connection status of connector (1 = connected, 0 = disconnected)",
		},
		[]string{"namespace", "name", "connector_type", "connector_name"},
	)

	// ConnectorSourcePollHealthy — last polling read attempt for source connectors (1 = success, 0 = error)
	ConnectorSourcePollHealthy = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dataflow_connector_source_poll_healthy",
			Help: "Whether the last polling read attempt succeeded (1 = success, 0 = failure)",
		},
		[]string{"namespace", "name", "connector_type", "connector_name"},
	)

	// TransformerExecutions — transformer execution count
	TransformerExecutions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_transformer_executions_total",
			Help: "Total number of transformer executions",
		},
		[]string{"namespace", "name", "transformer_type", "transformer_index"},
	)

	// TransformerErrors — errors in transformers
	TransformerErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_transformer_errors_total",
			Help: "Total number of errors in transformers",
		},
		[]string{"namespace", "name", "transformer_type", "transformer_index", "error_type"},
	)

	// TransformerDuration — transformer execution time
	TransformerDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dataflow_transformer_duration_seconds",
			Help:    "Time spent executing transformer",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 12), // 0.1ms to ~400ms
		},
		[]string{"namespace", "name", "transformer_type", "transformer_index"},
	)

	// TransformerMessagesIn — messages input to transformer
	TransformerMessagesIn = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_transformer_messages_in_total",
			Help: "Total number of messages input to transformer",
		},
		[]string{"namespace", "name", "transformer_type", "transformer_index"},
	)

	// TransformerMessagesOut — messages output from transformer
	TransformerMessagesOut = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_transformer_messages_out_total",
			Help: "Total number of messages output from transformer",
		},
		[]string{"namespace", "name", "transformer_type", "transformer_index"},
	)

	// DataFlowStatus — DataFlow manifest status
	DataFlowStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dataflow_status",
			Help: "Status of DataFlow manifest (1 = Running, 0 = Stopped/Error)",
		},
		[]string{"namespace", "name", "phase"},
	)

	// TaskStageDuration — individual task stage execution time
	TaskStageDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dataflow_task_stage_duration_seconds",
			Help:    "Time spent in each task execution stage",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 14), // 0.1ms to ~1.6s
		},
		[]string{"namespace", "name", "stage"},
	)

	// TaskMessageSize — message size at different processing stages
	TaskMessageSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dataflow_task_message_size_bytes",
			Help:    "Size of messages at different processing stages",
			Buckets: prometheus.ExponentialBuckets(64, 2, 16), // 64 bytes to ~4MB
		},
		[]string{"namespace", "name", "stage"},
	)

	// TaskStageLatency — latency between processing stages
	TaskStageLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dataflow_task_stage_latency_seconds",
			Help:    "Latency between processing stages",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 12), // 0.1ms to ~400ms
		},
		[]string{"namespace", "name", "from_stage", "to_stage"},
	)

	// TaskThroughput — throughput (messages per second)
	TaskThroughput = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dataflow_task_throughput_messages_per_second",
			Help: "Current throughput in messages per second",
		},
		[]string{"namespace", "name"},
	)

	// TaskSuccessRate — success rate of tasks
	TaskSuccessRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dataflow_task_success_rate",
			Help: "Success rate of task execution (0.0 to 1.0)",
		},
		[]string{"namespace", "name"},
	)

	// TaskEndToEndLatency — full message lifetime from receipt to delivery
	TaskEndToEndLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dataflow_task_end_to_end_latency_seconds",
			Help:    "End-to-end latency from message receipt to delivery",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12), // 1ms to ~2s
		},
		[]string{"namespace", "name"},
	)

	// TaskActiveMessages — active messages in processing
	TaskActiveMessages = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dataflow_task_active_messages",
			Help: "Number of messages currently being processed",
		},
		[]string{"namespace", "name"},
	)

	// TaskQueueSize — message queue size
	TaskQueueSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dataflow_task_queue_size",
			Help: "Current size of message queue",
		},
		[]string{"namespace", "name", "queue_type"},
	)

	// TaskQueueWaitTime — queue wait time
	TaskQueueWaitTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dataflow_task_queue_wait_time_seconds",
			Help:    "Time messages spend waiting in queue",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 12), // 0.1ms to ~400ms
		},
		[]string{"namespace", "name", "queue_type"},
	)

	// TaskOperationsTotal — total operations by type
	TaskOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_task_operations_total",
			Help: "Total number of operations by type",
		},
		[]string{"namespace", "name", "operation", "status"},
	)

	// TaskStageErrors — errors per stage
	TaskStageErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_task_stage_errors_total",
			Help: "Total number of errors per stage",
		},
		[]string{"namespace", "name", "stage", "error_type"},
	)

	// ControllerReconcileDuration — reconcile duration by result.
	ControllerReconcileDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dataflow_controller_reconcile_duration_seconds",
			Help:    "Duration of DataFlow controller reconcile loop",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"result"},
	)

	// ControllerReconcileErrors — reconcile errors by stage.
	ControllerReconcileErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_controller_reconcile_errors_total",
			Help: "Total number of DataFlow controller reconcile errors by stage",
		},
		[]string{"stage"},
	)

	// ControllerReconcileInflight — currently running reconcile loops.
	ControllerReconcileInflight = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "dataflow_controller_reconcile_inflight",
			Help: "Number of in-flight DataFlow reconciles",
		},
	)
)

func init() {
	// Register all metrics in controller-runtime registry
	metrics.Registry.MustRegister(
		DataFlowMessagesReceived,
		DataFlowMessagesSent,
		DataFlowProcessingDuration,
		ConnectorMessagesRead,
		ConnectorMessagesWritten,
		ConnectorErrors,
		ConnectorConnectionStatus,
		ConnectorSourcePollHealthy,
		TransformerExecutions,
		TransformerErrors,
		TransformerDuration,
		TransformerMessagesIn,
		TransformerMessagesOut,
		DataFlowStatus,
		TaskStageDuration,
		TaskMessageSize,
		TaskStageLatency,
		TaskThroughput,
		TaskSuccessRate,
		TaskEndToEndLatency,
		TaskActiveMessages,
		TaskQueueSize,
		TaskQueueWaitTime,
		TaskOperationsTotal,
		TaskStageErrors,
		ControllerReconcileDuration,
		ControllerReconcileErrors,
		ControllerReconcileInflight,
	)
}

// RecordMessageReceived records message received metric.
func RecordMessageReceived(namespace, name, sourceType string) {
	DataFlowMessagesReceived.WithLabelValues(namespace, name, sourceType).Inc()
}

// RecordMessageSent records message sent metric.
func RecordMessageSent(namespace, name, sinkType, route string) {
	DataFlowMessagesSent.WithLabelValues(namespace, name, sinkType, route).Inc()
}

// RecordConnectorMessageRead records connector message read metric.
func RecordConnectorMessageRead(namespace, name, connectorType, connectorName string) {
	ConnectorMessagesRead.WithLabelValues(namespace, name, connectorType, connectorName).Inc()
}

// RecordConnectorMessageWritten records connector message written metric.
func RecordConnectorMessageWritten(namespace, name, connectorType, connectorName, route string) {
	ConnectorMessagesWritten.WithLabelValues(namespace, name, connectorType, connectorName, route).Inc()
}

// RecordConnectorError records connector error metric.
func RecordConnectorError(namespace, name, connectorType, connectorName, operation, errorType string) {
	ConnectorErrors.WithLabelValues(namespace, name, connectorType, connectorName, operation, errorType).Inc()
}

// SetConnectorConnectionStatus sets connector connection status.
func SetConnectorConnectionStatus(namespace, name, connectorType, connectorName string, connected bool) {
	status := 0.0
	if connected {
		status = 1.0
	}
	ConnectorConnectionStatus.WithLabelValues(namespace, name, connectorType, connectorName).Set(status)
}

// SetConnectorSourcePollHealthy sets whether the last polling source read attempt succeeded.
func SetConnectorSourcePollHealthy(namespace, name, connectorType, connectorName string, healthy bool) {
	v := 0.0
	if healthy {
		v = 1.0
	}
	ConnectorSourcePollHealthy.WithLabelValues(namespace, name, connectorType, connectorName).Set(v)
}

// RecordTransformerExecution records transformer execution metric.
func RecordTransformerExecution(namespace, name, transformerType string, transformerIndex int) {
	TransformerExecutions.WithLabelValues(namespace, name, transformerType, formatIndex(transformerIndex)).Inc()
}

// RecordTransformerError records transformer error metric.
func RecordTransformerError(namespace, name, transformerType string, transformerIndex int, errorType string) {
	TransformerErrors.WithLabelValues(namespace, name, transformerType, formatIndex(transformerIndex), errorType).Inc()
}

// RecordTransformerDuration records transformer execution duration.
func RecordTransformerDuration(namespace, name, transformerType string, transformerIndex int, durationSeconds float64) {
	TransformerDuration.WithLabelValues(namespace, name, transformerType, formatIndex(transformerIndex)).Observe(durationSeconds)
}

// RecordTransformerMessagesIn records messages input to transformer.
func RecordTransformerMessagesIn(namespace, name, transformerType string, transformerIndex int, count int) {
	TransformerMessagesIn.WithLabelValues(namespace, name, transformerType, formatIndex(transformerIndex)).Add(float64(count))
}

// RecordTransformerMessagesOut records messages output from transformer.
func RecordTransformerMessagesOut(namespace, name, transformerType string, transformerIndex int, count int) {
	TransformerMessagesOut.WithLabelValues(namespace, name, transformerType, formatIndex(transformerIndex)).Add(float64(count))
}

// dataflowPhases lists all possible DataFlow phases. Used to clean up old
// time series when phase changes — Prometheus GaugeVec does not auto-delete
// label combinations, so old phases would otherwise persist indefinitely.
var dataflowPhases = []string{"Pending", "Running", "Error", "Stopped"}

// SetDataFlowStatus sets DataFlow manifest status.
// Removes previous phase time series for this dataflow to avoid multiple
// phases being reported simultaneously (GaugeVec retains old label combos).
func SetDataFlowStatus(namespace, name, phase string) {
	// Delete old phase time series for this (namespace, name)
	for _, p := range dataflowPhases {
		DataFlowStatus.DeleteLabelValues(namespace, name, p)
	}
	// Set 1 for Running, 0 for others
	status := 0.0
	if phase == "Running" {
		status = 1.0
	}
	DataFlowStatus.WithLabelValues(namespace, name, phase).Set(status)
}

// formatIndex formats index as string.
func formatIndex(index int) string {
	return fmt.Sprintf("%d", index)
}

// RecordTaskStageDuration records task stage execution duration.
func RecordTaskStageDuration(namespace, name, stage string, durationSeconds float64) {
	TaskStageDuration.WithLabelValues(namespace, name, stage).Observe(durationSeconds)
}

// RecordTaskMessageSize records message size at processing stage.
func RecordTaskMessageSize(namespace, name, stage string, sizeBytes int) {
	TaskMessageSize.WithLabelValues(namespace, name, stage).Observe(float64(sizeBytes))
}

// RecordTaskStageLatency records latency between stages.
func RecordTaskStageLatency(namespace, name, fromStage, toStage string, latencySeconds float64) {
	TaskStageLatency.WithLabelValues(namespace, name, fromStage, toStage).Observe(latencySeconds)
}

// SetTaskThroughput sets current throughput.
func SetTaskThroughput(namespace, name string, messagesPerSecond float64) {
	TaskThroughput.WithLabelValues(namespace, name).Set(messagesPerSecond)
}

// SetTaskSuccessRate sets task success rate.
func SetTaskSuccessRate(namespace, name string, rate float64) {
	TaskSuccessRate.WithLabelValues(namespace, name).Set(rate)
}

// RecordTaskEndToEndLatency records full message lifetime.
func RecordTaskEndToEndLatency(namespace, name string, latencySeconds float64) {
	TaskEndToEndLatency.WithLabelValues(namespace, name).Observe(latencySeconds)
}

// SetTaskActiveMessages sets active message count.
func SetTaskActiveMessages(namespace, name string, count int) {
	TaskActiveMessages.WithLabelValues(namespace, name).Set(float64(count))
}

// SetTaskQueueSize sets queue size.
func SetTaskQueueSize(namespace, name, queueType string, size int) {
	TaskQueueSize.WithLabelValues(namespace, name, queueType).Set(float64(size))
}

// RecordTaskQueueWaitTime records queue wait time.
func RecordTaskQueueWaitTime(namespace, name, queueType string, waitTimeSeconds float64) {
	TaskQueueWaitTime.WithLabelValues(namespace, name, queueType).Observe(waitTimeSeconds)
}

// RecordTaskOperation records operation execution.
func RecordTaskOperation(namespace, name, operation, status string) {
	TaskOperationsTotal.WithLabelValues(namespace, name, operation, status).Inc()
}

// RecordTaskStageError records error at processing stage.
func RecordTaskStageError(namespace, name, stage, errorType string) {
	TaskStageErrors.WithLabelValues(namespace, name, stage, errorType).Inc()
}

// ObserveControllerReconcileDuration records reconcile duration with result label.
func ObserveControllerReconcileDuration(result string, durationSeconds float64) {
	ControllerReconcileDuration.WithLabelValues(result).Observe(durationSeconds)
}

// RecordControllerReconcileError increments reconcile error counter for a stage.
func RecordControllerReconcileError(stage string) {
	ControllerReconcileErrors.WithLabelValues(stage).Inc()
}
