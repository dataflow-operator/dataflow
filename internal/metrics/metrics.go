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
	// DataFlowMessagesReceived - количество полученных сообщений по манифесту
	DataFlowMessagesReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_messages_received_total",
			Help: "Total number of messages received from source by dataflow manifest",
		},
		[]string{"namespace", "name", "source_type"},
	)

	// DataFlowMessagesSent - количество отправленных сообщений по манифесту
	DataFlowMessagesSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_messages_sent_total",
			Help: "Total number of messages sent to sink by dataflow manifest",
		},
		[]string{"namespace", "name", "sink_type", "route"},
	)

	// DataFlowProcessingDuration - время обработки сообщений
	DataFlowProcessingDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dataflow_processing_duration_seconds",
			Help:    "Time spent processing messages",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 10), // от 1ms до ~1s
		},
		[]string{"namespace", "name"},
	)

	// ConnectorMessagesRead - количество прочитанных сообщений из source коннектора
	ConnectorMessagesRead = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_connector_messages_read_total",
			Help: "Total number of messages read from source connector",
		},
		[]string{"namespace", "name", "connector_type", "connector_name"},
	)

	// ConnectorMessagesWritten - количество записанных сообщений в sink коннектор
	ConnectorMessagesWritten = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_connector_messages_written_total",
			Help: "Total number of messages written to sink connector",
		},
		[]string{"namespace", "name", "connector_type", "connector_name", "route"},
	)

	// ConnectorErrors - количество ошибок в коннекторах
	ConnectorErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_connector_errors_total",
			Help: "Total number of errors in connectors",
		},
		[]string{"namespace", "name", "connector_type", "connector_name", "operation", "error_type"},
	)

	// ConnectorConnectionStatus - статус подключения коннектора
	ConnectorConnectionStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dataflow_connector_connection_status",
			Help: "Connection status of connector (1 = connected, 0 = disconnected)",
		},
		[]string{"namespace", "name", "connector_type", "connector_name"},
	)

	// TransformerExecutions - количество выполнений трансформера
	TransformerExecutions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_transformer_executions_total",
			Help: "Total number of transformer executions",
		},
		[]string{"namespace", "name", "transformer_type", "transformer_index"},
	)

	// TransformerErrors - количество ошибок в трансформерах
	TransformerErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_transformer_errors_total",
			Help: "Total number of errors in transformers",
		},
		[]string{"namespace", "name", "transformer_type", "transformer_index", "error_type"},
	)

	// TransformerDuration - время выполнения трансформера
	TransformerDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dataflow_transformer_duration_seconds",
			Help:    "Time spent executing transformer",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 12), // от 0.1ms до ~400ms
		},
		[]string{"namespace", "name", "transformer_type", "transformer_index"},
	)

	// TransformerMessagesIn - количество входящих сообщений в трансформер
	TransformerMessagesIn = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_transformer_messages_in_total",
			Help: "Total number of messages input to transformer",
		},
		[]string{"namespace", "name", "transformer_type", "transformer_index"},
	)

	// TransformerMessagesOut - количество исходящих сообщений из трансформера
	TransformerMessagesOut = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_transformer_messages_out_total",
			Help: "Total number of messages output from transformer",
		},
		[]string{"namespace", "name", "transformer_type", "transformer_index"},
	)

	// DataFlowStatus - статус DataFlow манифеста
	DataFlowStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dataflow_status",
			Help: "Status of DataFlow manifest (1 = Running, 0 = Stopped/Error)",
		},
		[]string{"namespace", "name", "phase"},
	)

	// TaskStageDuration - время выполнения отдельных этапов задачи
	TaskStageDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dataflow_task_stage_duration_seconds",
			Help:    "Time spent in each task execution stage",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 14), // от 0.1ms до ~1.6s
		},
		[]string{"namespace", "name", "stage"},
	)

	// TaskMessageSize - размер сообщений на разных этапах обработки
	TaskMessageSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dataflow_task_message_size_bytes",
			Help:    "Size of messages at different processing stages",
			Buckets: prometheus.ExponentialBuckets(64, 2, 16), // от 64 байт до ~4MB
		},
		[]string{"namespace", "name", "stage"},
	)

	// TaskStageLatency - задержка между этапами обработки
	TaskStageLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dataflow_task_stage_latency_seconds",
			Help:    "Latency between processing stages",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 12), // от 0.1ms до ~400ms
		},
		[]string{"namespace", "name", "from_stage", "to_stage"},
	)

	// TaskThroughput - пропускная способность (сообщений в секунду)
	TaskThroughput = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dataflow_task_throughput_messages_per_second",
			Help: "Current throughput in messages per second",
		},
		[]string{"namespace", "name"},
	)

	// TaskSuccessRate - процент успешных задач
	TaskSuccessRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dataflow_task_success_rate",
			Help: "Success rate of task execution (0.0 to 1.0)",
		},
		[]string{"namespace", "name"},
	)

	// TaskEndToEndLatency - полное время жизни сообщения от получения до отправки
	TaskEndToEndLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dataflow_task_end_to_end_latency_seconds",
			Help:    "End-to-end latency from message receipt to delivery",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12), // от 1ms до ~2s
		},
		[]string{"namespace", "name"},
	)

	// TaskActiveMessages - количество активных сообщений в обработке
	TaskActiveMessages = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dataflow_task_active_messages",
			Help: "Number of messages currently being processed",
		},
		[]string{"namespace", "name"},
	)

	// TaskQueueSize - размер очереди сообщений
	TaskQueueSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dataflow_task_queue_size",
			Help: "Current size of message queue",
		},
		[]string{"namespace", "name", "queue_type"},
	)

	// TaskQueueWaitTime - время ожидания в очереди
	TaskQueueWaitTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dataflow_task_queue_wait_time_seconds",
			Help:    "Time messages spend waiting in queue",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 12), // от 0.1ms до ~400ms
		},
		[]string{"namespace", "name", "queue_type"},
	)

	// TaskOperationsTotal - общее количество операций по типу
	TaskOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_task_operations_total",
			Help: "Total number of operations by type",
		},
		[]string{"namespace", "name", "operation", "status"},
	)

	// TaskStageErrors - количество ошибок на каждом этапе
	TaskStageErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_task_stage_errors_total",
			Help: "Total number of errors per stage",
		},
		[]string{"namespace", "name", "stage", "error_type"},
	)
)

func init() {
	// Регистрируем все метрики в реестре controller-runtime
	metrics.Registry.MustRegister(
		DataFlowMessagesReceived,
		DataFlowMessagesSent,
		DataFlowProcessingDuration,
		ConnectorMessagesRead,
		ConnectorMessagesWritten,
		ConnectorErrors,
		ConnectorConnectionStatus,
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
	)
}

// RecordMessageReceived записывает метрику получения сообщения
func RecordMessageReceived(namespace, name, sourceType string) {
	DataFlowMessagesReceived.WithLabelValues(namespace, name, sourceType).Inc()
}

// RecordMessageSent записывает метрику отправки сообщения
func RecordMessageSent(namespace, name, sinkType, route string) {
	DataFlowMessagesSent.WithLabelValues(namespace, name, sinkType, route).Inc()
}

// RecordConnectorMessageRead записывает метрику чтения сообщения из коннектора
func RecordConnectorMessageRead(namespace, name, connectorType, connectorName string) {
	ConnectorMessagesRead.WithLabelValues(namespace, name, connectorType, connectorName).Inc()
}

// RecordConnectorMessageWritten записывает метрику записи сообщения в коннектор
func RecordConnectorMessageWritten(namespace, name, connectorType, connectorName, route string) {
	ConnectorMessagesWritten.WithLabelValues(namespace, name, connectorType, connectorName, route).Inc()
}

// RecordConnectorError записывает метрику ошибки коннектора
func RecordConnectorError(namespace, name, connectorType, connectorName, operation, errorType string) {
	ConnectorErrors.WithLabelValues(namespace, name, connectorType, connectorName, operation, errorType).Inc()
}

// SetConnectorConnectionStatus устанавливает статус подключения коннектора
func SetConnectorConnectionStatus(namespace, name, connectorType, connectorName string, connected bool) {
	status := 0.0
	if connected {
		status = 1.0
	}
	ConnectorConnectionStatus.WithLabelValues(namespace, name, connectorType, connectorName).Set(status)
}

// RecordTransformerExecution записывает метрику выполнения трансформера
func RecordTransformerExecution(namespace, name, transformerType string, transformerIndex int) {
	TransformerExecutions.WithLabelValues(namespace, name, transformerType, formatIndex(transformerIndex)).Inc()
}

// RecordTransformerError записывает метрику ошибки трансформера
func RecordTransformerError(namespace, name, transformerType string, transformerIndex int, errorType string) {
	TransformerErrors.WithLabelValues(namespace, name, transformerType, formatIndex(transformerIndex), errorType).Inc()
}

// RecordTransformerDuration записывает время выполнения трансформера
func RecordTransformerDuration(namespace, name, transformerType string, transformerIndex int, durationSeconds float64) {
	TransformerDuration.WithLabelValues(namespace, name, transformerType, formatIndex(transformerIndex)).Observe(durationSeconds)
}

// RecordTransformerMessagesIn записывает количество входящих сообщений в трансформер
func RecordTransformerMessagesIn(namespace, name, transformerType string, transformerIndex int, count int) {
	TransformerMessagesIn.WithLabelValues(namespace, name, transformerType, formatIndex(transformerIndex)).Add(float64(count))
}

// RecordTransformerMessagesOut записывает количество исходящих сообщений из трансформера
func RecordTransformerMessagesOut(namespace, name, transformerType string, transformerIndex int, count int) {
	TransformerMessagesOut.WithLabelValues(namespace, name, transformerType, formatIndex(transformerIndex)).Add(float64(count))
}

// SetDataFlowStatus устанавливает статус DataFlow манифеста
func SetDataFlowStatus(namespace, name, phase string) {
	// Устанавливаем 1 для Running, 0 для остальных
	status := 0.0
	if phase == "Running" {
		status = 1.0
	}
	DataFlowStatus.WithLabelValues(namespace, name, phase).Set(status)
}

// formatIndex форматирует индекс как строку
func formatIndex(index int) string {
	return fmt.Sprintf("%d", index)
}

// RecordTaskStageDuration записывает время выполнения этапа задачи
func RecordTaskStageDuration(namespace, name, stage string, durationSeconds float64) {
	TaskStageDuration.WithLabelValues(namespace, name, stage).Observe(durationSeconds)
}

// RecordTaskMessageSize записывает размер сообщения на этапе обработки
func RecordTaskMessageSize(namespace, name, stage string, sizeBytes int) {
	TaskMessageSize.WithLabelValues(namespace, name, stage).Observe(float64(sizeBytes))
}

// RecordTaskStageLatency записывает задержку между этапами
func RecordTaskStageLatency(namespace, name, fromStage, toStage string, latencySeconds float64) {
	TaskStageLatency.WithLabelValues(namespace, name, fromStage, toStage).Observe(latencySeconds)
}

// SetTaskThroughput устанавливает текущую пропускную способность
func SetTaskThroughput(namespace, name string, messagesPerSecond float64) {
	TaskThroughput.WithLabelValues(namespace, name).Set(messagesPerSecond)
}

// SetTaskSuccessRate устанавливает процент успешных задач
func SetTaskSuccessRate(namespace, name string, rate float64) {
	TaskSuccessRate.WithLabelValues(namespace, name).Set(rate)
}

// RecordTaskEndToEndLatency записывает полное время жизни сообщения
func RecordTaskEndToEndLatency(namespace, name string, latencySeconds float64) {
	TaskEndToEndLatency.WithLabelValues(namespace, name).Observe(latencySeconds)
}

// SetTaskActiveMessages устанавливает количество активных сообщений
func SetTaskActiveMessages(namespace, name string, count int) {
	TaskActiveMessages.WithLabelValues(namespace, name).Set(float64(count))
}

// SetTaskQueueSize устанавливает размер очереди
func SetTaskQueueSize(namespace, name, queueType string, size int) {
	TaskQueueSize.WithLabelValues(namespace, name, queueType).Set(float64(size))
}

// RecordTaskQueueWaitTime записывает время ожидания в очереди
func RecordTaskQueueWaitTime(namespace, name, queueType string, waitTimeSeconds float64) {
	TaskQueueWaitTime.WithLabelValues(namespace, name, queueType).Observe(waitTimeSeconds)
}

// RecordTaskOperation записывает выполнение операции
func RecordTaskOperation(namespace, name, operation, status string) {
	TaskOperationsTotal.WithLabelValues(namespace, name, operation, status).Inc()
}

// RecordTaskStageError записывает ошибку на этапе обработки
func RecordTaskStageError(namespace, name, stage, errorType string) {
	TaskStageErrors.WithLabelValues(namespace, name, stage, errorType).Inc()
}
