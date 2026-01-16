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
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// DataFlowMessagesReceived - количество полученных сообщений по манифесту
	DataFlowMessagesReceived = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_messages_received_total",
			Help: "Total number of messages received from source by dataflow manifest",
		},
		[]string{"namespace", "name", "source_type"},
	)

	// DataFlowMessagesSent - количество отправленных сообщений по манифесту
	DataFlowMessagesSent = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_messages_sent_total",
			Help: "Total number of messages sent to sink by dataflow manifest",
		},
		[]string{"namespace", "name", "sink_type", "route"},
	)

	// DataFlowProcessingDuration - время обработки сообщений
	DataFlowProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dataflow_processing_duration_seconds",
			Help:    "Time spent processing messages",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 10), // от 1ms до ~1s
		},
		[]string{"namespace", "name"},
	)

	// ConnectorMessagesRead - количество прочитанных сообщений из source коннектора
	ConnectorMessagesRead = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_connector_messages_read_total",
			Help: "Total number of messages read from source connector",
		},
		[]string{"namespace", "name", "connector_type", "connector_name"},
	)

	// ConnectorMessagesWritten - количество записанных сообщений в sink коннектор
	ConnectorMessagesWritten = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_connector_messages_written_total",
			Help: "Total number of messages written to sink connector",
		},
		[]string{"namespace", "name", "connector_type", "connector_name", "route"},
	)

	// ConnectorErrors - количество ошибок в коннекторах
	ConnectorErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_connector_errors_total",
			Help: "Total number of errors in connectors",
		},
		[]string{"namespace", "name", "connector_type", "connector_name", "operation", "error_type"},
	)

	// ConnectorConnectionStatus - статус подключения коннектора
	ConnectorConnectionStatus = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dataflow_connector_connection_status",
			Help: "Connection status of connector (1 = connected, 0 = disconnected)",
		},
		[]string{"namespace", "name", "connector_type", "connector_name"},
	)

	// TransformerExecutions - количество выполнений трансформера
	TransformerExecutions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_transformer_executions_total",
			Help: "Total number of transformer executions",
		},
		[]string{"namespace", "name", "transformer_type", "transformer_index"},
	)

	// TransformerErrors - количество ошибок в трансформерах
	TransformerErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_transformer_errors_total",
			Help: "Total number of errors in transformers",
		},
		[]string{"namespace", "name", "transformer_type", "transformer_index", "error_type"},
	)

	// TransformerDuration - время выполнения трансформера
	TransformerDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dataflow_transformer_duration_seconds",
			Help:    "Time spent executing transformer",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 12), // от 0.1ms до ~400ms
		},
		[]string{"namespace", "name", "transformer_type", "transformer_index"},
	)

	// TransformerMessagesIn - количество входящих сообщений в трансформер
	TransformerMessagesIn = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_transformer_messages_in_total",
			Help: "Total number of messages input to transformer",
		},
		[]string{"namespace", "name", "transformer_type", "transformer_index"},
	)

	// TransformerMessagesOut - количество исходящих сообщений из трансформера
	TransformerMessagesOut = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dataflow_transformer_messages_out_total",
			Help: "Total number of messages output from transformer",
		},
		[]string{"namespace", "name", "transformer_type", "transformer_index"},
	)

	// DataFlowStatus - статус DataFlow манифеста
	DataFlowStatus = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dataflow_status",
			Help: "Status of DataFlow manifest (1 = Running, 0 = Stopped/Error)",
		},
		[]string{"namespace", "name", "phase"},
	)
)

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
