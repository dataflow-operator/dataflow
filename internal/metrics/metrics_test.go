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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

func TestRecordMessageReceived(t *testing.T) {
	RecordMessageReceived("test-ns", "test-name", "kafka")

	// Проверяем, что метрика была записана
	metric, err := DataFlowMessagesReceived.GetMetricWithLabelValues("test-ns", "test-name", "kafka")
	if err != nil {
		t.Fatalf("Failed to get metric: %v", err)
	}

	var dtoMetric dto.Metric
	if err := metric.Write(&dtoMetric); err != nil {
		t.Fatalf("Failed to write metric: %v", err)
	}

	if dtoMetric.Counter == nil {
		t.Fatal("Counter is nil")
	}

	if *dtoMetric.Counter.Value != 1.0 {
		t.Errorf("Expected counter value 1.0, got %f", *dtoMetric.Counter.Value)
	}
}

func TestRecordMessageSent(t *testing.T) {
	RecordMessageSent("test-ns", "test-name", "kafka", "default")

	metric, err := DataFlowMessagesSent.GetMetricWithLabelValues("test-ns", "test-name", "kafka", "default")
	if err != nil {
		t.Fatalf("Failed to get metric: %v", err)
	}

	var dtoMetric dto.Metric
	if err := metric.Write(&dtoMetric); err != nil {
		t.Fatalf("Failed to write metric: %v", err)
	}

	if dtoMetric.Counter == nil {
		t.Fatal("Counter is nil")
	}

	if *dtoMetric.Counter.Value != 1.0 {
		t.Errorf("Expected counter value 1.0, got %f", *dtoMetric.Counter.Value)
	}
}

func TestRecordConnectorError(t *testing.T) {
	RecordConnectorError("test-ns", "test-name", "kafka", "source", "connect", "connection_error")

	metric, err := ConnectorErrors.GetMetricWithLabelValues("test-ns", "test-name", "kafka", "source", "connect", "connection_error")
	if err != nil {
		t.Fatalf("Failed to get metric: %v", err)
	}

	var dtoMetric dto.Metric
	if err := metric.Write(&dtoMetric); err != nil {
		t.Fatalf("Failed to write metric: %v", err)
	}

	if dtoMetric.Counter == nil {
		t.Fatal("Counter is nil")
	}

	if *dtoMetric.Counter.Value != 1.0 {
		t.Errorf("Expected counter value 1.0, got %f", *dtoMetric.Counter.Value)
	}
}

func TestSetConnectorConnectionStatus(t *testing.T) {
	SetConnectorConnectionStatus("test-ns", "test-name", "kafka", "source", true)

	metric, err := ConnectorConnectionStatus.GetMetricWithLabelValues("test-ns", "test-name", "kafka", "source")
	if err != nil {
		t.Fatalf("Failed to get metric: %v", err)
	}

	var dtoMetric dto.Metric
	if err := metric.Write(&dtoMetric); err != nil {
		t.Fatalf("Failed to write metric: %v", err)
	}

	if dtoMetric.Gauge == nil {
		t.Fatal("Gauge is nil")
	}

	if *dtoMetric.Gauge.Value != 1.0 {
		t.Errorf("Expected gauge value 1.0, got %f", *dtoMetric.Gauge.Value)
	}

	// Проверяем отключение
	SetConnectorConnectionStatus("test-ns", "test-name", "kafka", "source", false)

	if err := metric.Write(&dtoMetric); err != nil {
		t.Fatalf("Failed to write metric: %v", err)
	}

	if *dtoMetric.Gauge.Value != 0.0 {
		t.Errorf("Expected gauge value 0.0, got %f", *dtoMetric.Gauge.Value)
	}
}

func TestRecordTransformerExecution(t *testing.T) {
	RecordTransformerExecution("test-ns", "test-name", "filter", 0)

	metric, err := TransformerExecutions.GetMetricWithLabelValues("test-ns", "test-name", "filter", "0")
	if err != nil {
		t.Fatalf("Failed to get metric: %v", err)
	}

	var dtoMetric dto.Metric
	if err := metric.Write(&dtoMetric); err != nil {
		t.Fatalf("Failed to write metric: %v", err)
	}

	if dtoMetric.Counter == nil {
		t.Fatal("Counter is nil")
	}

	if *dtoMetric.Counter.Value != 1.0 {
		t.Errorf("Expected counter value 1.0, got %f", *dtoMetric.Counter.Value)
	}
}

func TestSetDataFlowStatus(t *testing.T) {
	SetDataFlowStatus("test-ns", "test-name", "Running")

	metric, err := DataFlowStatus.GetMetricWithLabelValues("test-ns", "test-name", "Running")
	if err != nil {
		t.Fatalf("Failed to get metric: %v", err)
	}

	var dtoMetric dto.Metric
	if err := metric.Write(&dtoMetric); err != nil {
		t.Fatalf("Failed to write metric: %v", err)
	}

	if dtoMetric.Gauge == nil {
		t.Fatal("Gauge is nil")
	}

	if *dtoMetric.Gauge.Value != 1.0 {
		t.Errorf("Expected gauge value 1.0 for Running, got %f", *dtoMetric.Gauge.Value)
	}

	// Проверяем статус Error
	SetDataFlowStatus("test-ns", "test-name", "Error")

	metric, err = DataFlowStatus.GetMetricWithLabelValues("test-ns", "test-name", "Error")
	if err != nil {
		t.Fatalf("Failed to get metric: %v", err)
	}

	if err := metric.Write(&dtoMetric); err != nil {
		t.Fatalf("Failed to write metric: %v", err)
	}

	if *dtoMetric.Gauge.Value != 0.0 {
		t.Errorf("Expected gauge value 0.0 for Error, got %f", *dtoMetric.Gauge.Value)
	}
}

func TestFormatIndex(t *testing.T) {
	tests := []struct {
		input    int
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{10, "10"},
		{100, "100"},
	}

	for _, tt := range tests {
		result := formatIndex(tt.input)
		if result != tt.expected {
			t.Errorf("formatIndex(%d) = %s, expected %s", tt.input, result, tt.expected)
		}
	}
}

func TestRecordTaskStageDuration(t *testing.T) {
	// Записываем метрику (она уже зарегистрирована в глобальном реестре через init())
	RecordTaskStageDuration("test-ns", "test-name", "read", 0.1)

	// Используем глобальный registry через controller-runtime metrics
	gatheredMetrics, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	var found bool
	var hasCorrectLabels bool
	var hasSampleCount bool

	for _, mf := range gatheredMetrics {
		if mf.GetName() == "dataflow_task_stage_duration_seconds" {
			found = true
			for _, metric := range mf.Metric {
				// Проверяем лейблы по имени, а не по индексу
				nsMatch := false
				nameMatch := false
				stageMatch := false
				for _, label := range metric.Label {
					switch label.GetName() {
					case "namespace":
						nsMatch = label.GetValue() == "test-ns"
					case "name":
						nameMatch = label.GetValue() == "test-name"
					case "stage":
						stageMatch = label.GetValue() == "read"
					}
				}

				if nsMatch && nameMatch && stageMatch {
					hasCorrectLabels = true
					if metric.Histogram != nil && *metric.Histogram.SampleCount >= 1 {
						hasSampleCount = true
						return // Успешно
					}
				}
			}
		}
	}

	if !found {
		t.Fatal("Metric dataflow_task_stage_duration_seconds not found")
	}
	if !hasCorrectLabels {
		t.Error("Metric with expected labels not found")
	}
	if !hasSampleCount {
		t.Error("Metric histogram has no sample count")
	}
}

func TestRecordTaskMessageSize(t *testing.T) {
	RecordTaskMessageSize("test-ns", "test-name", "input", 1024)

	registry := prometheus.NewRegistry()
	registry.MustRegister(TaskMessageSize)

	metrics, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	var found bool
	for _, mf := range metrics {
		if mf.GetName() == "dataflow_task_message_size_bytes" {
			found = true
			for _, metric := range mf.Metric {
				if len(metric.Label) >= 3 &&
					metric.Label[0].GetValue() == "test-ns" &&
					metric.Label[1].GetValue() == "test-name" &&
					metric.Label[2].GetValue() == "input" {
					if metric.Histogram != nil && *metric.Histogram.SampleCount >= 1 {
						return // Успешно
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("Metric dataflow_task_message_size_bytes not found")
	}
}

func TestRecordTaskStageLatency(t *testing.T) {
	RecordTaskStageLatency("test-ns", "test-name", "read", "transform", 0.05)

	registry := prometheus.NewRegistry()
	registry.MustRegister(TaskStageLatency)

	metrics, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	var found bool
	for _, mf := range metrics {
		if mf.GetName() == "dataflow_task_stage_latency_seconds" {
			found = true
			for _, metric := range mf.Metric {
				if len(metric.Label) >= 4 &&
					metric.Label[0].GetValue() == "test-ns" &&
					metric.Label[1].GetValue() == "test-name" &&
					metric.Label[2].GetValue() == "read" &&
					metric.Label[3].GetValue() == "transform" {
					if metric.Histogram != nil && *metric.Histogram.SampleCount >= 1 {
						return // Успешно
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("Metric dataflow_task_stage_latency_seconds not found")
	}
}

func TestSetTaskThroughput(t *testing.T) {
	SetTaskThroughput("test-ns", "test-name", 100.5)

	metric, err := TaskThroughput.GetMetricWithLabelValues("test-ns", "test-name")
	if err != nil {
		t.Fatalf("Failed to get metric: %v", err)
	}

	var dtoMetric dto.Metric
	if err := metric.Write(&dtoMetric); err != nil {
		t.Fatalf("Failed to write metric: %v", err)
	}

	if dtoMetric.Gauge == nil {
		t.Fatal("Gauge is nil")
	}

	if *dtoMetric.Gauge.Value != 100.5 {
		t.Errorf("Expected gauge value 100.5, got %f", *dtoMetric.Gauge.Value)
	}
}

func TestSetTaskSuccessRate(t *testing.T) {
	SetTaskSuccessRate("test-ns", "test-name", 0.95)

	metric, err := TaskSuccessRate.GetMetricWithLabelValues("test-ns", "test-name")
	if err != nil {
		t.Fatalf("Failed to get metric: %v", err)
	}

	var dtoMetric dto.Metric
	if err := metric.Write(&dtoMetric); err != nil {
		t.Fatalf("Failed to write metric: %v", err)
	}

	if dtoMetric.Gauge == nil {
		t.Fatal("Gauge is nil")
	}

	if *dtoMetric.Gauge.Value != 0.95 {
		t.Errorf("Expected gauge value 0.95, got %f", *dtoMetric.Gauge.Value)
	}
}

func TestRecordTaskEndToEndLatency(t *testing.T) {
	RecordTaskEndToEndLatency("test-ns", "test-name", 0.2)

	registry := prometheus.NewRegistry()
	registry.MustRegister(TaskEndToEndLatency)

	metrics, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	var found bool
	for _, mf := range metrics {
		if mf.GetName() == "dataflow_task_end_to_end_latency_seconds" {
			found = true
			for _, metric := range mf.Metric {
				if len(metric.Label) >= 2 &&
					metric.Label[0].GetValue() == "test-ns" &&
					metric.Label[1].GetValue() == "test-name" {
					if metric.Histogram != nil && *metric.Histogram.SampleCount >= 1 {
						return // Успешно
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("Metric dataflow_task_end_to_end_latency_seconds not found")
	}
}

func TestSetTaskActiveMessages(t *testing.T) {
	SetTaskActiveMessages("test-ns", "test-name", 5)

	metric, err := TaskActiveMessages.GetMetricWithLabelValues("test-ns", "test-name")
	if err != nil {
		t.Fatalf("Failed to get metric: %v", err)
	}

	var dtoMetric dto.Metric
	if err := metric.Write(&dtoMetric); err != nil {
		t.Fatalf("Failed to write metric: %v", err)
	}

	if dtoMetric.Gauge == nil {
		t.Fatal("Gauge is nil")
	}

	if *dtoMetric.Gauge.Value != 5.0 {
		t.Errorf("Expected gauge value 5.0, got %f", *dtoMetric.Gauge.Value)
	}
}

func TestSetTaskQueueSize(t *testing.T) {
	SetTaskQueueSize("test-ns", "test-name", "output", 10)

	metric, err := TaskQueueSize.GetMetricWithLabelValues("test-ns", "test-name", "output")
	if err != nil {
		t.Fatalf("Failed to get metric: %v", err)
	}

	var dtoMetric dto.Metric
	if err := metric.Write(&dtoMetric); err != nil {
		t.Fatalf("Failed to write metric: %v", err)
	}

	if dtoMetric.Gauge == nil {
		t.Fatal("Gauge is nil")
	}

	if *dtoMetric.Gauge.Value != 10.0 {
		t.Errorf("Expected gauge value 10.0, got %f", *dtoMetric.Gauge.Value)
	}
}

func TestRecordTaskQueueWaitTime(t *testing.T) {
	RecordTaskQueueWaitTime("test-ns", "test-name", "output", 0.01)

	registry := prometheus.NewRegistry()
	registry.MustRegister(TaskQueueWaitTime)

	metrics, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	var found bool
	for _, mf := range metrics {
		if mf.GetName() == "dataflow_task_queue_wait_time_seconds" {
			found = true
			for _, metric := range mf.Metric {
				if len(metric.Label) >= 3 &&
					metric.Label[0].GetValue() == "test-ns" &&
					metric.Label[1].GetValue() == "test-name" &&
					metric.Label[2].GetValue() == "output" {
					if metric.Histogram != nil && *metric.Histogram.SampleCount >= 1 {
						return // Успешно
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("Metric dataflow_task_queue_wait_time_seconds not found")
	}
}

func TestRecordTaskOperation(t *testing.T) {
	RecordTaskOperation("test-ns", "test-name", "transform", "success")

	metric, err := TaskOperationsTotal.GetMetricWithLabelValues("test-ns", "test-name", "transform", "success")
	if err != nil {
		t.Fatalf("Failed to get metric: %v", err)
	}

	var dtoMetric dto.Metric
	if err := metric.Write(&dtoMetric); err != nil {
		t.Fatalf("Failed to write metric: %v", err)
	}

	if dtoMetric.Counter == nil {
		t.Fatal("Counter is nil")
	}

	if *dtoMetric.Counter.Value != 1.0 {
		t.Errorf("Expected counter value 1.0, got %f", *dtoMetric.Counter.Value)
	}
}

func TestRecordTaskStageError(t *testing.T) {
	RecordTaskStageError("test-ns", "test-name", "transform", "transformation_error")

	metric, err := TaskStageErrors.GetMetricWithLabelValues("test-ns", "test-name", "transform", "transformation_error")
	if err != nil {
		t.Fatalf("Failed to get metric: %v", err)
	}

	var dtoMetric dto.Metric
	if err := metric.Write(&dtoMetric); err != nil {
		t.Fatalf("Failed to write metric: %v", err)
	}

	if dtoMetric.Counter == nil {
		t.Fatal("Counter is nil")
	}

	if *dtoMetric.Counter.Value != 1.0 {
		t.Errorf("Expected counter value 1.0, got %f", *dtoMetric.Counter.Value)
	}
}
