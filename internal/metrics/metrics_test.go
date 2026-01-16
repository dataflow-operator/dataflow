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

	dto "github.com/prometheus/client_model/go"
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
