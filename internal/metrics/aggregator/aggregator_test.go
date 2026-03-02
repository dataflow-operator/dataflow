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

package aggregator

import (
	"strings"
	"testing"
)

func TestFilterDataflowMetrics(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name: "keeps dataflow metrics",
			input: `# HELP dataflow_messages_received_total Total messages
# TYPE dataflow_messages_received_total counter
dataflow_messages_received_total{namespace="default",name="flow1"} 42
`,
			expected: `# HELP dataflow_messages_received_total Total messages
# TYPE dataflow_messages_received_total counter
dataflow_messages_received_total{namespace="default",name="flow1"} 42
`,
		},
		{
			name: "filters out go_ metrics",
			input: `# HELP go_goroutines Number of goroutines
# TYPE go_goroutines gauge
go_goroutines 10
# HELP dataflow_status Status
# TYPE dataflow_status gauge
dataflow_status{namespace="ns",name="n"} 1
`,
			expected: `# HELP dataflow_status Status
# TYPE dataflow_status gauge
dataflow_status{namespace="ns",name="n"} 1
`,
		},
		{
			name:     "empty input",
			input:    "",
			expected: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(filterDataflowMetrics([]byte(tt.input)))
			if got != tt.expected {
				t.Errorf("filterDataflowMetrics() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestMergeWithOperatorMetrics(t *testing.T) {
	operator := []byte("# HELP dataflow_status Status\n# TYPE dataflow_status gauge\ndataflow_status{ns=\"a\"} 1\n")
	processor := map[string][]byte{
		"default/flow1": []byte("# HELP dataflow_messages_received_total Total\n# TYPE dataflow_messages_received_total counter\ndataflow_messages_received_total{namespace=\"default\",name=\"flow1\"} 10\n"),
	}
	merged := MergeWithOperatorMetrics(operator, processor)
	if len(merged) == 0 {
		t.Error("MergeWithOperatorMetrics returned empty")
	}
	if !strings.Contains(string(merged), "dataflow_status") {
		t.Error("merged should contain dataflow_status")
	}
	if !strings.Contains(string(merged), "dataflow_messages_received_total") {
		t.Error("merged should contain dataflow_messages_received_total")
	}
}

func TestMergeWithOperatorMetrics_EmptyProcessor(t *testing.T) {
	operator := []byte("dataflow_status{ns=\"a\"} 1\n")
	merged := MergeWithOperatorMetrics(operator, nil)
	if string(merged) != string(operator) {
		t.Errorf("MergeWithOperatorMetrics(nil) = %q, want %q", merged, operator)
	}
}
