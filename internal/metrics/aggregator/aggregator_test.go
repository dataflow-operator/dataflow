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
	"compress/gzip"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
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

// TestMetricsFilter_NoGzip verifies that the filter strips Accept-Encoding so Prometheus
// receives plain text (gzip causes "expected a valid start token, got \"\x1f\"" error).
func TestMetricsFilter_NoGzip(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = dataflowv1.AddToScheme(scheme)
	_ = clientgoscheme.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	scraper := NewScraper(fakeClient)

	// Handler that returns gzip when Accept-Encoding: gzip is present
	gzipHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		plain := "# HELP dataflow_status Status\n# TYPE dataflow_status gauge\ndataflow_status 1\n"
		if r.Header.Get("Accept-Encoding") == "gzip" {
			w.Header().Set("Content-Encoding", "gzip")
			gz := gzip.NewWriter(w)
			_, _ = gz.Write([]byte(plain))
			_ = gz.Close()
		} else {
			w.Write([]byte(plain))
		}
	})

	filterFn := NewMetricsFilter(scraper)
	wrapped, err := filterFn(logr.Discard(), gzipHandler)
	if err != nil {
		t.Fatalf("NewMetricsFilter: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	body := rec.Body.Bytes()
	if len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b {
		t.Error("response is gzip-compressed; Prometheus expects plain text")
	}
	if !strings.Contains(string(body), "dataflow_status") {
		t.Errorf("response should contain dataflow_status, got %q", string(body))
	}
}
