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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
)

const (
	// ProcessorMetricsPort is the port where processor pods expose /metrics.
	ProcessorMetricsPort = 9090
	// ScrapeTimeout is the HTTP timeout for scraping each processor pod.
	ScrapeTimeout = 10 * time.Second
)

// Scraper scrapes metrics from processor pods and merges them with operator metrics.
type Scraper struct {
	client client.Client
}

// NewScraper creates a new metrics scraper.
func NewScraper(c client.Client) *Scraper {
	return &Scraper{client: c}
}

// ScrapeProcessorPods discovers processor pods (via DataFlow resources and pod labels),
// fetches /metrics from each, and returns a map of "namespace/name" -> metrics text.
// Only dataflow_* metrics are included from each processor to avoid duplicate go_*, process_* etc.
func (s *Scraper) ScrapeProcessorPods(ctx context.Context) (map[string][]byte, error) {
	var dfList dataflowv1.DataFlowList
	if err := s.client.List(ctx, &dfList); err != nil {
		return nil, fmt.Errorf("list dataflows: %w", err)
	}

	result := make(map[string][]byte)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range dfList.Items {
		df := &dfList.Items[i]
		key := df.Namespace + "/" + df.Name
		wg.Add(1)
		go func(ns, n, k string) {
			defer wg.Done()
			metrics, err := s.scrapePodForDataFlow(ctx, ns, n)
			if err != nil {
				return
			}
			if len(metrics) > 0 {
				mu.Lock()
				result[k] = metrics
				mu.Unlock()
			}
		}(df.Namespace, df.Name, key)
	}
	wg.Wait()
	return result, nil
}

// scrapePodForDataFlow finds the processor pod for the given DataFlow and fetches its metrics.
func (s *Scraper) scrapePodForDataFlow(ctx context.Context, namespace, name string) ([]byte, error) {
	var podList corev1.PodList
	err := s.client.List(ctx, &podList,
		client.InNamespace(namespace),
		client.MatchingLabels{
			"app":                       "dataflow-processor",
			"dataflow.dataflow.io/name": name,
		},
	)
	if err != nil {
		return nil, err
	}
	if len(podList.Items) == 0 {
		return nil, fmt.Errorf("no pod found for %s/%s", namespace, name)
	}
	pod := podList.Items[0]
	if pod.Status.PodIP == "" {
		return nil, fmt.Errorf("pod %s/%s has no IP", namespace, pod.Name)
	}
	url := fmt.Sprintf("http://%s:%d/metrics", pod.Status.PodIP, ProcessorMetricsPort)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: ScrapeTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics returned %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return filterDataflowMetrics(body), nil
}

// filterDataflowMetrics returns only lines for dataflow_* metrics (including HELP and TYPE).
func filterDataflowMetrics(data []byte) []byte {
	var out bytes.Buffer
	scanner := bufio.NewScanner(bytes.NewReader(data))
	const maxLineSize = 64 * 1024
	buf := make([]byte, 0, maxLineSize)
	scanner.Buffer(buf, maxLineSize)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			if strings.Contains(line, "dataflow_") {
				fmt.Fprintln(&out, line)
			}
			continue
		}
		if strings.HasPrefix(line, "dataflow_") {
			fmt.Fprintln(&out, line)
		}
	}
	return out.Bytes()
}

// MergeWithOperatorMetrics concatenates operator metrics with processor metrics.
// Operator metrics are passed as-is. Processor metrics (only dataflow_*) are appended.
func MergeWithOperatorMetrics(operatorMetrics []byte, processorMetrics map[string][]byte) []byte {
	var out bytes.Buffer
	out.Write(operatorMetrics)
	if len(operatorMetrics) > 0 && !bytes.HasSuffix(operatorMetrics, []byte("\n")) {
		out.WriteByte('\n')
	}
	for _, pm := range processorMetrics {
		if len(pm) > 0 {
			out.Write(pm)
			if !bytes.HasSuffix(pm, []byte("\n")) {
				out.WriteByte('\n')
			}
		}
	}
	return out.Bytes()
}

// ListProcessorPods returns pods that run dataflow processors (for testing or discovery).
func ListProcessorPods(ctx context.Context, c client.Client, namespace string) ([]corev1.Pod, error) {
	list := &corev1.PodList{}
	opts := []client.ListOption{
		client.MatchingLabels{"app": "dataflow-processor"},
	}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if err := c.List(ctx, list, opts...); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// NewMetricsFilter returns a filter function that wraps the default metrics handler to aggregate
// operator metrics with processor pod metrics. The returned function matches
// sigs.k8s.io/controller-runtime/pkg/metrics/server.Filter.
func NewMetricsFilter(scraper *Scraper) func(log logr.Logger, handler http.Handler) (http.Handler, error) {
	return func(log logr.Logger, handler http.Handler) (http.Handler, error) {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// 1. Get operator metrics by invoking the default handler
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, r)
			operatorMetrics := rec.Body.Bytes()

			// 2. Scrape processor pods
			procMetrics, err := scraper.ScrapeProcessorPods(r.Context())
			if err != nil {
				log.Error(err, "Failed to scrape processor pods, returning operator metrics only")
			}

			// 3. Merge and write to response
			merged := MergeWithOperatorMetrics(operatorMetrics, procMetrics)
			w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
			w.WriteHeader(rec.Code)
			w.Write(merged)
		}), nil
	}
}
