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

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	ctrl "sigs.k8s.io/controller-runtime"
	zaprctrl "sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/checkpoint"
	"github.com/dataflow-operator/dataflow/internal/constants"
	"github.com/dataflow-operator/dataflow/internal/logkeys"
	_ "github.com/dataflow-operator/dataflow/internal/metrics" // Register metrics
	"github.com/dataflow-operator/dataflow/internal/processor"
	retryutil "github.com/dataflow-operator/dataflow/internal/retry"
	"github.com/dataflow-operator/dataflow/internal/sentry"
	"github.com/dataflow-operator/dataflow/pkg/k8snames"
)

func main() {
	var specPath string
	var namespace string
	var name string
	var metricsPort string
	flag.StringVar(&specPath, "spec-path", "/etc/dataflow/spec.json", "Path to DataFlow spec JSON file")
	flag.StringVar(&namespace, "namespace", "", "Namespace of the DataFlow resource")
	flag.StringVar(&name, "name", "", "Name of the DataFlow resource")
	flag.StringVar(&metricsPort, "metrics-port", ":9090", "Address for the metrics HTTP server")
	opts := zaprctrl.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	if err := sentry.Init(); err != nil {
		log.Printf("Sentry init failed: %v", err)
	}
	defer sentry.Flush()

	// Log level: LOG_LEVEL env var (debug, info, warn, error) or flags
	levelEnabler := processorLevelFromEnv(os.Getenv("LOG_LEVEL"), opts.Level)
	zapOpts := []zaprctrl.Opts{zaprctrl.UseFlagOptions(&opts)}
	if levelEnabler != nil {
		zapOpts = append(zapOpts, zaprctrl.Level(levelEnabler))
	}
	ctrl.SetLogger(zaprctrl.New(zapOpts...))
	logger := ctrl.Log.WithName("processor").WithValues(logkeys.DataflowNamespace, namespace, logkeys.DataflowName, name)

	// Read spec from file
	specData, err := os.ReadFile(specPath)
	if err != nil {
		logger.Error(err, "Failed to read spec file", "path", specPath)
		os.Exit(1)
	}

	var spec dataflowv1.DataFlowSpec
	if err := json.Unmarshal(specData, &spec); err != nil {
		logger.Error(err, "Failed to unmarshal spec")
		os.Exit(1)
	}

	// Setup checkpoint store if persistence is enabled
	var procOpts []processor.ProcessorOption
	// CheckpointPersistence defaults to true when nil
	if (spec.CheckpointPersistence == nil || *spec.CheckpointPersistence) && name != "" && namespace != "" {
		configMapName := k8snames.ProcessorCheckpointConfigMap(name)
		store, err := checkpoint.NewConfigMapStore(namespace, configMapName)
		if err != nil {
			logger.Error(err, "Failed to create checkpoint store, continuing without persistence")
		} else {
			ctx := context.Background()
			store.Start(ctx)
			defer store.Stop()
			procOpts = append(procOpts, processor.WithCheckpointStore(store))
		}
	}

	// Create processor
	proc, err := processor.NewProcessorWithOptions(&spec, logger, namespace, name, procOpts...)
	if err != nil {
		logger.Error(err, "Failed to create processor")
		os.Exit(1)
	}

	// Create context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start metrics HTTP server (must be before proc.Start so /metrics is available from the start)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if proc.Ready() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready\n"))
	})
	progressTimeout := progressTimeoutFromEnv(os.Getenv("PROCESSOR_PROGRESS_TIMEOUT_SECONDS"))
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		writeLivez(w, proc.Ready, proc.ProgressStale, progressTimeout)
	})
	metricsServer := &http.Server{Addr: metricsPort, Handler: mux}
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error(err, "Metrics server exited")
		}
	}()

	// Signal handling for graceful shutdown
	sigChan := make(chan os.Signal, constants.DefaultSingleValueChannelBufferSize)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	var procErr error
	var shutdownRequested atomic.Bool
	go func() {
		sig := <-sigChan
		shutdownRequested.Store(true)
		logger.Info("Received signal, shutting down", "signal", sig)
		cancel()
	}()

	maxRetries := processorSinkErrorMaxRetriesFromEnv(os.Getenv("PROCESSOR_SINK_ERROR_MAX_RETRIES"))
	backoff := 30 * time.Second
	const maxBackoff = 5 * time.Minute

	for attempt := 1; ; attempt++ {
		logger.Info("Starting processor", "attempt", attempt)
		procErr = proc.Start(ctx)
		if procErr == nil || errors.Is(procErr, context.Canceled) {
			break
		}
		if !isRetryableProcessorError(procErr) {
			logger.Error(procErr, "Processor error is not retryable")
			break
		}
		if maxRetries > 0 && attempt >= maxRetries {
			procErr = fmt.Errorf("processor retry limit reached (%d): %w", maxRetries, procErr)
			logger.Error(procErr, "Processor error")
			break
		}
		logger.Error(procErr, "Transient processor error, retrying", "backoff", backoff.String(), "attempt", attempt)
		select {
		case <-ctx.Done():
			break
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
		if ctx.Err() != nil {
			break
		}
	}

	if shutdownRequested.Load() {
		// Flush checkpoint before exit
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := proc.FlushCheckpoint(flushCtx); err != nil {
			logger.Error(err, "Failed to flush checkpoint")
		}
		flushCancel()
	}

	if procErr != nil && !errors.Is(procErr, context.Canceled) {
		os.Exit(1)
	}
	logger.Info("Processor stopped successfully")
}

// writeLivez implements GET /livez for Kubernetes liveness probes.
func writeLivez(w http.ResponseWriter, ready func() bool, progressStale func(time.Duration) bool, progressTimeout time.Duration) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if !ready() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready\n"))
		return
	}
	if progressStale(progressTimeout) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("stale: no pipeline progress\n"))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// processorLevelFromEnv returns zap LevelEnabler from LOG_LEVEL env if set, otherwise optsLevel.
// progressTimeoutFromEnv parses PROCESSOR_PROGRESS_TIMEOUT_SECONDS (0 or unset disables stale checks).
func progressTimeoutFromEnv(env string) time.Duration {
	s := strings.TrimSpace(env)
	if s == "" {
		return 10 * time.Minute
	}
	sec, err := strconv.Atoi(s)
	if err != nil || sec <= 0 {
		return 0
	}
	return time.Duration(sec) * time.Second
}

func processorLevelFromEnv(envLevel string, optsLevel zapcore.LevelEnabler) zapcore.LevelEnabler {
	s := strings.TrimSpace(strings.ToLower(envLevel))
	if s == "" {
		return optsLevel
	}
	var l zapcore.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return optsLevel
	}
	return zap.NewAtomicLevelAt(l)
}

func isRetryableProcessorError(err error) bool {
	if err == nil {
		return false
	}
	return retryutil.IsTimeoutError(err) || retryutil.IsTransientTrinoError(err)
}

func processorSinkErrorMaxRetriesFromEnv(env string) int {
	s := strings.TrimSpace(env)
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return 0
	}
	return n
}
