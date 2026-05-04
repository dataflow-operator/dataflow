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
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
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
	"github.com/dataflow-operator/dataflow/internal/sentry"
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
		configMapName := "dataflow-" + name + "-checkpoint"
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
	metricsServer := &http.Server{Addr: metricsPort, Handler: mux}
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error(err, "Metrics server exited")
		}
	}()

	// Signal handling for graceful shutdown
	sigChan := make(chan os.Signal, constants.DefaultSingleValueChannelBufferSize)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start processor in goroutine
	errChan := make(chan error, constants.DefaultSingleValueChannelBufferSize)
	go func() {
		logger.Info("Starting processor")
		errChan <- proc.Start(ctx)
	}()

	// Wait for signal or error
	var procErr error
	select {
	case sig := <-sigChan:
		logger.Info("Received signal, shutting down", "signal", sig)
		cancel()
		procErr = <-errChan
		if procErr != nil {
			logger.Error(procErr, "Processor exited with error")
		}
		// Flush checkpoint before exit
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := proc.FlushCheckpoint(flushCtx); err != nil {
			logger.Error(err, "Failed to flush checkpoint")
		}
		flushCancel()
	case procErr = <-errChan:
		if procErr != nil {
			logger.Error(procErr, "Processor error")
			os.Exit(1)
		}
	}

	if procErr != nil {
		os.Exit(1)
	}
	logger.Info("Processor stopped successfully")
}

// processorLevelFromEnv returns zap LevelEnabler from LOG_LEVEL env if set, otherwise optsLevel.
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
