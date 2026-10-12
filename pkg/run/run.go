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

package run

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	"go.uber.org/zap/zapcore"
	zapr "sigs.k8s.io/controller-runtime/pkg/log/zap"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/checkpoint"
	"github.com/dataflow-operator/dataflow/internal/logkeys"
	"github.com/dataflow-operator/dataflow/internal/processor"
)

// RunOptions configures Run behaviour.
type RunOptions struct {
	// Logger is the logr.Logger to use. If nil, a default zap-based console logger is used.
	Logger *logr.Logger
	// Namespace and Name are metadata passed to the processor (e.g. for metrics labels).
	Namespace string
	Name      string
	// LogLevel sets the minimum log level when using the default logger (e.g. "debug", "info", "warn", "error").
	LogLevel string
	// CheckpointPath is a local JSON file for source checkpoints.
	// Empty disables file persistence. Ignored when spec.checkpointPersistence is false.
	CheckpointPath string
	// ResetCheckpoint clears the source checkpoint in CheckpointPath before start.
	ResetCheckpoint bool
	// Once rejects sources that never close (kafka, postgresql-cdc) before connecting.
	Once bool
}

// RunResult is the outcome of a pipeline run.
type RunResult struct {
	Processed      int64
	Errors         int64
	CheckpointPath string
}

// Run runs the dataflow pipeline in-process until the source is exhausted or ctx is cancelled.
// It does not require Kubernetes for the pipeline itself. SecretRef values are read from the cluster.
func Run(ctx context.Context, spec *dataflowv1.DataFlowSpec, opts RunOptions) (RunResult, error) {
	if spec == nil {
		return RunResult{}, fmt.Errorf("spec is required")
	}
	if opts.Once && !SourceExhausts(spec.Source.Type) {
		return RunResult{}, onceUnsupportedError(spec.Source.Type)
	}

	logger := runLogger(opts)
	resolved, err := resolveSecrets(ctx, opts.Namespace, spec)
	if err != nil {
		return RunResult{}, err
	}
	spec = resolved

	var result RunResult
	store, cleanup, err := openCheckpoint(ctx, logger, spec, opts)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		return result, err
	}
	if store != nil {
		result.CheckpointPath = opts.CheckpointPath
	}

	procOpts := []processor.ProcessorOption{}
	if store != nil {
		procOpts = append(procOpts, processor.WithCheckpointStore(store))
	}
	proc, err := processor.NewProcessorWithOptions(spec, logger, opts.Namespace, opts.Name, procOpts...)
	if err != nil {
		return result, err
	}

	err = proc.Start(ctx)
	result.Processed, result.Errors = proc.GetStats()
	return result, err
}

func runLogger(opts RunOptions) logr.Logger {
	if opts.Logger != nil {
		logger := *opts.Logger
		if opts.Namespace != "" || opts.Name != "" {
			logger = logger.WithValues(logkeys.DataflowNamespace, opts.Namespace, logkeys.DataflowName, opts.Name)
		}
		return logger
	}
	level := zapcore.InfoLevel
	if s := strings.TrimSpace(strings.ToLower(opts.LogLevel)); s != "" {
		_ = level.UnmarshalText([]byte(s))
	}
	zapOpts := []zapr.Opts{zapr.Level(level), zapr.UseDevMode(true)}
	return zapr.New(zapOpts...).WithName("run").WithValues(
		logkeys.DataflowNamespace, opts.Namespace,
		logkeys.DataflowName, opts.Name,
	)
}

func openCheckpoint(ctx context.Context, logger logr.Logger, spec *dataflowv1.DataFlowSpec, opts RunOptions) (checkpoint.Store, func(), error) {
	if opts.CheckpointPath == "" {
		return nil, nil, nil
	}
	if !dataflowv1.CheckpointPersistenceEnabled(spec) {
		logger.Info("checkpointPersistence is false; local checkpoint file will not be used", "path", opts.CheckpointPath)
		return nil, nil, nil
	}
	fileStore, err := checkpoint.NewFileStore(opts.CheckpointPath)
	if err != nil {
		return nil, nil, err
	}
	if opts.ResetCheckpoint {
		logger.Info("Clearing local checkpoint", "path", opts.CheckpointPath, "sourceType", spec.Source.Type)
		if err := fileStore.Clear(ctx, spec.Source.Type); err != nil {
			return nil, nil, fmt.Errorf("reset checkpoint: %w", err)
		}
	}
	syncStore := checkpoint.NewSyncStore(
		fileStore,
		dataflowv1.CheckpointSyncOnAckEnabled(spec),
		dataflowv1.CheckpointSaveIntervalDuration(spec),
		checkpoint.WithFlushErrorHandler(func(err error) {
			logger.Error(err, "async checkpoint flush after ack failed")
		}),
	)
	cleanup := func() {
		flushCtx := context.Background()
		if err := syncStore.Flush(flushCtx); err != nil {
			logger.Error(err, "checkpoint flush failed", "path", opts.CheckpointPath)
		}
		syncStore.Stop()
	}
	return syncStore, cleanup, nil
}
