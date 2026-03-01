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
}

// Run runs the dataflow pipeline in-process. It creates a processor from spec and runs it until ctx is cancelled.
// It does not require Kubernetes; the pipeline runs entirely inside the current process.
func Run(ctx context.Context, spec *dataflowv1.DataFlowSpec, opts RunOptions) error {
	if spec == nil {
		return fmt.Errorf("spec is required")
	}
	var logger logr.Logger
	if opts.Logger != nil {
		logger = *opts.Logger
	}
	if opts.Logger == nil {
		// Default: zap development logger to stderr
		level := zapcore.InfoLevel
		if s := strings.TrimSpace(strings.ToLower(opts.LogLevel)); s != "" {
			_ = level.UnmarshalText([]byte(s))
		}
		zapOpts := []zapr.Opts{zapr.Level(level), zapr.UseDevMode(true)}
		logger = zapr.New(zapOpts...).WithName("run").WithValues(
			logkeys.DataflowNamespace, opts.Namespace,
			logkeys.DataflowName, opts.Name,
		)
	} else if opts.Namespace != "" || opts.Name != "" {
		logger = logger.WithValues(logkeys.DataflowNamespace, opts.Namespace, logkeys.DataflowName, opts.Name)
	}

	proc, err := processor.NewProcessorWithLoggerAndMetadata(spec, logger, opts.Namespace, opts.Name)
	if err != nil {
		return err
	}

	return proc.Start(ctx)
}
