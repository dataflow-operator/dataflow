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
	"os"
	"os/signal"
	"strings"
	"syscall"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	ctrl "sigs.k8s.io/controller-runtime"
	zaprctrl "sigs.k8s.io/controller-runtime/pkg/log/zap"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/logkeys"
	"github.com/dataflow-operator/dataflow/internal/processor"
)

func main() {
	var specPath string
	var namespace string
	var name string
	flag.StringVar(&specPath, "spec-path", "/etc/dataflow/spec.json", "Path to DataFlow spec JSON file")
	flag.StringVar(&namespace, "namespace", "", "Namespace of the DataFlow resource")
	flag.StringVar(&name, "name", "", "Name of the DataFlow resource")
	opts := zaprctrl.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	// Уровень логирования: переменная окружения LOG_LEVEL (debug, info, warn, error) или флаги
	levelEnabler := processorLevelFromEnv(os.Getenv("LOG_LEVEL"), opts.Level)
	zapOpts := []zaprctrl.Opts{zaprctrl.UseFlagOptions(&opts)}
	if levelEnabler != nil {
		zapOpts = append(zapOpts, zaprctrl.Level(levelEnabler))
	}
	ctrl.SetLogger(zaprctrl.New(zapOpts...))
	logger := ctrl.Log.WithName("processor").WithValues(logkeys.DataflowNamespace, namespace, logkeys.DataflowName, name)

	// Читаем spec из файла
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

	// Создаем процессор
	proc, err := processor.NewProcessorWithLoggerAndMetadata(&spec, logger, namespace, name)
	if err != nil {
		logger.Error(err, "Failed to create processor")
		os.Exit(1)
	}

	// Создаем контекст с обработкой сигналов
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка сигналов для graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Запускаем процессор в горутине
	errChan := make(chan error, 1)
	go func() {
		logger.Info("Starting processor")
		errChan <- proc.Start(ctx)
	}()

	// Ждем сигнала или ошибки
	select {
	case sig := <-sigChan:
		logger.Info("Received signal, shutting down", "signal", sig)
		cancel()
		// Ждем завершения процессора
		if err := <-errChan; err != nil {
			logger.Error(err, "Processor exited with error")
			os.Exit(1)
		}
	case err := <-errChan:
		if err != nil {
			logger.Error(err, "Processor error")
			os.Exit(1)
		}
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
