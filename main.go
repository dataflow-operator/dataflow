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
	"flag"
	"os"
	"strings"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	zaprctrl "sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/controller"
	_ "github.com/dataflow-operator/dataflow/internal/metrics" // Импортируем для регистрации метрик
	"github.com/dataflow-operator/dataflow/internal/webhookenv"
	//+kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(dataflowv1.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

// levelFromEnvOrOptions returns zap LevelEnabler from LOG_LEVEL env (debug, info, warn, error) if set,
// otherwise returns optsLevel (e.g. from --zap-log-level).
func levelFromEnvOrOptions(envLevel string, optsLevel zapcore.LevelEnabler) zapcore.LevelEnabler {
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

func main() {
	var metricsAddr string
	var probeAddr string
	var logFile string
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":9090", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":9091", "The address the probe endpoint binds to.")
	flag.StringVar(&logFile, "log-file", "", "Path to log file. If empty, logs will be written to stdout.")
	opts := zaprctrl.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	// Уровень логирования: приоритет у переменной окружения LOG_LEVEL (debug, info, warn, error)
	levelEnabler := levelFromEnvOrOptions(os.Getenv("LOG_LEVEL"), opts.Level)

	// Настройка логгера с возможностью записи в файл
	if logFile != "" {
		// Создаем файл для записи логов
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			// Используем временный логгер для вывода ошибки
			tempLogger := zaprctrl.New(zaprctrl.UseFlagOptions(&opts))
			ctrl.SetLogger(tempLogger)
			setupLog := ctrl.Log.WithName("setup")
			setupLog.Error(err, "unable to open log file", "file", logFile)
			os.Exit(1)
		}
		defer file.Close()

		// Настраиваем zap для записи в файл
		config := zap.NewDevelopmentConfig()
		if opts.Development {
			config.Development = true
		}
		config.EncoderConfig = zap.NewDevelopmentEncoderConfig()

		// Создаем core, который пишет в файл
		core := zapcore.NewCore(
			zapcore.NewConsoleEncoder(config.EncoderConfig),
			zapcore.AddSync(file),
			levelEnabler,
		)

		// Создаем logger с этим core
		zapLogger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
		if opts.Development {
			zapLogger = zapLogger.WithOptions(zap.Development())
		}

		// Обертываем zap logger в logr.Logger через zapr
		ctrl.SetLogger(zapr.NewLogger(zapLogger))
	} else {
		// Используем стандартную настройку: LOG_LEVEL или флаги
		zapOpts := []zaprctrl.Opts{zaprctrl.UseFlagOptions(&opts)}
		if levelEnabler != nil {
			zapOpts = append(zapOpts, zaprctrl.Level(levelEnabler))
		}
		ctrl.SetLogger(zaprctrl.New(zapOpts...))
	}

	// Webhook server включается только при заданном WEBHOOK_CERT_DIR (в e2e и при отключённом webhook в Helm сертификаты не монтируются).
	certDir := webhookenv.GetWebhookCertDir()
	mgrOpts := ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         true, // Always HA-ready: only one active controller across replicas
		LeaderElectionID:       "dataflow-operator.dataflow.io",
	}
	if certDir != "" {
		mgrOpts.WebhookServer = webhook.NewServer(webhook.Options{Port: 9443, CertDir: certDir})
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), mgrOpts)
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err = controller.NewDataFlowReconciler(mgr.GetClient(), mgr.GetScheme(), mgr.GetEventRecorderFor("dataflow-controller")).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "DataFlow")
		os.Exit(1)
	}
	if certDir != "" {
		validator := admission.WithCustomValidator(mgr.GetScheme(), &dataflowv1.DataFlow{}, &dataflowv1.DataFlow{})
		mgr.GetWebhookServer().Register("/validate-dataflow-dataflow-io-v1-dataflow", validator)
	} else {
		setupLog.Info("webhook disabled (WEBHOOK_CERT_DIR not set), skipping validator registration")
	}
	//+kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
