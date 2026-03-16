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
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	zaprctrl "sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/controller"
	_ "github.com/dataflow-operator/dataflow/internal/metrics" // Import for metrics registration
	"github.com/dataflow-operator/dataflow/internal/metrics/aggregator"
	"github.com/dataflow-operator/dataflow/internal/sentry"
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

// leaderReadyCheck returns a healthz.Checker that succeeds only when electedCh is closed (this instance is the leader).
// Not used in readyz (all replicas report Ready to allow rolling updates); kept for tests and possible future use.
func leaderReadyCheck(electedCh <-chan struct{}) healthz.Checker {
	return func(_ *http.Request) error {
		select {
		case <-electedCh:
			return nil
		default:
			return fmt.Errorf("not the leader")
		}
	}
}

// leaderElectionDurationFromEnv parses envKey as seconds and returns duration; if unset or invalid, returns defaultDur.
func leaderElectionDurationFromEnv(envKey string, defaultDur time.Duration) time.Duration {
	s := strings.TrimSpace(os.Getenv(envKey))
	if s == "" {
		return defaultDur
	}
	sec, err := strconv.Atoi(s)
	if err != nil || sec <= 0 {
		return defaultDur
	}
	return time.Duration(sec) * time.Second
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

	if err := sentry.Init(); err != nil {
		setupLog.Error(err, "Sentry init failed")
	}
	defer sentry.Flush()

	// Log level: LOG_LEVEL env var takes priority (debug, info, warn, error)
	levelEnabler := levelFromEnvOrOptions(os.Getenv("LOG_LEVEL"), opts.Level)

	// Configure logger with optional file output
	if logFile != "" {
		// Create log file
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			// Use temporary logger to output error
			tempLogger := zaprctrl.New(zaprctrl.UseFlagOptions(&opts))
			ctrl.SetLogger(tempLogger)
			setupLog := ctrl.Log.WithName("setup")
			setupLog.Error(err, "unable to open log file", "file", logFile)
			os.Exit(1)
		}
		defer file.Close()

		// Configure zap for file output
		config := zap.NewDevelopmentConfig()
		config.EncoderConfig = zap.NewDevelopmentEncoderConfig()

		// Create core that writes to file
		core := zapcore.NewCore(
			zapcore.NewConsoleEncoder(config.EncoderConfig),
			zapcore.AddSync(file),
			levelEnabler,
		)

		// Create logger with this core
		zapLogger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
		zapLogger = zapLogger.WithOptions(zap.Development())

		// Wrap zap logger in logr.Logger via zapr
		ctrl.SetLogger(zapr.NewLogger(zapLogger))
	} else {
		// Use standard setup: LOG_LEVEL or flags
		zapOpts := []zaprctrl.Opts{zaprctrl.UseFlagOptions(&opts)}
		if levelEnabler != nil {
			zapOpts = append(zapOpts, zaprctrl.Level(levelEnabler))
		}
		ctrl.SetLogger(zaprctrl.New(zapOpts...))
	}

	// Webhook server is enabled only when WEBHOOK_CERT_DIR is set (in e2e and when webhook is disabled in Helm, certs are not mounted).
	certDir := webhookenv.GetWebhookCertDir()
	// Leader election: HTTP timeout for lease updates is RenewDeadline/2. Use larger defaults to avoid
	// "context deadline exceeded" when API server or network is slow (e.g. 60s/40s → 20s per request).
	leaseDuration := leaderElectionDurationFromEnv("LEADER_ELECTION_LEASE_DURATION_SECONDS", 60*time.Second)
	renewDeadline := leaderElectionDurationFromEnv("LEADER_ELECTION_RENEW_DEADLINE_SECONDS", 40*time.Second)
	if renewDeadline >= leaseDuration {
		renewDeadline = leaseDuration/2 + leaseDuration/4 // e.g. 45s if lease 60s
	}

	// Create a client for metrics aggregation (scraping processor pods). Used before manager exists.
	config := ctrl.GetConfigOrDie()
	metricsClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		setupLog.Error(err, "unable to create client for metrics aggregation")
		os.Exit(1)
	}

	metricsOpts := metricsserver.Options{
		BindAddress: metricsAddr,
		FilterProvider: func(_ *rest.Config, _ *http.Client) (metricsserver.Filter, error) {
			scraper := aggregator.NewScraper(metricsClient)
			return aggregator.NewMetricsFilter(scraper), nil
		},
	}

	mgrOpts := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsOpts,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         true, // Always HA-ready: only one active controller across replicas
		LeaderElectionID:       "dataflow-operator.dataflow.io",
		LeaseDuration:          ptr.To(leaseDuration),
		RenewDeadline:          ptr.To(renewDeadline),
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
	// readyz: only Ping — do not require leader, so non-leader replicas stay Ready during rolling updates
	// (otherwise new pod never becomes Ready while old leader holds the lease, causing update deadlock)
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
