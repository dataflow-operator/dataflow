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

package sentry

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
)

// Env keys for Sentry configuration.
const (
	EnvDSN              = "SENTRY_DSN"
	EnvEnvironment      = "SENTRY_ENVIRONMENT"
	EnvTracesSampleRate = "SENTRY_TRACES_SAMPLE_RATE"
	EnvDebug            = "SENTRY_DEBUG"
	EnvRelease          = "SENTRY_RELEASE"
)

// Init initializes Sentry from environment variables. If SENTRY_DSN is empty,
// Sentry is not initialized and nil is returned.
func Init() error {
	dsn := strings.TrimSpace(os.Getenv(EnvDSN))
	if dsn == "" {
		return nil
	}

	opts := sentry.ClientOptions{
		Dsn:           dsn,
		Environment:   os.Getenv(EnvEnvironment),
		Release:       os.Getenv(EnvRelease),
		Debug:         os.Getenv(EnvDebug) == "true" || os.Getenv(EnvDebug) == "1",
		EnableTracing: true,
	}
	if opts.Environment == "" {
		opts.Environment = "production"
	}

	if s := os.Getenv(EnvTracesSampleRate); s != "" {
		if rate, err := strconv.ParseFloat(s, 64); err == nil && rate >= 0 && rate <= 1 {
			opts.TracesSampleRate = rate
		} else {
			opts.TracesSampleRate = 0.1
		}
	} else {
		opts.TracesSampleRate = 0.1
	}

	return sentry.Init(opts)
}

// Flush flushes buffered Sentry events. Call before program exit.
func Flush() {
	sentry.Flush(2 * time.Second)
}
