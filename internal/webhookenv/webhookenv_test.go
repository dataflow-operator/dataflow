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

package webhookenv

import (
	"os"
	"testing"
)

const envKey = "WEBHOOK_CERT_DIR"

func TestGetWebhookCertDir(t *testing.T) {
	restore := os.Getenv(envKey)
	defer func() {
		if restore == "" {
			os.Unsetenv(envKey)
		} else {
			_ = os.Setenv(envKey, restore)
		}
	}()

	t.Run("unset", func(t *testing.T) {
		os.Unsetenv(envKey)
		if got := GetWebhookCertDir(); got != "" {
			t.Errorf("GetWebhookCertDir() = %q, want empty", got)
		}
	})

	t.Run("set", func(t *testing.T) {
		want := "/tmp/k8s-webhook-server/serving-certs"
		_ = os.Setenv(envKey, want)
		if got := GetWebhookCertDir(); got != want {
			t.Errorf("GetWebhookCertDir() = %q, want %q", got, want)
		}
	})

	t.Run("trimmed", func(t *testing.T) {
		_ = os.Setenv(envKey, "  /path/to/certs  ")
		if got := GetWebhookCertDir(); got != "/path/to/certs" {
			t.Errorf("GetWebhookCertDir() = %q, want /path/to/certs", got)
		}
	})
}
