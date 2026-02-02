/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gui

import (
	"os"
	"strings"
	"testing"
)

// TestNewServer_InClusterConfigFailure проверяет, что при пустом kubeconfig
// и недоступном in-cluster config ошибка содержит понятный текст (RBAC, token и т.д.).
// Пропускается вне кластера (нет KUBERNETES_SERVICE_HOST), т.к. InClusterConfig() может блокироваться.
func TestNewServer_InClusterConfigFailure(t *testing.T) {
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		t.Skip("running in cluster, skipping in-cluster config failure test")
	}
	_, err := NewServer(":0", "")
	if err == nil {
		t.Fatal("expected error when kubeconfig is empty and not in cluster")
	}
	msg := err.Error()
	if !strings.Contains(msg, "in-cluster") {
		t.Errorf("error should mention in-cluster config for easier debugging, got: %s", msg)
	}
}
