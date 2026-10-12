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
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/controller"
)

// kubeClientFactory builds a Kubernetes client for SecretRef resolution.
// Tests replace it to avoid a real cluster.
var kubeClientFactory = defaultKubeClient

func defaultKubeClient() (client.Client, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		cfg, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
		if err != nil {
			return nil, fmt.Errorf("failed to build kube config: %w", err)
		}
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	return client.New(cfg, client.Options{Scheme: scheme})
}

func resolveSecrets(ctx context.Context, namespace string, spec *dataflowv1.DataFlowSpec) (*dataflowv1.DataFlowSpec, error) {
	if !specHasSecretRef(spec) {
		return spec, nil
	}
	c, err := kubeClientFactory()
	if err != nil {
		return nil, fmt.Errorf("secret reference requires kubeconfig: %w", err)
	}
	return resolveWithClient(ctx, c, namespace, spec)
}

func resolveWithClient(ctx context.Context, c client.Client, namespace string, spec *dataflowv1.DataFlowSpec) (*dataflowv1.DataFlowSpec, error) {
	resolved, err := controller.NewSecretResolver(c).ResolveDataFlowSpec(ctx, namespace, spec)
	if err != nil {
		return nil, err
	}
	return resolved, nil
}

func specHasSecretRef(spec *dataflowv1.DataFlowSpec) bool {
	if spec == nil {
		return false
	}
	b, err := json.Marshal(spec)
	if err != nil {
		return true
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return true
	}
	return jsonHasSecretRef(v)
}

func jsonHasSecretRef(v any) bool {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if strings.HasSuffix(k, "SecretRef") && val != nil {
				return true
			}
			if jsonHasSecretRef(val) {
				return true
			}
		}
	case []any:
		for _, item := range t {
			if jsonHasSecretRef(item) {
				return true
			}
		}
	}
	return false
}
