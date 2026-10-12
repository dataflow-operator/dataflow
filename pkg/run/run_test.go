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
	"os"
	"path/filepath"
	"testing"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRun_InvalidSpec_ReturnsError(t *testing.T) {
	ctx := context.Background()
	spec := &dataflowv1.DataFlowSpec{
		Source: dataflowv1.SourceSpec{Type: "unsupported"},
		Sink:   dataflowv1.SinkSpec{Type: "unsupported"},
	}

	_, err := Run(ctx, spec, RunOptions{Namespace: "ns", Name: "test"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported source type")
}

func TestRun_NilSpec_ReturnsError(t *testing.T) {
	ctx := context.Background()
	_, err := Run(ctx, nil, RunOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "spec is required")
}

func TestRun_EmptySpec_ReturnsError(t *testing.T) {
	ctx := context.Background()
	spec := &dataflowv1.DataFlowSpec{
		Source: dataflowv1.SourceSpec{Type: ""},
		Sink:   dataflowv1.SinkSpec{Type: ""},
	}

	_, err := Run(ctx, spec, RunOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported source type")
}

func TestRun_OnceRejectsStreamingSources(t *testing.T) {
	ctx := context.Background()
	for _, sourceType := range []string{"kafka", "postgresql-cdc"} {
		t.Run(sourceType, func(t *testing.T) {
			spec := &dataflowv1.DataFlowSpec{
				Source: dataflowv1.SourceSpec{Type: sourceType},
				Sink:   dataflowv1.SinkSpec{Type: "postgresql"},
			}
			_, err := Run(ctx, spec, RunOptions{Once: true})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "does not finish a one-shot run")
			assert.Contains(t, err.Error(), sourceType)
		})
	}
}

func TestSourceExhausts(t *testing.T) {
	for _, sourceType := range []string{"postgresql", "clickhouse", "trino", "iceberg", "nessie"} {
		assert.True(t, SourceExhausts(sourceType), sourceType)
	}
	assert.False(t, SourceExhausts("kafka"))
	assert.False(t, SourceExhausts("postgresql-cdc"))
}

func TestRun_ResetCheckpointClearsFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "cp.json")
	require.NoError(t, os.WriteFile(path, []byte("{\"postgresql\":{\"orderByValue\":\"9\"}}\n"), 0o644))

	spec := &dataflowv1.DataFlowSpec{
		Source: dataflowv1.SourceSpec{Type: "postgresql"},
		Sink:   dataflowv1.SinkSpec{Type: "not-a-sink"},
	}
	_, err := Run(ctx, spec, RunOptions{
		Once:            true,
		CheckpointPath:  path,
		ResetCheckpoint: true,
		Namespace:       "ns",
		Name:            "mig",
		LogLevel:        "error",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "configuration is required")

	onDisk, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.NotContains(t, string(onDisk), "orderByValue")
}

func TestRun_SecretRefMissingKey(t *testing.T) {
	orig := kubeClientFactory
	t.Cleanup(func() { kubeClientFactory = orig })

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "default"},
		Data:       map[string][]byte{"other": []byte("x")},
	}
	kubeClientFactory = func() (client.Client, error) {
		return fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build(), nil
	}

	raw, err := json.Marshal(dataflowv1.PostgreSQLSourceSpec{
		ConnectionStringSecretRef: &dataflowv1.SecretRef{Name: "db", Key: "dsn"},
		Table:                     "public.src",
	})
	require.NoError(t, err)
	spec := &dataflowv1.DataFlowSpec{
		Source: dataflowv1.SourceSpec{
			Type:   "postgresql",
			Config: &runtime.RawExtension{Raw: raw},
		},
		Sink: dataflowv1.SinkSpec{Type: "postgresql"},
	}
	_, err = Run(context.Background(), spec, RunOptions{Once: true, Namespace: "default", LogLevel: "error"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "key dsn")
	assert.Contains(t, err.Error(), "db")
}

func TestRun_SecretRefWithoutKubeconfig(t *testing.T) {
	orig := kubeClientFactory
	t.Cleanup(func() { kubeClientFactory = orig })
	kubeClientFactory = func() (client.Client, error) {
		return nil, assert.AnError
	}
	raw, err := json.Marshal(dataflowv1.PostgreSQLSourceSpec{
		ConnectionStringSecretRef: &dataflowv1.SecretRef{Name: "db", Key: "dsn"},
	})
	require.NoError(t, err)
	spec := &dataflowv1.DataFlowSpec{
		Source: dataflowv1.SourceSpec{Type: "postgresql", Config: &runtime.RawExtension{Raw: raw}},
		Sink:   dataflowv1.SinkSpec{Type: "postgresql"},
	}
	_, err = Run(context.Background(), spec, RunOptions{Once: true, Namespace: "default"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "secret reference requires kubeconfig")
}
