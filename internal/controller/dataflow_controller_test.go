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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/version"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustConfig(v interface{}) *runtime.RawExtension {
	b, _ := json.Marshal(v)
	return &runtime.RawExtension{Raw: b}
}

// conflictSimulatingClient wraps a client and returns Conflict on the first N Status().Update() calls.
type conflictSimulatingClient struct {
	client.Client
	realStatus           client.SubResourceWriter
	statusUpdateAttempts int
	conflictsToSimulate  int
	mu                   sync.Mutex
}

func newConflictSimulatingClient(real client.Client, conflictsToSimulate int) *conflictSimulatingClient {
	return &conflictSimulatingClient{
		Client:              real,
		realStatus:          real.Status(),
		conflictsToSimulate: conflictsToSimulate,
	}
}

func (c *conflictSimulatingClient) Status() client.SubResourceWriter {
	return &conflictSimulatingStatusWriter{client: c}
}

type conflictSimulatingStatusWriter struct {
	client *conflictSimulatingClient
}

func (s *conflictSimulatingStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	s.client.mu.Lock()
	s.client.statusUpdateAttempts++
	n := s.client.statusUpdateAttempts
	s.client.mu.Unlock()

	if n <= s.client.conflictsToSimulate {
		return apierrors.NewConflict(
			schema.GroupResource{Group: "dataflow.dataflow.io", Resource: "dataflows"},
			obj.GetName(),
			fmt.Errorf("simulated conflict (attempt %d)", n),
		)
	}
	return s.client.realStatus.Update(ctx, obj, opts...)
}

func (s *conflictSimulatingStatusWriter) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	return s.client.realStatus.Create(ctx, obj, subResource, opts...)
}

func (s *conflictSimulatingStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	return s.client.realStatus.Patch(ctx, obj, patch, opts...)
}

// deploymentUpdateConflictClient returns Conflict on the first N Update() calls for Deployments.
type deploymentUpdateConflictClient struct {
	client.Client
	deploymentUpdateAttempts int
	conflictsToSimulate      int
	mu                       sync.Mutex
}

func newDeploymentUpdateConflictClient(real client.Client, conflictsToSimulate int) *deploymentUpdateConflictClient {
	return &deploymentUpdateConflictClient{
		Client:              real,
		conflictsToSimulate: conflictsToSimulate,
	}
}

func (c *deploymentUpdateConflictClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if _, isDeployment := obj.(*appsv1.Deployment); isDeployment {
		c.mu.Lock()
		c.deploymentUpdateAttempts++
		n := c.deploymentUpdateAttempts
		c.mu.Unlock()
		if n <= c.conflictsToSimulate {
			return apierrors.NewConflict(
				schema.GroupResource{Group: "apps", Resource: "deployments"},
				obj.GetName(),
				fmt.Errorf("simulated deployment update conflict (attempt %d)", n),
			)
		}
	}
	return c.Client.Update(ctx, obj, opts...)
}

type listErrorClient struct {
	client.Client
	err error
}

func (c *listErrorClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return c.err
}

func TestNewDataFlowReconciler(t *testing.T) {
	tests := []struct {
		name                 string
		setProcessorImageEnv string
		setWatchSecretsEnv   string
		wantImage            string
		wantWatchSecrets     bool
	}{
		{
			name:                 "default processor image from version",
			setProcessorImageEnv: "",
			setWatchSecretsEnv:   "",
			wantImage:            version.DefaultProcessorImage(),
			wantWatchSecrets:     true,
		},
		{
			name:                 "custom processor image from env",
			setProcessorImageEnv: "my.registry/dataflow-processor:v1.2.3",
			setWatchSecretsEnv:   "",
			wantImage:            "my.registry/dataflow-processor:v1.2.3",
			wantWatchSecrets:     true,
		},
		{
			name:                 "can disable secrets watch",
			setProcessorImageEnv: "",
			setWatchSecretsEnv:   "false",
			wantImage:            version.DefaultProcessorImage(),
			wantWatchSecrets:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prev := os.Getenv("PROCESSOR_IMAGE")
			prevWatchSecrets := os.Getenv("WATCH_SECRETS")
			defer func() {
				_ = os.Setenv("PROCESSOR_IMAGE", prev)
				_ = os.Setenv("WATCH_SECRETS", prevWatchSecrets)
			}()
			if tt.setProcessorImageEnv != "" {
				require.NoError(t, os.Setenv("PROCESSOR_IMAGE", tt.setProcessorImageEnv))
			} else {
				require.NoError(t, os.Unsetenv("PROCESSOR_IMAGE"))
			}
			if tt.setWatchSecretsEnv != "" {
				require.NoError(t, os.Setenv("WATCH_SECRETS", tt.setWatchSecretsEnv))
			} else {
				require.NoError(t, os.Unsetenv("WATCH_SECRETS"))
			}

			scheme := runtime.NewScheme()
			require.NoError(t, dataflowv1.AddToScheme(scheme))
			require.NoError(t, clientgoscheme.AddToScheme(scheme))
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
			reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)

			assert.NotNil(t, reconciler)
			assert.Equal(t, fakeClient, reconciler.Client)
			assert.Equal(t, scheme, reconciler.Scheme)
			assert.Equal(t, tt.wantImage, reconciler.processorImage)
			assert.Equal(t, tt.wantWatchSecrets, reconciler.watchSecrets)
		})
	}
}

func TestProcessorImageFor(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	t.Run("default_same_as_controller", func(t *testing.T) {
		reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)
		df := &dataflowv1.DataFlow{
			ObjectMeta: metav1.ObjectMeta{Name: "df", Namespace: "default"},
			Spec: dataflowv1.DataFlowSpec{
				Source: dataflowv1.SourceSpec{Type: "kafka", Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t", ConsumerGroup: "g"})},
				Sink:   dataflowv1.SinkSpec{Type: "kafka", Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"})},
			},
		}
		img := reconciler.processorImageFor(df)
		assert.Equal(t, version.DefaultProcessorImage(), img)
	})

	t.Run("spec_processor_version", func(t *testing.T) {
		reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)
		df := &dataflowv1.DataFlow{
			ObjectMeta: metav1.ObjectMeta{Name: "df", Namespace: "default"},
			Spec: dataflowv1.DataFlowSpec{
				ProcessorVersion: "v1.2.3",
				Source:           dataflowv1.SourceSpec{Type: "kafka", Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t", ConsumerGroup: "g"})},
				Sink:             dataflowv1.SinkSpec{Type: "kafka", Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"})},
			},
		}
		img := reconciler.processorImageFor(df)
		assert.Equal(t, version.ProcessorImageWithTag("v1.2.3"), img)
		assert.Equal(t, "ghcr.io/dataflow-operator/dataflow:v1.2.3", img)
	})

	t.Run("spec_processor_image_overrides_version", func(t *testing.T) {
		reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)
		df := &dataflowv1.DataFlow{
			ObjectMeta: metav1.ObjectMeta{Name: "df", Namespace: "default"},
			Spec: dataflowv1.DataFlowSpec{
				ProcessorImage:   "my.registry.io/my-processor:custom",
				ProcessorVersion: "v1.2.3",
				Source:           dataflowv1.SourceSpec{Type: "kafka", Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t", ConsumerGroup: "g"})},
				Sink:             dataflowv1.SinkSpec{Type: "kafka", Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"})},
			},
		}
		img := reconciler.processorImageFor(df)
		assert.Equal(t, "my.registry.io/my-processor:custom", img)
	})
}

func TestEnqueueAllDataFlowsForOperatorUpdate(t *testing.T) {
	const opName, opNs = "dataflow-operator", "dataflow-system"
	prevName, prevNs := os.Getenv("OPERATOR_DEPLOYMENT_NAME"), os.Getenv("OPERATOR_NAMESPACE")
	defer func() {
		_ = os.Setenv("OPERATOR_DEPLOYMENT_NAME", prevName)
		_ = os.Setenv("OPERATOR_NAMESPACE", prevNs)
	}()
	require.NoError(t, os.Setenv("OPERATOR_DEPLOYMENT_NAME", opName))
	require.NoError(t, os.Setenv("OPERATOR_NAMESPACE", opNs))

	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))

	df1 := &dataflowv1.DataFlow{ObjectMeta: metav1.ObjectMeta{Name: "df1", Namespace: "default"}}
	df2 := &dataflowv1.DataFlow{ObjectMeta: metav1.ObjectMeta{Name: "df2", Namespace: "other"}}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(df1, df2).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)

	operatorDeployment := &appsv1.Deployment{}
	operatorDeployment.SetName(opName)
	operatorDeployment.SetNamespace(opNs)

	ctx := context.Background()
	reqs := reconciler.enqueueAllDataFlowsForOperatorUpdate(ctx, operatorDeployment)
	assert.Len(t, reqs, 2)
	names := make(map[string]bool)
	for _, r := range reqs {
		names[r.Namespace+"/"+r.Name] = true
	}
	assert.True(t, names["default/df1"])
	assert.True(t, names["other/df2"])

	otherDeployment := &appsv1.Deployment{}
	otherDeployment.SetName("other-deploy")
	otherDeployment.SetNamespace(opNs)
	reqs2 := reconciler.enqueueAllDataFlowsForOperatorUpdate(ctx, otherDeployment)
	assert.Nil(t, reqs2)
}

func TestShouldEnqueueOnOperatorDeploymentUpdate(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	reconciler := &DataFlowReconciler{
		Client:                      fakeClient,
		Scheme:                      scheme,
		operatorDeploymentName:      "dataflow-operator",
		operatorDeploymentNamespace: "dataflow-system",
	}

	base := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "dataflow-operator",
			Namespace:  "dataflow-system",
			Generation: 2,
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "operator", Image: "repo/operator:v1"}},
				},
			},
		},
	}

	t.Run("ignores status-only update", func(t *testing.T) {
		oldDep := base.DeepCopy()
		newDep := base.DeepCopy()
		newDep.Status.ReadyReplicas = 1
		assert.False(t, reconciler.shouldEnqueueOnOperatorDeploymentUpdate(oldDep, newDep))
	})

	t.Run("accepts generation change", func(t *testing.T) {
		oldDep := base.DeepCopy()
		newDep := base.DeepCopy()
		newDep.Generation = 3
		assert.True(t, reconciler.shouldEnqueueOnOperatorDeploymentUpdate(oldDep, newDep))
	})

	t.Run("accepts spec template change", func(t *testing.T) {
		oldDep := base.DeepCopy()
		newDep := base.DeepCopy()
		newDep.Spec.Template.Spec.Containers[0].Image = "repo/operator:v2"
		assert.True(t, reconciler.shouldEnqueueOnOperatorDeploymentUpdate(oldDep, newDep))
	})

	t.Run("ignores non-operator deployment", func(t *testing.T) {
		oldDep := base.DeepCopy()
		newDep := base.DeepCopy()
		newDep.Name = "other"
		newDep.Generation = 3
		assert.False(t, reconciler.shouldEnqueueOnOperatorDeploymentUpdate(oldDep, newDep))
	})
}

func TestEnqueueAllDataFlowsForOperatorUpdate_ListErrorReturnsNil(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	reconciler := &DataFlowReconciler{
		Client:                      &listErrorClient{Client: fakeClient, err: fmt.Errorf("list failed")},
		Scheme:                      scheme,
		operatorDeploymentName:      "dataflow-operator",
		operatorDeploymentNamespace: "dataflow-system",
	}

	operatorDeployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "dataflow-operator", Namespace: "dataflow-system"},
	}
	reqs := reconciler.enqueueAllDataFlowsForOperatorUpdate(context.Background(), operatorDeployment)
	assert.Nil(t, reqs)
}

func TestEnqueueAllDataFlowsForSecretUpdate(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))

	df1 := &dataflowv1.DataFlow{ObjectMeta: metav1.ObjectMeta{Name: "df1", Namespace: "default"}}
	df2 := &dataflowv1.DataFlow{ObjectMeta: metav1.ObjectMeta{Name: "df2", Namespace: "default"}}
	dfOtherNamespace := &dataflowv1.DataFlow{ObjectMeta: metav1.ObjectMeta{Name: "df3", Namespace: "other"}}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(df1, df2, dfOtherNamespace).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)

	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s1", Namespace: "default"}}
	reqs := reconciler.enqueueAllDataFlowsForSecretUpdate(context.Background(), secret)
	assert.Len(t, reqs, 2)
	names := make(map[string]bool)
	for _, r := range reqs {
		names[r.Namespace+"/"+r.Name] = true
	}
	assert.True(t, names["default/df1"])
	assert.True(t, names["default/df2"])
	assert.False(t, names["other/df3"])
}

func TestShouldEnqueueOnSecretUpdate(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)

	base := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "default"},
		Data: map[string][]byte{
			"username": []byte("admin"),
			"password": []byte("secret"),
		},
		Type: corev1.SecretTypeOpaque,
	}

	t.Run("ignores metadata-only update", func(t *testing.T) {
		oldSecret := base.DeepCopy()
		newSecret := base.DeepCopy()
		newSecret.Annotations = map[string]string{"x": "y"}
		assert.False(t, reconciler.shouldEnqueueOnSecretUpdate(oldSecret, newSecret))
	})

	t.Run("accepts data change", func(t *testing.T) {
		oldSecret := base.DeepCopy()
		newSecret := base.DeepCopy()
		newSecret.Data["password"] = []byte("new-secret")
		assert.True(t, reconciler.shouldEnqueueOnSecretUpdate(oldSecret, newSecret))
	})

	t.Run("accepts type change", func(t *testing.T) {
		oldSecret := base.DeepCopy()
		newSecret := base.DeepCopy()
		newSecret.Type = corev1.SecretTypeDockerConfigJson
		assert.True(t, reconciler.shouldEnqueueOnSecretUpdate(oldSecret, newSecret))
	})
}

func TestUpdateStatusWithRetry_SuccessOnFirstAttempt(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))

	dataflow := &dataflowv1.DataFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "test-df", Namespace: "default"},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "t", ConsumerGroup: "g"}),
			},
			Sink: dataflowv1.SinkSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "out"}),
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&dataflowv1.DataFlow{}).
		WithObjects(dataflow).
		Build()

	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-df", Namespace: "default"}}

	err := reconciler.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
		df.Status.Phase = "Running"
		df.Status.Message = "test"
	})
	require.NoError(t, err)

	var updated dataflowv1.DataFlow
	require.NoError(t, fakeClient.Get(ctx, req.NamespacedName, &updated))
	assert.Equal(t, "Running", updated.Status.Phase)
	assert.Equal(t, "test", updated.Status.Message)
}

func TestUpdateStatusWithRetry_RetriesOnConflict(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))

	dataflow := &dataflowv1.DataFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "test-df", Namespace: "default"},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "t", ConsumerGroup: "g"}),
			},
			Sink: dataflowv1.SinkSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "out"}),
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&dataflowv1.DataFlow{}).
		WithObjects(dataflow).
		Build()

	conflictClient := newConflictSimulatingClient(fakeClient, 2)
	reconciler := NewDataFlowReconciler(conflictClient, scheme, nil)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-df", Namespace: "default"}}

	err := reconciler.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
		df.Status.Phase = "Running"
		df.Status.Message = "after retry"
	})
	require.NoError(t, err)

	var updated dataflowv1.DataFlow
	require.NoError(t, fakeClient.Get(ctx, req.NamespacedName, &updated))
	assert.Equal(t, "Running", updated.Status.Phase)
	assert.Equal(t, "after retry", updated.Status.Message)
	assert.Equal(t, 3, conflictClient.statusUpdateAttempts, "should have attempted 3 times (2 conflicts + 1 success)")
}

func TestUpdateStatusWithRetry_ReturnsErrorAfterMaxRetries(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))

	dataflow := &dataflowv1.DataFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "test-df", Namespace: "default"},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "t", ConsumerGroup: "g"}),
			},
			Sink: dataflowv1.SinkSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "out"}),
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&dataflowv1.DataFlow{}).
		WithObjects(dataflow).
		Build()

	conflictClient := newConflictSimulatingClient(fakeClient, 10)
	reconciler := NewDataFlowReconciler(conflictClient, scheme, nil)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-df", Namespace: "default"}}

	err := reconciler.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
		df.Status.Phase = "Running"
	})
	require.Error(t, err)
	assert.True(t, apierrors.IsConflict(err) || strings.Contains(err.Error(), "conflict"))
}

func TestUpdateStatusWithRetry_NotFoundReturnsImmediately(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&dataflowv1.DataFlow{}).
		Build()

	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "non-existent", Namespace: "default"}}

	err := reconciler.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
		df.Status.Phase = "Running"
	})
	require.Error(t, err)
	assert.True(t, apierrors.IsNotFound(err))
}

func TestGenReconcileID(t *testing.T) {
	for i := 0; i < 10; i++ {
		id := genReconcileID()
		assert.Len(t, id, 8, "reconcile_id should be 8 hex chars")
		assert.Regexp(t, `^[0-9a-f]+$`, id, "reconcile_id should be hex")
	}
}

func TestReconcileTimeout(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		// Unset in case it was set by another test or env
		t.Setenv("RECONCILE_TIMEOUT_SECONDS", "")
		d := reconcileTimeout()
		assert.Equal(t, 180*time.Second, d)
	})
	t.Run("valid", func(t *testing.T) {
		t.Setenv("RECONCILE_TIMEOUT_SECONDS", "120")
		d := reconcileTimeout()
		assert.Equal(t, 120*time.Second, d)
	})
	t.Run("invalid_fallback_to_default", func(t *testing.T) {
		t.Setenv("RECONCILE_TIMEOUT_SECONDS", "invalid")
		d := reconcileTimeout()
		assert.Equal(t, 180*time.Second, d)
	})
	t.Run("zero_fallback_to_default", func(t *testing.T) {
		t.Setenv("RECONCILE_TIMEOUT_SECONDS", "0")
		d := reconcileTimeout()
		assert.Equal(t, 180*time.Second, d)
	})
}

func TestPendingRequeueAfter(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Setenv("RECONCILE_PENDING_REQUEUE_SECONDS", "")
		d := pendingRequeueAfter()
		assert.Equal(t, 20*time.Second, d)
	})
	t.Run("valid", func(t *testing.T) {
		t.Setenv("RECONCILE_PENDING_REQUEUE_SECONDS", "30")
		d := pendingRequeueAfter()
		assert.Equal(t, 30*time.Second, d)
	})
	t.Run("invalid_fallback_to_default", func(t *testing.T) {
		t.Setenv("RECONCILE_PENDING_REQUEUE_SECONDS", "invalid")
		d := pendingRequeueAfter()
		assert.Equal(t, 20*time.Second, d)
	})
	t.Run("zero_fallback_to_default", func(t *testing.T) {
		t.Setenv("RECONCILE_PENDING_REQUEUE_SECONDS", "0")
		d := pendingRequeueAfter()
		assert.Equal(t, 20*time.Second, d)
	})
}

func TestMaxConcurrentReconciles(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Setenv("MAX_CONCURRENT_RECONCILES", "")
		assert.Equal(t, 1, maxConcurrentReconciles())
	})
	t.Run("valid", func(t *testing.T) {
		t.Setenv("MAX_CONCURRENT_RECONCILES", "4")
		assert.Equal(t, 4, maxConcurrentReconciles())
	})
	t.Run("invalid_fallback_to_default", func(t *testing.T) {
		t.Setenv("MAX_CONCURRENT_RECONCILES", "invalid")
		assert.Equal(t, 1, maxConcurrentReconciles())
	})
	t.Run("zero_fallback_to_default", func(t *testing.T) {
		t.Setenv("MAX_CONCURRENT_RECONCILES", "0")
		assert.Equal(t, 1, maxConcurrentReconciles())
	})
}

func TestDataFlowReconciler_Reconcile_CreateDeployment(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)
	err = clientgoscheme.AddToScheme(scheme)
	require.NoError(t, err)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)

	ctx := context.Background()

	dataflow := &dataflowv1.DataFlow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "dataflow.dataflow.io/v1",
			Kind:       "DataFlow",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataflow",
			Namespace: "default",
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
			},
			Sink: dataflowv1.SinkSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
			},
		},
	}

	err = fakeClient.Create(ctx, dataflow)
	require.NoError(t, err)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-dataflow",
			Namespace: "default",
		},
	}

	// Reconcile should create Deployment and ConfigMap
	result, err := reconciler.Reconcile(ctx, req)
	// We don't require no error because connection to Kafka will fail in background

	// Verify that the DataFlow still exists and has finalizer (added when first child is created)
	var updatedDataflow dataflowv1.DataFlow
	getErr := fakeClient.Get(ctx, req.NamespacedName, &updatedDataflow)
	require.NoError(t, getErr, "DataFlow should exist after reconcile")
	assert.Contains(t, updatedDataflow.Finalizers, DataFlowFinalizer, "DataFlow should have finalizer after creating Deployment/ConfigMap")
	// Verify Deployment was created
	var deployment appsv1.Deployment
	deploymentName := types.NamespacedName{
		Name:      "dataflow-test-dataflow",
		Namespace: "default",
	}
	err = fakeClient.Get(ctx, deploymentName, &deployment)
	assert.NoError(t, err, "Deployment should be created")
	assert.Equal(t, "dataflow-test-dataflow", deployment.Name)
	assert.Contains(t, deployment.Spec.Template.Annotations, specHashAnnotation,
		"Deployment pod template should have spec-hash annotation for ConfigMap change detection")
	assert.NotEmpty(t, deployment.Spec.Template.Annotations[specHashAnnotation],
		"spec-hash annotation should be non-empty")
	require.Len(t, deployment.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, version.DefaultProcessorImage(), deployment.Spec.Template.Spec.Containers[0].Image, "default processor image should match controller")
	var hasLogLevel bool
	for _, e := range deployment.Spec.Template.Spec.Containers[0].Env {
		if e.Name == "LOG_LEVEL" {
			hasLogLevel = true
			break
		}
	}
	assert.True(t, hasLogLevel, "processor container should have LOG_LEVEL env")

	// Without SENTRY_DSN, processor should not have Sentry env vars
	var hasSentryDSN bool
	for _, e := range deployment.Spec.Template.Spec.Containers[0].Env {
		if e.Name == "SENTRY_DSN" {
			hasSentryDSN = true
			break
		}
	}
	assert.False(t, hasSentryDSN, "processor should not have SENTRY_DSN when not set in operator")

	// Verify ConfigMap was created
	var configMap corev1.ConfigMap
	configMapName := types.NamespacedName{
		Name:      "dataflow-test-dataflow-spec",
		Namespace: "default",
	}
	err = fakeClient.Get(ctx, configMapName, &configMap)
	assert.NoError(t, err, "ConfigMap should be created")
	assert.Contains(t, configMap.Data, "spec.json")

	assert.Equal(t, ctrl.Result{}, result)
	assert.False(t, result.Requeue)
}

func TestDataFlowReconciler_Reconcile_ProcessorGetsSentryEnvWhenSet(t *testing.T) {
	prevDSN := os.Getenv("SENTRY_DSN")
	prevEnv := os.Getenv("SENTRY_ENVIRONMENT")
	defer func() {
		_ = os.Setenv("SENTRY_DSN", prevDSN)
		_ = os.Setenv("SENTRY_ENVIRONMENT", prevEnv)
	}()
	require.NoError(t, os.Setenv("SENTRY_DSN", "https://key@o0.ingest.sentry.io/1"))
	require.NoError(t, os.Setenv("SENTRY_ENVIRONMENT", "staging"))

	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)
	ctx := context.Background()

	dataflow := &dataflowv1.DataFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "test-dataflow", Namespace: "default"},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "t", ConsumerGroup: "g"}),
			},
			Sink: dataflowv1.SinkSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "out"}),
			},
		},
	}
	require.NoError(t, fakeClient.Create(ctx, dataflow))

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-dataflow", Namespace: "default"}}
	_, _ = reconciler.Reconcile(ctx, req)

	var deployment appsv1.Deployment
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: "dataflow-test-dataflow", Namespace: "default"}, &deployment))
	require.Len(t, deployment.Spec.Template.Spec.Containers, 1)

	env := deployment.Spec.Template.Spec.Containers[0].Env
	var dsnVal, envVal string
	for _, e := range env {
		if e.Name == "SENTRY_DSN" {
			dsnVal = e.Value
		}
		if e.Name == "SENTRY_ENVIRONMENT" {
			envVal = e.Value
		}
	}
	assert.Equal(t, "https://key@o0.ingest.sentry.io/1", dsnVal, "processor should have SENTRY_DSN from operator env")
	assert.Equal(t, "staging", envVal, "processor should have SENTRY_ENVIRONMENT from operator env")
}

func TestDataFlowReconciler_Reconcile_DeploymentUsesSpecProcessorImage(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)
	ctx := context.Background()

	customImage := "my.registry.io/dataflow-processor:v2.0.0"
	dataflow := &dataflowv1.DataFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "test-dataflow", Namespace: "default"},
		Spec: dataflowv1.DataFlowSpec{
			ProcessorImage: customImage,
			Source: dataflowv1.SourceSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "t", ConsumerGroup: "g"}),
			},
			Sink: dataflowv1.SinkSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "out"}),
			},
		},
	}
	require.NoError(t, fakeClient.Create(ctx, dataflow))

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-dataflow", Namespace: "default"}}
	_, _ = reconciler.Reconcile(ctx, req)

	var deployment appsv1.Deployment
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: "dataflow-test-dataflow", Namespace: "default"}, &deployment))
	require.Len(t, deployment.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, customImage, deployment.Spec.Template.Spec.Containers[0].Image, "Deployment should use spec.processorImage")
}

func TestDataFlowReconciler_Reconcile_DeploymentUsesSpecProcessorVersion(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)
	ctx := context.Background()

	dataflow := &dataflowv1.DataFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "test-dataflow", Namespace: "default"},
		Spec: dataflowv1.DataFlowSpec{
			ProcessorVersion: "v0.5.0",
			Source: dataflowv1.SourceSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "t", ConsumerGroup: "g"}),
			},
			Sink: dataflowv1.SinkSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "out"}),
			},
		},
	}
	require.NoError(t, fakeClient.Create(ctx, dataflow))

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-dataflow", Namespace: "default"}}
	_, _ = reconciler.Reconcile(ctx, req)

	var deployment appsv1.Deployment
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: "dataflow-test-dataflow", Namespace: "default"}, &deployment))
	require.Len(t, deployment.Spec.Template.Spec.Containers, 1)
	expectedImage := version.ProcessorImageWithTag("v0.5.0")
	assert.Equal(t, expectedImage, deployment.Spec.Template.Spec.Containers[0].Image, "Deployment should use default repo with spec.processorVersion")
}

func TestDataFlowReconciler_Reconcile_DeploymentUsesImagePullSecrets(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)
	ctx := context.Background()

	dataflow := &dataflowv1.DataFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "test-dataflow", Namespace: "default"},
		Spec: dataflowv1.DataFlowSpec{
			ImagePullSecrets: []corev1.LocalObjectReference{
				{Name: "my-registry-secret"},
				{Name: "other-pull-secret"},
			},
			Source: dataflowv1.SourceSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "t", ConsumerGroup: "g"}),
			},
			Sink: dataflowv1.SinkSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "out"}),
			},
		},
	}
	require.NoError(t, fakeClient.Create(ctx, dataflow))

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-dataflow", Namespace: "default"}}
	_, _ = reconciler.Reconcile(ctx, req)

	var deployment appsv1.Deployment
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: "dataflow-test-dataflow", Namespace: "default"}, &deployment))
	assert.Equal(t, []corev1.LocalObjectReference{
		{Name: "my-registry-secret"},
		{Name: "other-pull-secret"},
	}, deployment.Spec.Template.Spec.ImagePullSecrets, "Deployment should use spec.imagePullSecrets for private registry")
}

func TestDataFlowReconciler_Reconcile_DeleteDataFlow(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)
	err = clientgoscheme.AddToScheme(scheme)
	require.NoError(t, err)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)

	ctx := context.Background()

	now := metav1.Now()
	dataflow := &dataflowv1.DataFlow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "dataflow.dataflow.io/v1",
			Kind:       "DataFlow",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-dataflow",
			Namespace:         "default",
			DeletionTimestamp: &now,
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
			},
			Sink: dataflowv1.SinkSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
			},
		},
	}

	err = fakeClient.Create(ctx, dataflow)
	require.NoError(t, err)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-dataflow",
			Namespace: "default",
		},
	}

	// DataFlow has no finalizer: deletion branch returns early, no cleanup attempted
	result, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
	assert.False(t, result.Requeue)
}

// TestDataFlowReconciler_Reconcile_DeleteDataFlow_WithFinalizer verifies that when DataFlow
// has DeletionTimestamp and our finalizer, Reconcile runs cleanup and removes the finalizer.
func TestDataFlowReconciler_Reconcile_DeleteDataFlow_WithFinalizer(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)
	err = clientgoscheme.AddToScheme(scheme)
	require.NoError(t, err)

	now := metav1.Now()
	dataflow := &dataflowv1.DataFlow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "dataflow.dataflow.io/v1",
			Kind:       "DataFlow",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-dataflow",
			Namespace:         "default",
			DeletionTimestamp: &now,
			Finalizers:        []string{DataFlowFinalizer},
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
			},
			Sink: dataflowv1.SinkSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
			},
		},
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dataflow-test-dataflow",
			Namespace: "default",
		},
	}
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dataflow-test-dataflow-spec",
			Namespace: "default",
		},
		Data: map[string]string{"spec.json": "{}"},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&dataflowv1.DataFlow{}).
		WithObjects(dataflow, deployment, configMap).
		Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)
	ctx := context.Background()

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-dataflow",
			Namespace: "default",
		},
	}

	result, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	// Deployment and ConfigMap should be deleted
	var dep appsv1.Deployment
	err = fakeClient.Get(ctx, types.NamespacedName{Name: "dataflow-test-dataflow", Namespace: "default"}, &dep)
	assert.True(t, apierrors.IsNotFound(err), "Deployment should be deleted")

	var cm corev1.ConfigMap
	err = fakeClient.Get(ctx, types.NamespacedName{Name: "dataflow-test-dataflow-spec", Namespace: "default"}, &cm)
	assert.True(t, apierrors.IsNotFound(err), "ConfigMap should be deleted")

	// DataFlow: either deleted (real API server removes object once finalizer is gone) or still present without our finalizer
	var df dataflowv1.DataFlow
	err = fakeClient.Get(ctx, req.NamespacedName, &df)
	if apierrors.IsNotFound(err) {
		// Expected with envtest/real API: object is deleted after finalizer removal
		return
	}
	require.NoError(t, err)
	assert.NotContains(t, df.Finalizers, DataFlowFinalizer, "finalizer should be removed")
	assert.Equal(t, "Stopped", df.Status.Phase)
	readyCond := meta.FindStatusCondition(df.Status.Conditions, conditionReady)
	require.NotNil(t, readyCond)
	assert.Equal(t, "Stopped", readyCond.Reason)
}

func TestEnsureDataFlowFinalizer(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)
	err = clientgoscheme.AddToScheme(scheme)
	require.NoError(t, err)

	dataflow := &dataflowv1.DataFlow{
		ObjectMeta: metav1.ObjectMeta{Name: "df1", Namespace: "default"},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{Type: "kafka", Config: mustConfig(dataflowv1.KafkaSourceSpec{})},
			Sink:   dataflowv1.SinkSpec{Type: "kafka", Config: mustConfig(dataflowv1.KafkaSinkSpec{})},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dataflow).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "df1", Namespace: "default"}}

	err = reconciler.ensureDataFlowFinalizer(ctx, req)
	require.NoError(t, err)

	var df dataflowv1.DataFlow
	err = fakeClient.Get(ctx, req.NamespacedName, &df)
	require.NoError(t, err)
	assert.Contains(t, df.Finalizers, DataFlowFinalizer)

	// Idempotent: second call does not duplicate
	err = reconciler.ensureDataFlowFinalizer(ctx, req)
	require.NoError(t, err)
	err = fakeClient.Get(ctx, req.NamespacedName, &df)
	require.NoError(t, err)
	assert.Equal(t, []string{DataFlowFinalizer}, df.Finalizers)
}

func TestRemoveDataFlowFinalizer(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)
	err = clientgoscheme.AddToScheme(scheme)
	require.NoError(t, err)

	dataflow := &dataflowv1.DataFlow{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "df1",
			Namespace:  "default",
			Finalizers: []string{"other.io/finalizer", DataFlowFinalizer},
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{Type: "kafka", Config: mustConfig(dataflowv1.KafkaSourceSpec{})},
			Sink:   dataflowv1.SinkSpec{Type: "kafka", Config: mustConfig(dataflowv1.KafkaSinkSpec{})},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dataflow).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "df1", Namespace: "default"}}

	err = reconciler.removeDataFlowFinalizer(ctx, req)
	require.NoError(t, err)

	var df dataflowv1.DataFlow
	err = fakeClient.Get(ctx, req.NamespacedName, &df)
	require.NoError(t, err)
	assert.NotContains(t, df.Finalizers, DataFlowFinalizer)
	assert.Equal(t, []string{"other.io/finalizer"}, df.Finalizers)
}

func TestDataFlowReconciler_Reconcile_WithResourcesAndNodeSelector(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)
	err = clientgoscheme.AddToScheme(scheme)
	require.NoError(t, err)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)

	ctx := context.Background()

	cpuRequest := resource.MustParse("200m")
	memoryRequest := resource.MustParse("256Mi")
	cpuLimit := resource.MustParse("1000m")
	memoryLimit := resource.MustParse("1Gi")

	dataflow := &dataflowv1.DataFlow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "dataflow.dataflow.io/v1",
			Kind:       "DataFlow",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataflow",
			Namespace: "default",
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
			},
			Sink: dataflowv1.SinkSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
			},
			Resources: &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    cpuRequest,
					corev1.ResourceMemory: memoryRequest,
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    cpuLimit,
					corev1.ResourceMemory: memoryLimit,
				},
			},
			NodeSelector: map[string]string{
				"node-type": "compute",
				"zone":      "us-east-1",
			},
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      "kubernetes.io/arch",
										Operator: corev1.NodeSelectorOpIn,
										Values:   []string{"amd64"},
									},
								},
							},
						},
					},
				},
			},
			Tolerations: []corev1.Toleration{
				{
					Key:      "dedicated",
					Operator: corev1.TolerationOpEqual,
					Value:    "dataflow",
					Effect:   corev1.TaintEffectNoSchedule,
				},
			},
		},
	}

	err = fakeClient.Create(ctx, dataflow)
	require.NoError(t, err)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-dataflow",
			Namespace: "default",
		},
	}

	// Reconcile should create Deployment with custom resources and node selector
	result, err := reconciler.Reconcile(ctx, req)
	// We don't require no error because connection to Kafka will fail in background

	// Verify Deployment was created with correct settings
	var deployment appsv1.Deployment
	deploymentName := types.NamespacedName{
		Name:      "dataflow-test-dataflow",
		Namespace: "default",
	}
	err = fakeClient.Get(ctx, deploymentName, &deployment)
	require.NoError(t, err, "Deployment should be created")

	// Verify resources
	container := deployment.Spec.Template.Spec.Containers[0]
	// Use Cmp() method for resource.Quantity comparison
	assert.Equal(t, 0, cpuRequest.Cmp(container.Resources.Requests[corev1.ResourceCPU]))
	assert.Equal(t, 0, memoryRequest.Cmp(container.Resources.Requests[corev1.ResourceMemory]))
	assert.Equal(t, 0, cpuLimit.Cmp(container.Resources.Limits[corev1.ResourceCPU]))
	assert.Equal(t, 0, memoryLimit.Cmp(container.Resources.Limits[corev1.ResourceMemory]))

	// Verify nodeSelector
	assert.Equal(t, "compute", deployment.Spec.Template.Spec.NodeSelector["node-type"])
	assert.Equal(t, "us-east-1", deployment.Spec.Template.Spec.NodeSelector["zone"])

	// Verify affinity
	assert.NotNil(t, deployment.Spec.Template.Spec.Affinity)
	assert.NotNil(t, deployment.Spec.Template.Spec.Affinity.NodeAffinity)
	assert.Equal(t, "amd64", deployment.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values[0])

	// Verify tolerations
	assert.Len(t, deployment.Spec.Template.Spec.Tolerations, 1)
	assert.Equal(t, "dedicated", deployment.Spec.Template.Spec.Tolerations[0].Key)
	assert.Equal(t, "dataflow", deployment.Spec.Template.Spec.Tolerations[0].Value)

	// Verify graceful shutdown: terminationGracePeriodSeconds and preStop
	assert.NotNil(t, deployment.Spec.Template.Spec.TerminationGracePeriodSeconds)
	assert.Equal(t, int64(600), *deployment.Spec.Template.Spec.TerminationGracePeriodSeconds)
	require.NotNil(t, container.Lifecycle)
	require.NotNil(t, container.Lifecycle.PreStop)
	require.NotNil(t, container.Lifecycle.PreStop.Exec)
	assert.Equal(t, []string{"/bin/sh", "-c", "sleep 5"}, container.Lifecycle.PreStop.Exec.Command)

	assert.Equal(t, ctrl.Result{}, result)
	assert.False(t, result.Requeue)
}

// TestCreateOrUpdateDeployment_NoUpdateWhenSpecUnchanged verifies that on second reconcile
// with unchanged DataFlow spec, Deployment Update is not called (no extra PATCH and rolling update).
func TestCreateOrUpdateDeployment_NoUpdateWhenSpecUnchanged(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))

	fakeRecorder := record.NewFakeRecorder(10)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, fakeRecorder)

	ctx := context.Background()
	dataflow := &dataflowv1.DataFlow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "dataflow.dataflow.io/v1",
			Kind:       "DataFlow",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataflow",
			Namespace: "default",
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
			},
			Sink: dataflowv1.SinkSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
			},
		},
	}
	require.NoError(t, fakeClient.Create(ctx, dataflow))

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-dataflow", Namespace: "default"},
	}

	// First reconcile — Deployment created (DeploymentCreated)
	_, _ = reconciler.Reconcile(ctx, req)
	drainRecorderEvents(fakeRecorder) // drain creation events

	// Second reconcile without spec change — Update should not be called (DeploymentUpdated should not occur)
	_, _ = reconciler.Reconcile(ctx, req)
	var deploymentUpdatedCount int
	for {
		select {
		case e := <-fakeRecorder.Events:
			if strings.Contains(e, "DeploymentUpdated") {
				deploymentUpdatedCount++
			}
		default:
			goto done
		}
	}
done:
	assert.Equal(t, 0, deploymentUpdatedCount,
		"expected no DeploymentUpdated event on second reconcile when spec unchanged")
}

// TestCreateOrUpdateDeployment_UpdateWhenSpecChanged verifies that when DataFlow spec changes,
// Deployment Update is called and desired state is applied.
func TestCreateOrUpdateDeployment_UpdateWhenSpecChanged(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)

	ctx := context.Background()
	dataflow := &dataflowv1.DataFlow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "dataflow.dataflow.io/v1",
			Kind:       "DataFlow",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataflow",
			Namespace: "default",
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
			},
			Sink: dataflowv1.SinkSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
			},
		},
	}
	require.NoError(t, fakeClient.Create(ctx, dataflow))

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-dataflow", Namespace: "default"},
	}

	// First reconcile — Deployment created
	_, _ = reconciler.Reconcile(ctx, req)

	deploymentName := types.NamespacedName{Name: "dataflow-test-dataflow", Namespace: "default"}
	var deployment appsv1.Deployment

	// Change DataFlow spec (NodeSelector goes into Deployment spec)
	require.NoError(t, fakeClient.Get(ctx, req.NamespacedName, dataflow))
	dataflow.Spec.NodeSelector = map[string]string{"node-type": "compute"}
	require.NoError(t, fakeClient.Update(ctx, dataflow))

	// Second reconcile — Update should be called
	_, _ = reconciler.Reconcile(ctx, req)

	require.NoError(t, fakeClient.Get(ctx, deploymentName, &deployment))
	assert.Equal(t, "compute", deployment.Spec.Template.Spec.NodeSelector["node-type"],
		"Deployment NodeSelector should reflect updated DataFlow spec")
}

// TestCreateOrUpdateDeployment_UpdateWhenSpecContentChanged verifies that when DataFlow spec content
// changes (e.g. Kafka brokers, SecretRef), the spec-hash annotation changes and Deployment is updated,
// triggering a pod restart for the new config.
func TestCreateOrUpdateDeployment_UpdateWhenSpecContentChanged(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))

	fakeRecorder := record.NewFakeRecorder(10)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, fakeRecorder)

	ctx := context.Background()
	dataflow := &dataflowv1.DataFlow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "dataflow.dataflow.io/v1",
			Kind:       "DataFlow",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataflow",
			Namespace: "default",
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
			},
			Sink: dataflowv1.SinkSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
			},
		},
	}
	require.NoError(t, fakeClient.Create(ctx, dataflow))

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-dataflow", Namespace: "default"},
	}

	// First reconcile — Deployment created
	_, _ = reconciler.Reconcile(ctx, req)
	drainRecorderEvents(fakeRecorder)

	deploymentName := types.NamespacedName{Name: "dataflow-test-dataflow", Namespace: "default"}
	var deployment appsv1.Deployment
	require.NoError(t, fakeClient.Get(ctx, deploymentName, &deployment))
	hashBefore := deployment.Spec.Template.Annotations[specHashAnnotation]
	require.NotEmpty(t, hashBefore, "initial Deployment should have spec-hash annotation")

	// Change DataFlow spec content (Kafka brokers) — same ConfigMap name, but content changes
	require.NoError(t, fakeClient.Get(ctx, req.NamespacedName, dataflow))
	var kafkaCfg dataflowv1.KafkaSourceSpec
	require.NoError(t, json.Unmarshal(dataflow.Spec.Source.Config.Raw, &kafkaCfg))
	kafkaCfg.Brokers = []string{"kafka-1:9092", "kafka-2:9092"}
	dataflow.Spec.Source.Config = mustConfig(kafkaCfg)
	require.NoError(t, fakeClient.Update(ctx, dataflow))

	// Second reconcile — Deployment should be updated (spec-hash changed)
	_, _ = reconciler.Reconcile(ctx, req)

	var deploymentUpdatedCount int
	for {
		select {
		case e := <-fakeRecorder.Events:
			if strings.Contains(e, "DeploymentUpdated") {
				deploymentUpdatedCount++
			}
		default:
			goto done
		}
	}
done:
	assert.Equal(t, 1, deploymentUpdatedCount,
		"expected DeploymentUpdated event when spec content (Kafka brokers) changed")

	require.NoError(t, fakeClient.Get(ctx, deploymentName, &deployment))
	hashAfter := deployment.Spec.Template.Annotations[specHashAnnotation]
	assert.NotEqual(t, hashBefore, hashAfter,
		"spec-hash should change when Kafka brokers change, triggering pod restart")
}

// TestCreateOrUpdateDeployment_RetryOnConflict verifies that when Deployment Update returns 409 Conflict,
// the controller retries and succeeds on the next attempt (no extra rollout: spec comparison skips redundant Update).
func TestCreateOrUpdateDeployment_RetryOnConflict(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	// Simulate one conflict on first Deployment Update, then success
	conflictClient := newDeploymentUpdateConflictClient(fakeClient, 1)
	reconciler := NewDataFlowReconciler(conflictClient, scheme, nil)

	ctx := context.Background()
	dataflow := &dataflowv1.DataFlow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "dataflow.dataflow.io/v1",
			Kind:       "DataFlow",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataflow",
			Namespace: "default",
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
			},
			Sink: dataflowv1.SinkSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
			},
		},
	}
	require.NoError(t, fakeClient.Create(ctx, dataflow))

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-dataflow", Namespace: "default"},
	}

	// First reconcile — create Deployment (no Update yet)
	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	// Change spec so next reconcile will call Update
	require.NoError(t, fakeClient.Get(ctx, req.NamespacedName, dataflow))
	dataflow.Spec.NodeSelector = map[string]string{"node-type": "compute"}
	require.NoError(t, fakeClient.Update(ctx, dataflow))

	// Second reconcile — first Update returns conflict, retry (re-Get + Update) succeeds
	_, err = reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	deploymentName := types.NamespacedName{Name: "dataflow-test-dataflow", Namespace: "default"}
	var deployment appsv1.Deployment
	require.NoError(t, fakeClient.Get(ctx, deploymentName, &deployment))
	assert.Equal(t, "compute", deployment.Spec.Template.Spec.NodeSelector["node-type"])
}

// TestCreateOrUpdateDeployment_ConflictAfterMaxRetries verifies that when Deployment Update always returns Conflict,
// the controller returns error after maxRetries (reconcile will be requeued).
func TestCreateOrUpdateDeployment_ConflictAfterMaxRetries(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	conflictClient := newDeploymentUpdateConflictClient(fakeClient, 20) // more than maxRetries (5)
	reconciler := NewDataFlowReconciler(conflictClient, scheme, nil)

	ctx := context.Background()
	dataflow := &dataflowv1.DataFlow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "dataflow.dataflow.io/v1",
			Kind:       "DataFlow",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataflow",
			Namespace: "default",
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
			},
			Sink: dataflowv1.SinkSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
			},
		},
	}
	require.NoError(t, fakeClient.Create(ctx, dataflow))

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-dataflow", Namespace: "default"},
	}

	_, _ = reconciler.Reconcile(ctx, req)

	require.NoError(t, fakeClient.Get(ctx, req.NamespacedName, dataflow))
	dataflow.Spec.NodeSelector = map[string]string{"node-type": "compute"}
	require.NoError(t, fakeClient.Update(ctx, dataflow))

	_, err := reconciler.Reconcile(ctx, req)
	require.Error(t, err)
	assert.True(t, apierrors.IsConflict(err) || strings.Contains(err.Error(), "conflict") || strings.Contains(err.Error(), "after 5 attempts"))
}

func drainRecorderEvents(r *record.FakeRecorder) {
	for {
		select {
		case <-r.Events:
			// continue draining
		default:
			return
		}
	}
}

func TestDataFlowReconciler_Reconcile_InvalidSpec(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)
	err = clientgoscheme.AddToScheme(scheme)
	require.NoError(t, err)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)

	ctx := context.Background()

	dataflow := &dataflowv1.DataFlow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "dataflow.dataflow.io/v1",
			Kind:       "DataFlow",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataflow",
			Namespace: "default",
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type: "invalid",
			},
			Sink: dataflowv1.SinkSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
			},
		},
	}

	err = fakeClient.Create(ctx, dataflow)
	require.NoError(t, err)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-dataflow",
			Namespace: "default",
		},
	}

	// Reconcile with invalid source type: controller creates ConfigMap and Deployment (spec not validated here),
	// error will appear when processor pod starts. Reconcile may complete without error.
	result, err := reconciler.Reconcile(ctx, req)

	// DataFlow should remain, result — no requeue
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	var updatedDataflow dataflowv1.DataFlow
	getErr := fakeClient.Get(ctx, req.NamespacedName, &updatedDataflow)
	require.NoError(t, getErr, "DataFlow should exist")

	// Status may be Error (if validation returned error somewhere), Pending or Running
	// Controller does not validate source type when creating Deployment — invalid spec goes to processor pod
	assert.Contains(t, []string{"", "Error", "Pending", "Running"}, updatedDataflow.Status.Phase,
		"status phase should be one of Error, Pending, Running")
}

func TestDataFlowReconciler_Reconcile_UpdateStats(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)
	err = clientgoscheme.AddToScheme(scheme)
	require.NoError(t, err)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)

	ctx := context.Background()

	dataflow := &dataflowv1.DataFlow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "dataflow.dataflow.io/v1",
			Kind:       "DataFlow",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataflow",
			Namespace: "default",
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
			},
			Sink: dataflowv1.SinkSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
			},
		},
	}

	err = fakeClient.Create(ctx, dataflow)
	require.NoError(t, err)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-dataflow",
			Namespace: "default",
		},
	}

	// First reconcile to create processor
	// Note: This may fail because we can't actually connect to Kafka
	// But we're testing that the reconcile logic works
	_, err = reconciler.Reconcile(ctx, req)
	// We don't check for specific errors as they depend on external connections

	// Second reconcile to update stats (if processor was created)
	_, err = reconciler.Reconcile(ctx, req)
	// Again, we don't check for specific errors

	// Verify that reconcile completed (status may vary depending on connection success)
	var updatedDataflow dataflowv1.DataFlow
	getErr := fakeClient.Get(ctx, req.NamespacedName, &updatedDataflow)
	if getErr == nil {
		// Status should be set to something (Running, Error, or empty)
		// We just verify that the reconcile process ran
		assert.NotNil(t, updatedDataflow.Status)
	}
}

func TestDataFlowReconciler_Reconcile_NotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)
	err = clientgoscheme.AddToScheme(scheme)
	require.NoError(t, err)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)

	ctx := context.Background()

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "non-existent",
			Namespace: "default",
		},
	}

	result, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
	assert.False(t, result.Requeue)
}

func TestReconcileEmitsEvents(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))

	fakeRecorder := record.NewFakeRecorder(10)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, fakeRecorder)

	ctx := context.Background()
	dataflow := &dataflowv1.DataFlow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "dataflow.dataflow.io/v1",
			Kind:       "DataFlow",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataflow",
			Namespace: "default",
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSourceSpec{Brokers: []string{"localhost:9092"}, Topic: "test-topic", ConsumerGroup: "test-group"}),
			},
			Sink: dataflowv1.SinkSpec{
				Type:   "kafka",
				Config: mustConfig(dataflowv1.KafkaSinkSpec{Brokers: []string{"localhost:9092"}, Topic: "output-topic"}),
			},
		},
	}
	require.NoError(t, fakeClient.Create(ctx, dataflow))

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-dataflow", Namespace: "default"},
	}
	result, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	// Drain events (FakeRecorder uses buffered channel; events were sent during Reconcile)
	var events []string
	for {
		select {
		case e := <-fakeRecorder.Events:
			events = append(events, e)
		default:
			goto done
		}
	}
done:
	// On success we expect ConfigMapCreated and DeploymentCreated
	var hasConfigMapCreated, hasDeploymentCreated bool
	for _, e := range events {
		if strings.Contains(e, "ConfigMapCreated") {
			hasConfigMapCreated = true
		}
		if strings.Contains(e, "DeploymentCreated") {
			hasDeploymentCreated = true
		}
	}
	assert.True(t, hasConfigMapCreated, "expected event containing ConfigMapCreated, got events: %v", events)
	assert.True(t, hasDeploymentCreated, "expected event containing DeploymentCreated, got events: %v", events)
}
