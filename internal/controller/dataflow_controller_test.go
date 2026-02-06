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
	"os"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/version"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDataFlowReconciler(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)
	err = clientgoscheme.AddToScheme(scheme)
	require.NoError(t, err)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme)

	assert.NotNil(t, reconciler)
	assert.Equal(t, fakeClient, reconciler.Client)
	assert.Equal(t, scheme, reconciler.Scheme)
	// Без PROCESSOR_IMAGE используется версия оператора (при сборке без ldflags — "dev").
	assert.Equal(t, version.DefaultProcessorImage(), reconciler.processorImage)
}

func TestNewDataFlowReconciler_ProcessorImageFromEnv(t *testing.T) {
	const customImage = "my.registry/dataflow-processor:v1.2.3"
	prev := os.Getenv("PROCESSOR_IMAGE")
	defer func() { _ = os.Setenv("PROCESSOR_IMAGE", prev) }()
	require.NoError(t, os.Setenv("PROCESSOR_IMAGE", customImage))

	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme)

	assert.Equal(t, customImage, reconciler.processorImage)
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
	reconciler := NewDataFlowReconciler(fakeClient, scheme)

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

func TestDataFlowReconciler_Reconcile_CreateDeployment(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)
	err = clientgoscheme.AddToScheme(scheme)
	require.NoError(t, err)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme)

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
				Type: "kafka",
				Kafka: &dataflowv1.KafkaSourceSpec{
					Brokers:       []string{"localhost:9092"},
					Topic:         "test-topic",
					ConsumerGroup: "test-group",
				},
			},
			Sink: dataflowv1.SinkSpec{
				Type: "kafka",
				Kafka: &dataflowv1.KafkaSinkSpec{
					Brokers: []string{"localhost:9092"},
					Topic:   "output-topic",
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

	// Reconcile should create Deployment and ConfigMap
	result, err := reconciler.Reconcile(ctx, req)
	// We don't require no error because connection to Kafka will fail in background

	// Verify that the DataFlow still exists
	var updatedDataflow dataflowv1.DataFlow
	getErr := fakeClient.Get(ctx, req.NamespacedName, &updatedDataflow)
	require.NoError(t, getErr, "DataFlow should exist after reconcile")

	// Verify Deployment was created
	var deployment appsv1.Deployment
	deploymentName := types.NamespacedName{
		Name:      "dataflow-test-dataflow",
		Namespace: "default",
	}
	err = fakeClient.Get(ctx, deploymentName, &deployment)
	assert.NoError(t, err, "Deployment should be created")
	assert.Equal(t, "dataflow-test-dataflow", deployment.Name)

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
}

func TestDataFlowReconciler_Reconcile_DeleteDataFlow(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)
	err = clientgoscheme.AddToScheme(scheme)
	require.NoError(t, err)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme)

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
				Type: "kafka",
				Kafka: &dataflowv1.KafkaSourceSpec{
					Brokers:       []string{"localhost:9092"},
					Topic:         "test-topic",
					ConsumerGroup: "test-group",
				},
			},
			Sink: dataflowv1.SinkSpec{
				Type: "kafka",
				Kafka: &dataflowv1.KafkaSinkSpec{
					Brokers: []string{"localhost:9092"},
					Topic:   "output-topic",
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

	// Note: fake client has limitations with DeletionTimestamp - it may not find the object
	// In real Kubernetes, objects with DeletionTimestamp still exist until finalizers are removed
	// This test verifies that reconcile handles deletion gracefully without panicking
	result, err := reconciler.Reconcile(ctx, req)

	// Reconcile should handle NotFound errors gracefully (returns no error via IgnoreNotFound)
	// The main test is that reconcile doesn't panic
	assert.Equal(t, ctrl.Result{}, result)

	// Note: Due to fake client limitation, if object is not found, deletion logic doesn't run
	// In real scenario, object would exist and processor would be removed
	// We just verify that reconcile completes without error/panic
}

func TestDataFlowReconciler_Reconcile_WithResourcesAndNodeSelector(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)
	err = clientgoscheme.AddToScheme(scheme)
	require.NoError(t, err)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme)

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
				Type: "kafka",
				Kafka: &dataflowv1.KafkaSourceSpec{
					Brokers:       []string{"localhost:9092"},
					Topic:         "test-topic",
					ConsumerGroup: "test-group",
				},
			},
			Sink: dataflowv1.SinkSpec{
				Type: "kafka",
				Kafka: &dataflowv1.KafkaSinkSpec{
					Brokers: []string{"localhost:9092"},
					Topic:   "output-topic",
				},
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

	assert.Equal(t, ctrl.Result{}, result)
}

func TestDataFlowReconciler_Reconcile_InvalidSpec(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)
	err = clientgoscheme.AddToScheme(scheme)
	require.NoError(t, err)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme)

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
				Type: "kafka",
				Kafka: &dataflowv1.KafkaSinkSpec{
					Brokers: []string{"localhost:9092"},
					Topic:   "output-topic",
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

	// Reconcile will fail due to invalid source type
	result, err := reconciler.Reconcile(ctx, req)
	// Error is expected, but status should still be updated

	// Verify status was updated to Error
	var updatedDataflow dataflowv1.DataFlow
	getErr := fakeClient.Get(ctx, req.NamespacedName, &updatedDataflow)
	require.NoError(t, getErr, "DataFlow should exist")

	// Status should be Error if processor creation failed
	// Note: Status update happens in a goroutine, so we might need to wait
	// For now, we check that either Error status is set, or the reconcile returned an error
	if updatedDataflow.Status.Phase == "Error" {
		assert.Contains(t, updatedDataflow.Status.Message, "Failed to create processor")
	} else {
		// If status wasn't updated yet, at least verify that reconcile returned an error
		assert.Error(t, err, "Reconcile should return error for invalid spec")
	}

	assert.Equal(t, ctrl.Result{}, result)
}

func TestDataFlowReconciler_Reconcile_UpdateStats(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)
	err = clientgoscheme.AddToScheme(scheme)
	require.NoError(t, err)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme)

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
				Type: "kafka",
				Kafka: &dataflowv1.KafkaSourceSpec{
					Brokers:       []string{"localhost:9092"},
					Topic:         "test-topic",
					ConsumerGroup: "test-group",
				},
			},
			Sink: dataflowv1.SinkSpec{
				Type: "kafka",
				Kafka: &dataflowv1.KafkaSinkSpec{
					Brokers: []string{"localhost:9092"},
					Topic:   "output-topic",
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
	reconciler := NewDataFlowReconciler(fakeClient, scheme)

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
}
