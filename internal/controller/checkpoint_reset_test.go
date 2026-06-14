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
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyCheckpointResetIntent(t *testing.T) {
	t.Parallel()

	trueVal := true
	df := &dataflowv1.DataFlow{}
	df.Annotations = map[string]string{dataflowv1.AnnotationResetCheckpoint: "true"}
	resolved := &dataflowv1.DataFlowSpec{}
	assert.True(t, applyCheckpointResetIntent(df, resolved))
	assert.NotNil(t, resolved.CheckpointReset)
	assert.True(t, *resolved.CheckpointReset)

	df2 := &dataflowv1.DataFlow{Spec: dataflowv1.DataFlowSpec{CheckpointReset: &trueVal}}
	resolved2 := &dataflowv1.DataFlowSpec{}
	assert.True(t, applyCheckpointResetIntent(df2, resolved2))
}

func TestCheckpointResetPending(t *testing.T) {
	t.Parallel()

	trueVal := true
	assert.False(t, checkpointResetPending(nil))
	assert.False(t, checkpointResetPending(&dataflowv1.DataFlow{}))
	assert.True(t, checkpointResetPending(&dataflowv1.DataFlow{
		Spec: dataflowv1.DataFlowSpec{CheckpointReset: &trueVal},
	}))
	assert.True(t, checkpointResetPending(&dataflowv1.DataFlow{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{dataflowv1.AnnotationResetCheckpoint: "true"},
		},
	}))
}

func TestProcessorDeploymentRolloutReady(t *testing.T) {
	t.Parallel()

	replicas := int32(1)
	base := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Generation: 2},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
	}

	assert.False(t, processorDeploymentRolloutReady(nil, 1))
	assert.False(t, processorDeploymentRolloutReady(base, 0))

	notObserved := base.DeepCopy()
	notObserved.Status.ObservedGeneration = 1
	notObserved.Status.UpdatedReplicas = 1
	notObserved.Status.ReadyReplicas = 1
	assert.False(t, processorDeploymentRolloutReady(notObserved, 1))

	oldPodsReady := base.DeepCopy()
	oldPodsReady.Status.ObservedGeneration = 2
	oldPodsReady.Status.UpdatedReplicas = 0
	oldPodsReady.Status.ReadyReplicas = 1
	assert.False(t, processorDeploymentRolloutReady(oldPodsReady, 1))

	newPodsNotReady := base.DeepCopy()
	newPodsNotReady.Status.ObservedGeneration = 2
	newPodsNotReady.Status.UpdatedReplicas = 1
	newPodsNotReady.Status.ReadyReplicas = 0
	assert.False(t, processorDeploymentRolloutReady(newPodsNotReady, 1))

	ready := base.DeepCopy()
	ready.Status.ObservedGeneration = 2
	ready.Status.UpdatedReplicas = 1
	ready.Status.ReadyReplicas = 1
	assert.True(t, processorDeploymentRolloutReady(ready, 1))

	scaled := int32(3)
	scaledDeployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Generation: 4},
		Spec:       appsv1.DeploymentSpec{Replicas: &scaled},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 4,
			UpdatedReplicas:    3,
			ReadyReplicas:      3,
		},
	}
	assert.True(t, processorDeploymentRolloutReady(scaledDeployment, 3))
}

func TestDataFlowReconciler_CheckpointResetConsumedAfterRolloutReady(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, dataflowv1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))

	trueVal := true
	dataflow := &dataflowv1.DataFlow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "dataflow.dataflow.io/v1",
			Kind:       "DataFlow",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "reset-test",
			Namespace: "default",
		},
		Spec: dataflowv1.DataFlowSpec{
			CheckpointReset: &trueVal,
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

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dataflow).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme, nil)

	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "reset-test", Namespace: "default"}}

	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	var afterFirst dataflowv1.DataFlow
	require.NoError(t, fakeClient.Get(ctx, req.NamespacedName, &afterFirst))
	require.NotNil(t, afterFirst.Spec.CheckpointReset)
	assert.True(t, *afterFirst.Spec.CheckpointReset, "reset flag must remain until rollout is ready")

	var configMapAfterFirst struct {
		CheckpointReset *bool `json:"checkpointReset"`
	}
	cmName := types.NamespacedName{Name: "df-reset-test-spec", Namespace: "default"}
	var cm corev1.ConfigMap
	require.NoError(t, fakeClient.Get(ctx, cmName, &cm))
	require.NoError(t, json.Unmarshal([]byte(cm.Data["spec.json"]), &configMapAfterFirst))
	require.NotNil(t, configMapAfterFirst.CheckpointReset)
	assert.True(t, *configMapAfterFirst.CheckpointReset)

	var deployment appsv1.Deployment
	deploymentName := types.NamespacedName{Name: "df-reset-test", Namespace: "default"}
	require.NoError(t, fakeClient.Get(ctx, deploymentName, &deployment))
	deployment.Status.ObservedGeneration = deployment.Generation
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.ReadyReplicas = 1
	require.NoError(t, fakeClient.Status().Update(ctx, &deployment))

	_, err = reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	var afterSecond dataflowv1.DataFlow
	require.NoError(t, fakeClient.Get(ctx, req.NamespacedName, &afterSecond))
	assert.Nil(t, afterSecond.Spec.CheckpointReset, "reset flag must be consumed after rollout is ready")
}

func TestApplyCheckpointResetIntentCron(t *testing.T) {
	t.Parallel()

	trueVal := true
	dfc := &dataflowv1.DataFlowCron{}
	dfc.Annotations = map[string]string{dataflowv1.AnnotationResetCheckpoint: "true"}
	resolved := &dataflowv1.DataFlowSpec{}
	assert.True(t, applyCheckpointResetIntentCron(dfc, resolved))
	assert.NotNil(t, resolved.CheckpointReset)
	assert.True(t, *resolved.CheckpointReset)

	dfc2 := &dataflowv1.DataFlowCron{Spec: dataflowv1.DataFlowCronSpec{
		DataFlowSpec: dataflowv1.DataFlowSpec{CheckpointReset: &trueVal},
	}}
	resolved2 := &dataflowv1.DataFlowSpec{}
	assert.True(t, applyCheckpointResetIntentCron(dfc2, resolved2))
}

func TestCheckpointResetPendingCron(t *testing.T) {
	t.Parallel()

	trueVal := true
	assert.False(t, checkpointResetPendingCron(nil))
	assert.False(t, checkpointResetPendingCron(&dataflowv1.DataFlowCron{}))
	assert.True(t, checkpointResetPendingCron(&dataflowv1.DataFlowCron{
		Spec: dataflowv1.DataFlowCronSpec{
			DataFlowSpec: dataflowv1.DataFlowSpec{CheckpointReset: &trueVal},
		},
	}))
	assert.True(t, checkpointResetPendingCron(&dataflowv1.DataFlowCron{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{dataflowv1.AnnotationResetCheckpoint: "true"},
		},
	}))
}

func TestProcessorJobStartedAfterCheckpointReset(t *testing.T) {
	t.Parallel()

	appliedAt := time.Date(2026, 6, 14, 12, 0, 0, 0, time.UTC)
	before := metav1.NewTime(appliedAt.Add(-time.Minute))
	after := metav1.NewTime(appliedAt.Add(time.Minute))

	assert.False(t, processorJobStartedAfterCheckpointReset(nil, appliedAt))
	assert.False(t, processorJobStartedAfterCheckpointReset(&batchv1.Job{}, appliedAt))
	assert.False(t, processorJobStartedAfterCheckpointReset(&batchv1.Job{
		Status: batchv1.JobStatus{StartTime: &before},
	}, appliedAt))
	assert.True(t, processorJobStartedAfterCheckpointReset(&batchv1.Job{
		Status: batchv1.JobStatus{StartTime: &after},
	}, appliedAt))
	assert.True(t, processorJobStartedAfterCheckpointReset(&batchv1.Job{
		Status: batchv1.JobStatus{StartTime: &metav1.Time{Time: appliedAt}},
	}, appliedAt))
}
