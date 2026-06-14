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
	"time"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// AnnotationCheckpointResetAppliedAt records when the controller last wrote checkpointReset into a spec ConfigMap.
const AnnotationCheckpointResetAppliedAt = "dataflow.dataflow.io/checkpoint-reset-applied-at"

func checkpointResetMarkersPending(spec *dataflowv1.DataFlowSpec, annotations map[string]string) bool {
	if dataflowv1.CheckpointResetRequested(spec) {
		return true
	}
	return annotations != nil && annotations[dataflowv1.AnnotationResetCheckpoint] == "true"
}

func applyCheckpointResetMarkers(spec *dataflowv1.DataFlowSpec, annotations map[string]string, resolvedSpec *dataflowv1.DataFlowSpec) bool {
	if resolvedSpec == nil || !checkpointResetMarkersPending(spec, annotations) {
		return false
	}
	trueVal := true
	resolvedSpec.CheckpointReset = &trueVal
	return true
}

func clearCheckpointResetMarkers(spec *dataflowv1.DataFlowSpec, annotations map[string]string) bool {
	changed := false
	if spec != nil && spec.CheckpointReset != nil && *spec.CheckpointReset {
		spec.CheckpointReset = nil
		changed = true
	}
	if annotations != nil {
		if _, ok := annotations[dataflowv1.AnnotationResetCheckpoint]; ok {
			delete(annotations, dataflowv1.AnnotationResetCheckpoint)
			changed = true
		}
	}
	return changed
}

func setCheckpointResetAppliedAt(cm *corev1.ConfigMap, applied bool) {
	if cm == nil {
		return
	}
	if applied {
		if cm.Annotations == nil {
			cm.Annotations = map[string]string{}
		}
		cm.Annotations[AnnotationCheckpointResetAppliedAt] = metav1.Now().Format(time.RFC3339Nano)
		return
	}
	if cm.Annotations != nil {
		delete(cm.Annotations, AnnotationCheckpointResetAppliedAt)
	}
}

func checkpointResetAppliedAt(cm *corev1.ConfigMap) (time.Time, bool) {
	if cm == nil || cm.Annotations == nil {
		return time.Time{}, false
	}
	raw, ok := cm.Annotations[AnnotationCheckpointResetAppliedAt]
	if !ok || raw == "" {
		return time.Time{}, false
	}
	appliedAt, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, false
	}
	return appliedAt, true
}

func processorJobStartedAfterCheckpointReset(job *batchv1.Job, appliedAt time.Time) bool {
	if job == nil || job.Status.StartTime == nil {
		return false
	}
	return !job.Status.StartTime.Time.Before(appliedAt)
}

// checkpointResetPending reports whether the DataFlow CR still carries one-shot checkpoint reset markers.
func checkpointResetPending(df *dataflowv1.DataFlow) bool {
	if df == nil {
		return false
	}
	return checkpointResetMarkersPending(&df.Spec, df.Annotations)
}

// checkpointResetPendingCron reports whether the DataFlowCron CR still carries one-shot checkpoint reset markers.
func checkpointResetPendingCron(dfc *dataflowv1.DataFlowCron) bool {
	if dfc == nil {
		return false
	}
	return checkpointResetMarkersPending(&dfc.Spec.DataFlowSpec, dfc.Annotations)
}

// applyCheckpointResetIntent sets resolvedSpec.CheckpointReset when the user requested a one-shot reset.
func applyCheckpointResetIntent(df *dataflowv1.DataFlow, resolvedSpec *dataflowv1.DataFlowSpec) bool {
	if df == nil {
		return false
	}
	return applyCheckpointResetMarkers(&df.Spec, df.Annotations, resolvedSpec)
}

// applyCheckpointResetIntentCron sets resolvedSpec.CheckpointReset when the user requested a one-shot reset.
func applyCheckpointResetIntentCron(dfc *dataflowv1.DataFlowCron, resolvedSpec *dataflowv1.DataFlowSpec) bool {
	if dfc == nil {
		return false
	}
	return applyCheckpointResetMarkers(&dfc.Spec.DataFlowSpec, dfc.Annotations, resolvedSpec)
}

// processorDeploymentRolloutReady reports whether the processor Deployment finished rolling out the current spec.
func processorDeploymentRolloutReady(deployment *appsv1.Deployment, desired int32) bool {
	if deployment == nil || desired <= 0 {
		return false
	}
	status := deployment.Status
	if deployment.Generation > status.ObservedGeneration {
		return false
	}
	return status.UpdatedReplicas >= desired && status.ReadyReplicas >= desired
}

// tryConsumeCheckpointResetAfterRollout clears one-shot reset markers only after the processor Deployment rollout completes.
func (r *DataFlowReconciler) tryConsumeCheckpointResetAfterRollout(
	ctx context.Context,
	req types.NamespacedName,
	dataflow *dataflowv1.DataFlow,
	deployment *appsv1.Deployment,
	deploymentFound bool,
) error {
	if !checkpointResetPending(dataflow) || !deploymentFound {
		return nil
	}
	desired := desiredProcessorReplicas(&dataflow.Spec)
	if !processorDeploymentRolloutReady(deployment, desired) {
		return nil
	}
	return r.consumeCheckpointResetFlags(ctx, req)
}

// consumeCheckpointResetFlags clears one-shot reset markers from the DataFlow CR after rollout-ready pods applied them.
func (r *DataFlowReconciler) consumeCheckpointResetFlags(ctx context.Context, req types.NamespacedName) error {
	var df dataflowv1.DataFlow
	if err := r.Get(ctx, req, &df); err != nil {
		return err
	}
	patch := client.MergeFrom(df.DeepCopy())
	if !clearCheckpointResetMarkers(&df.Spec, df.Annotations) {
		return nil
	}
	return r.Patch(ctx, &df, patch)
}

// tryConsumeCheckpointResetAfterProcessorJob clears one-shot reset markers after a processor Job applied them.
func (r *DataFlowCronReconciler) tryConsumeCheckpointResetAfterProcessorJob(
	ctx context.Context,
	req types.NamespacedName,
	dfc *dataflowv1.DataFlowCron,
	jobs []batchv1.Job,
	specConfigMapName string,
) (bool, error) {
	if !checkpointResetPendingCron(dfc) {
		return false, nil
	}
	var cm corev1.ConfigMap
	if err := r.Get(ctx, types.NamespacedName{Name: specConfigMapName, Namespace: dfc.Namespace}, &cm); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	appliedAt, ok := checkpointResetAppliedAt(&cm)
	if !ok {
		return false, nil
	}
	for i := range jobs {
		job := &jobs[i]
		if job.Labels[dataFlowCronTriggerIndexLabel] != dataFlowCronProcessorStepLabel {
			continue
		}
		if !isJobSucceeded(job) && !isJobFailed(job) {
			continue
		}
		if !processorJobStartedAfterCheckpointReset(job, appliedAt) {
			continue
		}
		if err := r.consumeCheckpointResetFlags(ctx, req); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// consumeCheckpointResetFlags clears one-shot reset markers from the DataFlowCron CR after a processor Job applied them.
func (r *DataFlowCronReconciler) consumeCheckpointResetFlags(ctx context.Context, req types.NamespacedName) error {
	var dfc dataflowv1.DataFlowCron
	if err := r.Get(ctx, req, &dfc); err != nil {
		return err
	}
	patch := client.MergeFrom(dfc.DeepCopy())
	if !clearCheckpointResetMarkers(&dfc.Spec.DataFlowSpec, dfc.Annotations) {
		return nil
	}
	return r.Patch(ctx, &dfc, patch)
}
