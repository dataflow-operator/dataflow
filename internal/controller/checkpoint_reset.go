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

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// checkpointResetPending reports whether the DataFlow CR still carries one-shot checkpoint reset markers.
func checkpointResetPending(df *dataflowv1.DataFlow) bool {
	if df == nil {
		return false
	}
	if dataflowv1.CheckpointResetRequested(&df.Spec) {
		return true
	}
	return df.Annotations != nil && df.Annotations[dataflowv1.AnnotationResetCheckpoint] == "true"
}

// applyCheckpointResetIntent sets resolvedSpec.CheckpointReset when the user requested a one-shot reset.
func applyCheckpointResetIntent(df *dataflowv1.DataFlow, resolvedSpec *dataflowv1.DataFlowSpec) bool {
	if df == nil || resolvedSpec == nil {
		return false
	}
	if !checkpointResetPending(df) {
		return false
	}
	trueVal := true
	resolvedSpec.CheckpointReset = &trueVal
	return true
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
	changed := false
	if df.Spec.CheckpointReset != nil && *df.Spec.CheckpointReset {
		df.Spec.CheckpointReset = nil
		changed = true
	}
	if df.Annotations != nil {
		if _, ok := df.Annotations[dataflowv1.AnnotationResetCheckpoint]; ok {
			delete(df.Annotations, dataflowv1.AnnotationResetCheckpoint)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return r.Patch(ctx, &df, patch)
}
