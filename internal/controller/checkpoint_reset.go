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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// applyCheckpointResetIntent sets resolvedSpec.CheckpointReset when the user requested a one-shot reset.
func applyCheckpointResetIntent(df *dataflowv1.DataFlow, resolvedSpec *dataflowv1.DataFlowSpec) bool {
	if df == nil || resolvedSpec == nil {
		return false
	}
	reset := dataflowv1.CheckpointResetRequested(&df.Spec)
	if df.Annotations != nil && df.Annotations[dataflowv1.AnnotationResetCheckpoint] == "true" {
		reset = true
	}
	if !reset {
		return false
	}
	trueVal := true
	resolvedSpec.CheckpointReset = &trueVal
	return true
}

// consumeCheckpointResetFlags clears one-shot reset markers from the DataFlow CR after they were propagated to the processor spec.
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
