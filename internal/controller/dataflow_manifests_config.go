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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/pkg/k8snames"
)

// createOrUpdateConfigMap creates or updates ConfigMap with spec.
func (r *DataFlowReconciler) createOrUpdateConfigMap(ctx context.Context, req ctrl.Request, spec *dataflowv1.DataFlowSpec) error {
	log := log.FromContext(ctx)

	// Serialize spec to JSON
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("failed to marshal spec: %w", err)
	}

	configMapName := k8snames.ProcessorSpecConfigMap(req.Name)
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: req.Namespace,
		},
		Data: map[string]string{
			"spec.json": string(specJSON),
		},
	}

	// Get DataFlow to set owner reference
	var df dataflowv1.DataFlow
	if err := r.Get(ctx, req.NamespacedName, &df); err != nil {
		return fmt.Errorf("failed to get DataFlow: %w", err)
	}

	// Set owner reference
	if err := ctrl.SetControllerReference(&df, configMap, r.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %w", err)
	}

	// Check if ConfigMap exists
	existing := &corev1.ConfigMap{}
	err = r.Get(ctx, types.NamespacedName{Name: configMapName, Namespace: req.Namespace}, existing)
	if err != nil && apierrors.IsNotFound(err) {
		// Ensure finalizer before creating first child so deletion is coordinated
		if err := r.ensureDataFlowFinalizer(ctx, req); err != nil {
			return fmt.Errorf("failed to ensure DataFlow finalizer: %w", err)
		}
		// Create new ConfigMap
		if err := r.Create(ctx, configMap); err != nil {
			return fmt.Errorf("failed to create ConfigMap: %w", err)
		}
		log.Info("Created ConfigMap", "name", configMapName)
		if r.Recorder != nil {
			r.Recorder.Eventf(&df, corev1.EventTypeNormal, "ConfigMapCreated", "Created ConfigMap %s", configMapName)
			log.V(1).Info("Emitted Kubernetes event", "reason", "ConfigMapCreated", "object", configMapName)
		}
	} else if err != nil {
		return fmt.Errorf("failed to get ConfigMap: %w", err)
	} else {
		// Update existing ConfigMap
		existing.Data = configMap.Data
		if err := r.Update(ctx, existing); err != nil {
			return fmt.Errorf("failed to update ConfigMap: %w", err)
		}
		log.Info("Updated ConfigMap", "name", configMapName)
		if r.Recorder != nil {
			r.Recorder.Eventf(&df, corev1.EventTypeNormal, "ConfigMapUpdated", "Updated ConfigMap %s", configMapName)
			log.V(1).Info("Emitted Kubernetes event", "reason", "ConfigMapUpdated", "object", configMapName)
		}
	}

	return nil
}

// createOrUpdateCheckpointConfigMap creates an empty ConfigMap for checkpoint persistence.
func (r *DataFlowReconciler) createOrUpdateCheckpointConfigMap(ctx context.Context, req ctrl.Request, dataflow *dataflowv1.DataFlow) error {
	log := log.FromContext(ctx)
	configMapName := k8snames.ProcessorCheckpointConfigMap(dataflow.Name)
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: req.Namespace,
		},
		Data: map[string]string{},
	}
	if err := ctrl.SetControllerReference(dataflow, configMap, r.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %w", err)
	}
	existing := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Name: configMapName, Namespace: req.Namespace}, existing)
	if err != nil && apierrors.IsNotFound(err) {
		if err := r.Create(ctx, configMap); err != nil {
			return fmt.Errorf("failed to create checkpoint ConfigMap: %w", err)
		}
		log.Info("Created checkpoint ConfigMap", "name", configMapName)
		if r.Recorder != nil {
			r.Recorder.Eventf(dataflow, corev1.EventTypeNormal, "CheckpointConfigMapCreated", "Created checkpoint ConfigMap %s", configMapName)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get checkpoint ConfigMap: %w", err)
	}
	return nil
}
