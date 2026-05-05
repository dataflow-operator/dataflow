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
	"fmt"
	"math/rand"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
)

const (
	conditionReady                    = "Ready"
	conditionProcessorDeploymentReady = "ProcessorDeploymentReady"
	conditionSpecResolved             = "SpecResolved"
)

func buildStatusConditions(phase, message string, specResolved bool, deploymentReady bool, deploymentReason string) []metav1.Condition {
	now := metav1.Now()
	readyStatus := metav1.ConditionFalse
	readyReason := "NotReady"
	if phase == "Running" {
		readyStatus = metav1.ConditionTrue
		readyReason = "Running"
	}
	if phase == "Stopped" {
		readyReason = "Stopped"
	}
	if phase == "Error" {
		readyReason = "Error"
	}

	specStatus := metav1.ConditionFalse
	specReason := "ResolutionFailed"
	specMessage := "DataFlow spec resolution failed"
	if specResolved {
		specStatus = metav1.ConditionTrue
		specReason = "Resolved"
		specMessage = "DataFlow spec resolved"
	}

	deploymentStatus := metav1.ConditionFalse
	if deploymentReady {
		deploymentStatus = metav1.ConditionTrue
	}
	if deploymentReason == "" {
		deploymentReason = "Unknown"
	}

	conditions := []metav1.Condition{}
	meta.SetStatusCondition(&conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             readyStatus,
		Reason:             readyReason,
		Message:            message,
		LastTransitionTime: now,
	})
	meta.SetStatusCondition(&conditions, metav1.Condition{
		Type:               conditionSpecResolved,
		Status:             specStatus,
		Reason:             specReason,
		Message:            specMessage,
		LastTransitionTime: now,
	})
	meta.SetStatusCondition(&conditions, metav1.Condition{
		Type:               conditionProcessorDeploymentReady,
		Status:             deploymentStatus,
		Reason:             deploymentReason,
		Message:            message,
		LastTransitionTime: now,
	})
	return conditions
}

// updateStatusWithRetry updates DataFlow status with retry logic to handle optimistic locking conflicts.
func (r *DataFlowReconciler) updateStatusWithRetry(ctx context.Context, req ctrl.Request, updateFn func(*dataflowv1.DataFlow)) error {
	log := log.FromContext(ctx)
	maxRetries := 5

	for attempt := 0; attempt < maxRetries; attempt++ {
		var df dataflowv1.DataFlow
		if err := r.Get(ctx, req.NamespacedName, &df); err != nil {
			if apierrors.IsNotFound(err) {
				// If object not found, no point retrying
				return err
			}
			if attempt < maxRetries-1 {
				log.Error(err, "unable to fetch DataFlow for status update, retrying", "attempt", attempt+1, "maxRetries", maxRetries)
				// Exponential backoff with jitter: base delay * (2^attempt) + random delay up to 50ms
				baseDelay := time.Duration(1<<uint(attempt)) * 200 * time.Millisecond
				jitter := time.Duration(rand.Intn(50)) * time.Millisecond
				time.Sleep(baseDelay + jitter)
				continue
			}
			return fmt.Errorf("failed to fetch DataFlow after %d attempts: %w", maxRetries, err)
		}

		// Apply status update function
		updateFn(&df)

		if err := r.Status().Update(ctx, &df); err != nil {
			if apierrors.IsConflict(err) {
				if attempt < maxRetries-1 {
					log.Info("status update conflict, retrying", "attempt", attempt+1, "maxRetries", maxRetries)
					// Exponential backoff with jitter: base delay * (2^attempt) + random delay up to 50ms
					baseDelay := time.Duration(1<<uint(attempt)) * 200 * time.Millisecond
					jitter := time.Duration(rand.Intn(50)) * time.Millisecond
					time.Sleep(baseDelay + jitter)
					continue
				}
				// After all attempts return conflict to trigger requeue
				return err
			}
			// For other errors return immediately
			return err
		}

		// Successful update
		if attempt > 0 {
			log.Info("Successfully updated DataFlow status after retry", "attempt", attempt+1)
		}
		return nil
	}

	return fmt.Errorf("failed to update status after %d attempts", maxRetries)
}
