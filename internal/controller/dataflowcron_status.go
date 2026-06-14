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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
)

// updateDataFlowCronStatusWithRetry updates DataFlowCron status with retry logic to handle optimistic locking conflicts.
func (r *DataFlowCronReconciler) updateDataFlowCronStatusWithRetry(ctx context.Context, req ctrl.Request, updateFn func(*dataflowv1.DataFlowCron)) error {
	log := log.FromContext(ctx)
	maxRetries := 5

	for attempt := 0; attempt < maxRetries; attempt++ {
		var dfc dataflowv1.DataFlowCron
		if err := r.Get(ctx, req.NamespacedName, &dfc); err != nil {
			if apierrors.IsNotFound(err) {
				return err
			}
			if attempt < maxRetries-1 {
				log.Error(err, "unable to fetch DataFlowCron for status update, retrying", "attempt", attempt+1, "maxRetries", maxRetries)
				time.Sleep(statusRetryDelay(attempt))
				continue
			}
			return fmt.Errorf("failed to fetch DataFlowCron after %d attempts: %w", maxRetries, err)
		}

		updateFn(&dfc)

		if err := r.Status().Update(ctx, &dfc); err != nil {
			if apierrors.IsConflict(err) {
				if attempt < maxRetries-1 {
					log.Info("DataFlowCron status update conflict, retrying", "attempt", attempt+1, "maxRetries", maxRetries)
					time.Sleep(statusRetryDelay(attempt))
					continue
				}
				return err
			}
			return err
		}

		if attempt > 0 {
			log.Info("Successfully updated DataFlowCron status after retry", "attempt", attempt+1)
		}
		return nil
	}

	return fmt.Errorf("failed to update DataFlowCron status after %d attempts", maxRetries)
}

func (r *DataFlowCronReconciler) patchDataFlowCronWithRetry(ctx context.Context, req ctrl.Request, mutateFn func(*dataflowv1.DataFlowCron) bool) error {
	log := log.FromContext(ctx)
	maxRetries := 5

	for attempt := 0; attempt < maxRetries; attempt++ {
		var dfc dataflowv1.DataFlowCron
		if err := r.Get(ctx, req.NamespacedName, &dfc); err != nil {
			if apierrors.IsNotFound(err) {
				return err
			}
			if attempt < maxRetries-1 {
				log.Error(err, "unable to fetch DataFlowCron for patch, retrying", "attempt", attempt+1, "maxRetries", maxRetries)
				time.Sleep(statusRetryDelay(attempt))
				continue
			}
			return fmt.Errorf("failed to fetch DataFlowCron after %d attempts: %w", maxRetries, err)
		}

		patch := client.MergeFrom(dfc.DeepCopy())
		if !mutateFn(&dfc) {
			return nil
		}

		if err := r.Patch(ctx, &dfc, patch); err != nil {
			if apierrors.IsConflict(err) {
				if attempt < maxRetries-1 {
					log.Info("DataFlowCron patch conflict, retrying", "attempt", attempt+1, "maxRetries", maxRetries)
					time.Sleep(statusRetryDelay(attempt))
					continue
				}
				return err
			}
			return err
		}

		if attempt > 0 {
			log.Info("Successfully patched DataFlowCron after retry", "attempt", attempt+1)
		}
		return nil
	}

	return fmt.Errorf("failed to patch DataFlowCron after %d attempts", maxRetries)
}

func statusRetryDelay(attempt int) time.Duration {
	baseDelay := time.Duration(1<<uint(attempt)) * 200 * time.Millisecond
	jitter := time.Duration(rand.Intn(50)) * time.Millisecond
	return baseDelay + jitter
}

func requeueOnConflict(err error) (ctrl.Result, error) {
	if apierrors.IsConflict(err) {
		return ctrl.Result{Requeue: true}, nil
	}
	return ctrl.Result{}, err
}
