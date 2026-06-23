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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
)

// syncMaintenanceStatus updates dataflow.Status.MaintenanceStatus from spec and current time.
// Returns whether the processor should be paused and any evaluation error.
func syncMaintenanceStatus(dataflow *dataflowv1.DataFlow, now time.Time) (paused bool, err error) {
	if dataflow == nil {
		return false, nil
	}

	status := dataflow.Status.MaintenanceStatus
	if status == nil {
		status = &dataflowv1.MaintenanceStatus{}
		dataflow.Status.MaintenanceStatus = status
	}

	spec := dataflow.Spec.Maintenance
	status.Suspended = dataflowv1.IsMaintenanceSuspended(spec)
	status.InMaintenance = false
	status.NextMaintenanceTime = nil

	if status.Suspended {
		return true, nil
	}

	if spec == nil || spec.StartTime == "" || spec.Duration == "" {
		return false, nil
	}

	result, evalErr := dataflowv1.EvaluateMaintenanceWindow(spec, now)
	if evalErr != nil {
		return false, evalErr
	}

	status.InMaintenance = result.InWindow
	if !result.NextWindowStart.IsZero() {
		t := metav1.NewTime(result.NextWindowStart)
		status.NextMaintenanceTime = &t
	}
	if result.InWindow && !result.CurrentWindowStart.IsZero() {
		t := metav1.NewTime(result.CurrentWindowStart)
		status.LastMaintenanceTime = &t
	}

	return result.InWindow, nil
}

func processorPausedMessage(dataflow *dataflowv1.DataFlow) string {
	if dataflow == nil {
		return "Processor scaled to zero replicas"
	}
	if dataflow.Status.MaintenanceStatus != nil && dataflow.Status.MaintenanceStatus.Suspended {
		return "Processor suspended manually"
	}
	if dataflow.Status.MaintenanceStatus != nil && dataflow.Status.MaintenanceStatus.InMaintenance {
		return "Processor paused for scheduled maintenance window"
	}
	return "Processor scaled to zero replicas"
}
