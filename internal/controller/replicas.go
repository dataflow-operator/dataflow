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

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
)

// desiredProcessorReplicas returns the processor Deployment replica count from spec.
// Scales to zero when maintenance suspension or a scheduled window is active.
func desiredProcessorReplicas(spec *dataflowv1.DataFlowSpec) int32 {
	return desiredProcessorReplicasAt(spec, time.Now())
}

func desiredProcessorReplicasAt(spec *dataflowv1.DataFlowSpec, now time.Time) int32 {
	if spec == nil {
		return 1
	}
	paused, err := dataflowv1.IsProcessorPaused(spec, now)
	if err == nil && paused {
		return 0
	}
	if spec.Replicas == nil {
		return 1
	}
	return *spec.Replicas
}
