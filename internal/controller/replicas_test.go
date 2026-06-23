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
	"testing"
	"time"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/stretchr/testify/assert"
)

func boolPtrReplicas(b bool) *bool { return &b }

func TestDesiredProcessorReplicas(t *testing.T) {
	assert.Equal(t, int32(1), desiredProcessorReplicas(nil))
	assert.Equal(t, int32(1), desiredProcessorReplicas(&dataflowv1.DataFlowSpec{}))
	r := int32(3)
	assert.Equal(t, int32(3), desiredProcessorReplicas(&dataflowv1.DataFlowSpec{Replicas: &r}))

	suspended := &dataflowv1.DataFlowSpec{
		Replicas: &r,
		Maintenance: &dataflowv1.MaintenanceSpec{
			Suspended: boolPtrReplicas(true),
		},
	}
	assert.Equal(t, int32(0), desiredProcessorReplicas(suspended))

	future := time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)
	window := &dataflowv1.DataFlowSpec{
		Replicas: &r,
		Maintenance: &dataflowv1.MaintenanceSpec{
			StartTime: future,
			Duration:  "1h",
		},
	}
	assert.Equal(t, int32(3), desiredProcessorReplicasAt(window, time.Now()))
}
