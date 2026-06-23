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
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSyncMaintenanceStatus_Suspended(t *testing.T) {
	df := &dataflowv1.DataFlow{
		Spec: dataflowv1.DataFlowSpec{
			Maintenance: &dataflowv1.MaintenanceSpec{Suspended: boolPtrReplicas(true)},
		},
	}
	paused, err := syncMaintenanceStatus(df, time.Now())
	require.NoError(t, err)
	assert.True(t, paused)
	require.NotNil(t, df.Status.MaintenanceStatus)
	assert.True(t, df.Status.MaintenanceStatus.Suspended)
}

func TestSyncMaintenanceStatus_ScheduledWindow(t *testing.T) {
	now := time.Date(2024, 6, 2, 2, 15, 0, 0, time.UTC)
	df := &dataflowv1.DataFlow{
		Spec: dataflowv1.DataFlowSpec{
			Maintenance: &dataflowv1.MaintenanceSpec{
				StartTime: "2024-06-01T02:00:00Z",
				Duration:  "30m",
				Repeat:    dataflowv1.MaintenanceRepeatDaily,
			},
		},
	}
	paused, err := syncMaintenanceStatus(df, now)
	require.NoError(t, err)
	assert.True(t, paused)
	require.NotNil(t, df.Status.MaintenanceStatus)
	assert.True(t, df.Status.MaintenanceStatus.InMaintenance)
	assert.NotNil(t, df.Status.MaintenanceStatus.LastMaintenanceTime)
}

func TestProcessorPausedMessage(t *testing.T) {
	df := &dataflowv1.DataFlow{
		Status: dataflowv1.DataFlowStatus{
			MaintenanceStatus: &dataflowv1.MaintenanceStatus{Suspended: true},
		},
	}
	assert.Equal(t, "Processor suspended manually", processorPausedMessage(df))

	df.Status.MaintenanceStatus = &dataflowv1.MaintenanceStatus{InMaintenance: true}
	assert.Equal(t, "Processor paused for scheduled maintenance window", processorPausedMessage(df))
}

func TestSyncMaintenanceStatus_NilMaintenance(t *testing.T) {
	df := &dataflowv1.DataFlow{}
	paused, err := syncMaintenanceStatus(df, metav1.Now().Time)
	require.NoError(t, err)
	assert.False(t, paused)
}
