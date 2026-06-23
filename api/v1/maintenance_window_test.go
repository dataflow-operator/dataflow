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

package v1

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func boolPtr(b bool) *bool { return &b }

func TestIsMaintenanceSuspended(t *testing.T) {
	assert.False(t, IsMaintenanceSuspended(nil))
	assert.False(t, IsMaintenanceSuspended(&MaintenanceSpec{}))
	assert.True(t, IsMaintenanceSuspended(&MaintenanceSpec{Suspended: boolPtr(true)}))
}

func TestEvaluateMaintenanceWindow_OneTime(t *testing.T) {
	spec := &MaintenanceSpec{
		StartTime: "2024-06-01T10:00:00Z",
		Duration:  "1h",
	}
	start, err := time.Parse(time.RFC3339, spec.StartTime)
	require.NoError(t, err)

	before, err := EvaluateMaintenanceWindow(spec, start.Add(-time.Minute))
	require.NoError(t, err)
	assert.False(t, before.InWindow)
	assert.Equal(t, start, before.NextWindowStart)

	inside, err := EvaluateMaintenanceWindow(spec, start.Add(30*time.Minute))
	require.NoError(t, err)
	assert.True(t, inside.InWindow)
	assert.Equal(t, start, inside.CurrentWindowStart)

	after, err := EvaluateMaintenanceWindow(spec, start.Add(2*time.Hour))
	require.NoError(t, err)
	assert.False(t, after.InWindow)
	assert.True(t, after.NextWindowStart.IsZero())
}

func TestEvaluateMaintenanceWindow_Daily(t *testing.T) {
	spec := &MaintenanceSpec{
		StartTime: "2024-06-01T02:00:00Z",
		Duration:  "30m",
		Repeat:    MaintenanceRepeatDaily,
		Timezone:  "UTC",
	}
	day2 := time.Date(2024, 6, 2, 2, 15, 0, 0, time.UTC)
	result, err := EvaluateMaintenanceWindow(spec, day2)
	require.NoError(t, err)
	assert.True(t, result.InWindow)
}

func TestIsProcessorPaused_Suspended(t *testing.T) {
	spec := &DataFlowSpec{
		Maintenance: &MaintenanceSpec{Suspended: boolPtr(true)},
	}
	paused, err := IsProcessorPaused(spec, time.Now())
	require.NoError(t, err)
	assert.True(t, paused)
}

func TestIsProcessorPaused_ReplicasWhenRunning(t *testing.T) {
	replicas := int32(3)
	spec := &DataFlowSpec{Replicas: &replicas}
	paused, err := IsProcessorPaused(spec, time.Now())
	require.NoError(t, err)
	assert.False(t, paused)
}
