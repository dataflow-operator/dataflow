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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const defaultCheckpointSaveInterval = 30 * time.Second

// CheckpointPersistenceEnabled reports whether checkpoint persistence is enabled (default true).
func CheckpointPersistenceEnabled(spec *DataFlowSpec) bool {
	return spec == nil || spec.CheckpointPersistence == nil || *spec.CheckpointPersistence
}

// CheckpointSyncOnAckEnabled reports whether checkpoint should flush after each sink batch ack.
func CheckpointSyncOnAckEnabled(spec *DataFlowSpec) bool {
	return spec != nil && spec.CheckpointSyncOnAck != nil && *spec.CheckpointSyncOnAck
}

// CheckpointSaveIntervalDuration returns the debounce/sync coalesce interval (default 30s).
func CheckpointSaveIntervalDuration(spec *DataFlowSpec) time.Duration {
	if spec != nil && spec.CheckpointSaveInterval != nil && spec.CheckpointSaveInterval.Duration > 0 {
		return spec.CheckpointSaveInterval.Duration
	}
	return defaultCheckpointSaveInterval
}

// CheckpointSaveIntervalOrDefault returns the spec interval pointer or a default metav1.Duration.
func CheckpointSaveIntervalOrDefault(spec *DataFlowSpec) metav1.Duration {
	return metav1.Duration{Duration: CheckpointSaveIntervalDuration(spec)}
}
