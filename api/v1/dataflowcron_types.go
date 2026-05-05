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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type DataFlowCronConcurrencyPolicy string

const (
	DataFlowCronConcurrencyAllow   DataFlowCronConcurrencyPolicy = "Allow"
	DataFlowCronConcurrencyForbid  DataFlowCronConcurrencyPolicy = "Forbid"
	DataFlowCronConcurrencyReplace DataFlowCronConcurrencyPolicy = "Replace"
)

type DataFlowCronSpec struct {
	DataFlowSpec `json:",inline"`

	// Schedule defines when to start a new run in standard cron format.
	Schedule string `json:"schedule"`

	// +optional
	ConcurrencyPolicy DataFlowCronConcurrencyPolicy `json:"concurrencyPolicy,omitempty"`
	// +optional
	SuccessfulJobsHistoryLimit *int32 `json:"successfulJobsHistoryLimit,omitempty"`
	// +optional
	FailedJobsHistoryLimit *int32 `json:"failedJobsHistoryLimit,omitempty"`
	// +optional
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty"`
	// +optional
	Suspend *bool `json:"suspend,omitempty"`
	// +optional
	Triggers []DataFlowCronTrigger `json:"triggers,omitempty"`
}

type DataFlowCronTrigger struct {
	Name string `json:"name,omitempty"`
	// Container image used to execute the trigger step.
	Image string `json:"image"`
	// +optional
	Command []string `json:"command,omitempty"`
	// +optional
	Args []string `json:"args,omitempty"`
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`
}

type DataFlowCronStatus struct {
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`
	// +optional
	CurrentRunID string `json:"currentRunID,omitempty"`
	// +optional
	CurrentTriggerIndex *int32 `json:"currentTriggerIndex,omitempty"`
	// +optional
	ActiveJobName string `json:"activeJobName,omitempty"`
	// +optional
	LastSuccessfulTime *metav1.Time `json:"lastSuccessfulTime,omitempty"`
	// +optional
	LastFailedTime *metav1.Time `json:"lastFailedTime,omitempty"`
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

type DataFlowCron struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DataFlowCronSpec   `json:"spec,omitempty"`
	Status DataFlowCronStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

type DataFlowCronList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DataFlowCron `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DataFlowCron{}, &DataFlowCronList{})
}
