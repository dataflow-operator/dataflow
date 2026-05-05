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

// Package k8snames defines stable Kubernetes object names derived from DataFlow CR metadata.name.
package k8snames

// ProcessorChildPrefix is used for Deployment, ConfigMaps, and processor RBAC names owned by the operator.
const ProcessorChildPrefix = "df"

// ProcessorDeployment returns the Deployment name for the processor workload.
func ProcessorDeployment(dataflowName string) string {
	return ProcessorChildPrefix + "-" + dataflowName
}

// ProcessorSpecConfigMap returns the ConfigMap name holding resolved spec JSON for the processor.
func ProcessorSpecConfigMap(dataflowName string) string {
	return ProcessorChildPrefix + "-" + dataflowName + "-spec"
}

// ProcessorCheckpointConfigMap returns the checkpoint persistence ConfigMap name.
func ProcessorCheckpointConfigMap(dataflowName string) string {
	return ProcessorChildPrefix + "-" + dataflowName + "-checkpoint"
}

// ProcessorServiceAccount returns the ServiceAccount name used when RBAC is created for the processor.
func ProcessorServiceAccount(dataflowName string) string {
	return ProcessorChildPrefix + "-" + dataflowName + "-processor"
}

// DataFlowCronPrefix is used for CronJob/Job names managed by DataFlowCron controller.
const DataFlowCronPrefix = "dfc"

// CronSpecConfigMap returns the ConfigMap name holding resolved spec JSON for DataFlowCron.
func CronSpecConfigMap(name string) string {
	return DataFlowCronPrefix + "-" + name + "-spec"
}

// CronJobName returns the CronJob name for DataFlowCron.
func CronJobName(name string) string {
	return DataFlowCronPrefix + "-" + name
}

// CronRunJobName returns deterministic Job name for a run and step.
func CronRunJobName(name, runID, step string) string {
	return DataFlowCronPrefix + "-" + name + "-" + runID + "-" + step
}
