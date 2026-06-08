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

	ctrl "sigs.k8s.io/controller-runtime"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
)

// createOrUpdateProcessorRBAC creates ServiceAccount, Role, and RoleBinding for the processor pod:
// checkpoint ConfigMap access when checkpoint persistence is enabled, and Secret get when Nessie sink uses in-namespace S3 refs.
func (r *DataFlowReconciler) createOrUpdateProcessorRBAC(ctx context.Context, req ctrl.Request, dataflow *dataflowv1.DataFlow, resolvedSpec *dataflowv1.DataFlowSpec) error {
	return createOrUpdateProcessorRBAC(ctx, r.Client, r.Scheme, req.Namespace, dataflow.Name, dataflow, resolvedSpec)
}
