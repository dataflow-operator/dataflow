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

package validation

import (
	"k8s.io/apimachinery/pkg/util/validation/field"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
)

// ValidateDataFlowSpec validates DataFlow spec and returns a list of field errors.
// It delegates to api/v1.ValidateDataFlowSpec for use from packages that cannot import api/v1 (e.g. to avoid cycles).
func ValidateDataFlowSpec(spec *dataflowv1.DataFlowSpec) field.ErrorList {
	return dataflowv1.ValidateDataFlowSpec(spec)
}
