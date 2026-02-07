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
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// ValidateCreate implements admission.CustomValidator.
func (r *DataFlow) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	df, ok := obj.(*DataFlow)
	if !ok {
		return nil, nil
	}
	errs := ValidateDataFlowSpec(&df.Spec)
	if len(errs) == 0 {
		return nil, nil
	}
	return nil, errs.ToAggregate()
}

// ValidateUpdate implements admission.CustomValidator.
func (r *DataFlow) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	df, ok := newObj.(*DataFlow)
	if !ok {
		return nil, nil
	}
	errs := ValidateDataFlowSpec(&df.Spec)
	if len(errs) == 0 {
		return nil, nil
	}
	return nil, errs.ToAggregate()
}

// ValidateDelete implements admission.CustomValidator.
func (r *DataFlow) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return nil, nil
}
