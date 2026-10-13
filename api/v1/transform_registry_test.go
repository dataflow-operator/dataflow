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
	"slices"
	"sort"
	"testing"

	"github.com/dataflow-operator/dataflow/pkg/transformtypes"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestTransformValidatorsMatchCatalog(t *testing.T) {
	want := slices.Clone(transformtypes.All())
	sort.Strings(want)
	got := transformtypes.RegisteredTypes()
	if !slices.Equal(want, got) {
		t.Fatalf("validators %v do not match transformtypes.All() %v", got, want)
	}
	for _, typ := range want {
		if transformtypes.Validator(typ) == nil {
			t.Fatalf("validator missing for %s", typ)
		}
	}
}

func TestTransformValidatorsRejectEmptyConfig(t *testing.T) {
	path := field.NewPath("spec", "transformations").Index(0)
	for _, typ := range transformtypes.All() {
		validator := transformtypes.Validator(typ)
		if validator == nil {
			t.Fatalf("validator missing for %s", typ)
		}
		errs := validator(nil, path)
		if len(errs) == 0 {
			t.Errorf("%s: empty config produced no errors", typ)
		}
	}
}
