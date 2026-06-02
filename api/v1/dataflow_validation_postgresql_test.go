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

	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidatePostgreSQLSource_orderByColumn(t *testing.T) {
	path := field.NewPath("spec").Child("source").Child("config")

	t.Run("valid identifier", func(t *testing.T) {
		spec := &PostgreSQLSourceSpec{
			ConnectionString: "postgres://x",
			Table:            "t",
			OrderByColumn:    "price_id",
		}
		if errs := validatePostgreSQLSource(spec, path); len(errs) != 0 {
			t.Fatalf("expected no errors, got %v", errs)
		}
	})

	t.Run("invalid identifier", func(t *testing.T) {
		spec := &PostgreSQLSourceSpec{
			ConnectionString: "postgres://x",
			Table:            "t",
			OrderByColumn:    "price-id",
		}
		errs := validatePostgreSQLSource(spec, path)
		if len(errs) == 0 {
			t.Fatal("expected validation error for invalid orderByColumn")
		}
	})
}
