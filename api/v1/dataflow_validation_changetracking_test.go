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

func TestValidateChangeTrackingColumn_sources(t *testing.T) {
	path := field.NewPath("spec").Child("source").Child("config")

	t.Run("postgresql valid", func(t *testing.T) {
		spec := &PostgreSQLSourceSpec{
			ConnectionString:     "postgres://x",
			Table:                "t",
			ChangeTrackingColumn: "updated_at",
		}
		if errs := validatePostgreSQLSource(spec, path); len(errs) != 0 {
			t.Fatalf("expected no errors, got %v", errs)
		}
	})

	t.Run("trino invalid", func(t *testing.T) {
		spec := &TrinoSourceSpec{
			ServerURL:            "http://trino:8080",
			Catalog:              "c",
			Schema:               "s",
			Table:                "t",
			ChangeTrackingColumn: "bad-col",
		}
		errs := validateTrinoSource(spec, path)
		if len(errs) == 0 {
			t.Fatal("expected validation error")
		}
	})

	t.Run("clickhouse valid", func(t *testing.T) {
		spec := &ClickHouseSourceSpec{
			ConnectionString:     "clickhouse://x",
			Table:                "t",
			ChangeTrackingColumn: "created_at",
		}
		if errs := validateClickHouseSource(spec, path); len(errs) != 0 {
			t.Fatalf("expected no errors, got %v", errs)
		}
	})
}
