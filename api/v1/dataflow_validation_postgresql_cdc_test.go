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

func TestValidatePostgreSQLCDCSource_valid(t *testing.T) {
	t.Parallel()
	spec := &PostgreSQLCDCSourceSpec{
		ConnectionString: "postgres://user:pass@localhost:5432/db",
		SlotName:         "dataflow_slot",
		PublicationName:  "dataflow_pub",
		Tables:           []string{"public.orders", "customers"},
		SnapshotMode:     "initial",
	}
	path := field.NewPath("config")
	if errs := validatePostgreSQLCDCSource(spec, path); len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
}

func TestValidatePostgreSQLCDCSource_missingRequired(t *testing.T) {
	t.Parallel()
	spec := &PostgreSQLCDCSourceSpec{}
	path := field.NewPath("config")
	errs := validatePostgreSQLCDCSource(spec, path)
	if len(errs) < 4 {
		t.Fatalf("expected multiple required field errors, got %v", errs)
	}
}

func TestValidatePostgreSQLCDCSource_invalidTable(t *testing.T) {
	t.Parallel()
	spec := &PostgreSQLCDCSourceSpec{
		ConnectionString: "postgres://localhost/db",
		SlotName:         "slot",
		PublicationName:  "pub",
		Tables:           []string{"bad-table-name"},
	}
	path := field.NewPath("config")
	errs := validatePostgreSQLCDCSource(spec, path)
	if len(errs) == 0 {
		t.Fatal("expected invalid table error")
	}
}
