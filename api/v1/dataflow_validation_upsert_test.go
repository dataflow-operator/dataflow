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

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidatePostgreSQLSink_ifNewerRequiresVersionColumn(t *testing.T) {
	strategy := "ifNewer"
	spec := &PostgreSQLSinkSpec{
		ConnectionString: "postgres://localhost/db",
		Table:            "t",
		UpsertStrategy:   &strategy,
	}
	errs := validatePostgreSQLSink(spec, field.NewPath("sink"))
	assert.NotEmpty(t, errs)
}

func TestValidateTrinoSink_upsertRequiresIcebergCatalog(t *testing.T) {
	upsertMode := true
	conflictKey := "id"
	spec := &TrinoSinkSpec{
		ServerURL:   "http://trino:8080",
		Catalog:     "hive",
		Schema:      "default",
		Table:       "t",
		UpsertMode:  &upsertMode,
		ConflictKey: &conflictKey,
	}
	errs := validateTrinoSink(spec, field.NewPath("sink"))
	assert.NotEmpty(t, errs)
}

func TestValidateTrinoSink_upsertValidIceberg(t *testing.T) {
	upsertMode := true
	conflictKey := "id"
	spec := &TrinoSinkSpec{
		ServerURL:   "http://trino:8080",
		Catalog:     "nessie_iceberg",
		Schema:      "default",
		Table:       "t",
		UpsertMode:  &upsertMode,
		ConflictKey: &conflictKey,
	}
	errs := validateTrinoSink(spec, field.NewPath("sink"))
	assert.Empty(t, errs)
}

func TestValidateClickHouseSink_upsertEngineFields(t *testing.T) {
	badEngine := "SummingMergeTree"
	spec := &ClickHouseSinkSpec{
		ConnectionString: "clickhouse://localhost:9000",
		Table:            "t",
		TableEngine:      &badEngine,
	}
	errs := validateClickHouseSink(spec, field.NewPath("sink"))
	assert.NotEmpty(t, errs)
}
