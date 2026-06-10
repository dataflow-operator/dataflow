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

package connectors

import (
	"testing"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/stretchr/testify/assert"
)

func TestBuildPostgreSQLOnConflictClause_always(t *testing.T) {
	config := &v1.PostgreSQLSinkSpec{Table: "public.prices"}
	clause := buildPostgreSQLOnConflictClause(`"public"."prices"`, "price_id", []string{"price_id", "amount", "update_date"}, config)
	assert.Contains(t, clause, `ON CONFLICT ("price_id") DO UPDATE SET`)
	assert.Contains(t, clause, `"amount" = EXCLUDED."amount"`)
	assert.Contains(t, clause, `"update_date" = EXCLUDED."update_date"`)
	assert.NotContains(t, clause, "WHERE")
}

func TestBuildPostgreSQLOnConflictClause_ifNewer(t *testing.T) {
	strategy := "ifNewer"
	versionCol := "update_date"
	config := &v1.PostgreSQLSinkSpec{
		Table:               "prices",
		UpsertStrategy:      &strategy,
		UpsertVersionColumn: &versionCol,
	}
	clause := buildPostgreSQLOnConflictClause(`"prices"`, "price_id", []string{"price_id", "amount", "update_date"}, config)
	assert.Contains(t, clause, `ON CONFLICT ("price_id") DO UPDATE SET`)
	assert.Contains(t, clause, `WHERE EXCLUDED."update_date" > "prices"."update_date"`)
}

func TestBuildPostgreSQLOnConflictClause_conflictKeyOnly(t *testing.T) {
	config := &v1.PostgreSQLSinkSpec{Table: "t"}
	clause := buildPostgreSQLOnConflictClause(`"t"`, "id", []string{"id"}, config)
	assert.Equal(t, `ON CONFLICT ("id") DO NOTHING`, clause)
}
