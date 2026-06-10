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

func TestBuildClickHouseCreateEngineClause_defaultMergeTree(t *testing.T) {
	spec := &v1.ClickHouseSinkSpec{Table: "events"}
	engine, orderBy := buildClickHouseCreateEngineClause(spec, []string{"id"})
	assert.Equal(t, "ENGINE = MergeTree()", engine)
	assert.Equal(t, "ORDER BY `id`", orderBy)
}

func TestBuildClickHouseCreateEngineClause_upsertMode(t *testing.T) {
	upsertMode := true
	conflictKey := "price_id"
	versionCol := "update_date"
	spec := &v1.ClickHouseSinkSpec{
		Table:                  "prices",
		UpsertMode:             &upsertMode,
		ConflictKey:            &conflictKey,
		ReplacingVersionColumn: &versionCol,
	}
	orderByCols := resolveClickHouseOrderByColumns(spec, []string{"amount", "price_id"})
	engine, orderBy := buildClickHouseCreateEngineClause(spec, orderByCols)
	assert.Equal(t, "ENGINE = ReplacingMergeTree(`update_date`)", engine)
	assert.Equal(t, "ORDER BY `price_id`", orderBy)
}

func TestBuildClickHouseCreateEngineClause_explicitReplacingMergeTree(t *testing.T) {
	engineName := "ReplacingMergeTree"
	spec := &v1.ClickHouseSinkSpec{
		Table:       "events",
		TableEngine: &engineName,
	}
	engine, orderBy := buildClickHouseCreateEngineClause(spec, []string{"event_id"})
	assert.Equal(t, "ENGINE = ReplacingMergeTree()", engine)
	assert.Equal(t, "ORDER BY `event_id`", orderBy)
}

func TestResolveClickHouseOrderByColumns(t *testing.T) {
	conflictKey := "sku"
	spec := &v1.ClickHouseSinkSpec{ConflictKey: &conflictKey}
	assert.Equal(t, []string{"sku"}, resolveClickHouseOrderByColumns(spec, []string{"id", "name"}))

	spec2 := &v1.ClickHouseSinkSpec{}
	assert.Equal(t, []string{"id"}, resolveClickHouseOrderByColumns(spec2, []string{"id", "name"}))
	assert.Equal(t, []string{"created_at"}, resolveClickHouseOrderByColumns(spec2, nil))
}
