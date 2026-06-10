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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildTrinoMergeQuery(t *testing.T) {
	columns := []TableColumnInfo{
		{Name: "id", Type: "bigint"},
		{Name: "name", Type: "varchar"},
		{Name: "amount", Type: "double"},
	}
	valueRows := []string{"(1, 'foo', 10.5)", "(2, 'bar', 20.0)"}

	query, err := buildTrinoMergeQuery("iceberg", "analytics", "prices", "id", columns, valueRows)
	require.NoError(t, err)
	assert.Contains(t, query, `MERGE INTO iceberg.analytics.prices AS target`)
	assert.Contains(t, query, `USING (VALUES (1, 'foo', 10.5), (2, 'bar', 20.0)) AS source("id", "name", "amount")`)
	assert.Contains(t, query, `ON target."id" = source."id"`)
	assert.Contains(t, query, `WHEN MATCHED THEN UPDATE SET "name" = source."name", "amount" = source."amount"`)
	assert.Contains(t, query, `WHEN NOT MATCHED THEN INSERT ("id", "name", "amount") VALUES (source."id", source."name", source."amount")`)
}

func TestBuildTrinoMergeQuery_requiresConflictKey(t *testing.T) {
	_, err := buildTrinoMergeQuery("iceberg", "s", "t", "", []TableColumnInfo{{Name: "id", Type: "bigint"}}, []string{"(1)"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "conflictKey")
}

func TestIsTrinoIcebergCatalog(t *testing.T) {
	assert.True(t, isTrinoIcebergCatalog("nessie_iceberg"))
	assert.True(t, isTrinoIcebergCatalog("ICEBERG_PROD"))
	assert.False(t, isTrinoIcebergCatalog("hive"))
}
