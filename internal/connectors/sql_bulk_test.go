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

func TestBuildPostgreSQLMultiValuesInsert(t *testing.T) {
	t.Parallel()

	t.Run("single row passthrough", func(t *testing.T) {
		t.Parallel()
		q, args, ok := buildPostgreSQLMultiValuesInsert([]queuedSQL{{
			query:  `INSERT INTO "t" (data) VALUES ($1::jsonb)`,
			values: []interface{}{`{"a":1}`},
		}})
		require.True(t, ok)
		assert.Equal(t, `INSERT INTO "t" (data) VALUES ($1::jsonb)`, q)
		assert.Equal(t, []interface{}{`{"a":1}`}, args)
	})

	t.Run("multi row with casts and on conflict", func(t *testing.T) {
		t.Parallel()
		base := `INSERT INTO "t" (data, _metadata) VALUES ($1::jsonb, $2::jsonb) ON CONFLICT DO NOTHING`
		q, args, ok := buildPostgreSQLMultiValuesInsert([]queuedSQL{
			{query: base, values: []interface{}{`{"a":1}`, `{}`}},
			{query: base, values: []interface{}{`{"a":2}`, `{}`}},
			{query: base, values: []interface{}{`{"a":3}`, `{}`}},
		})
		require.True(t, ok)
		assert.Equal(t,
			`INSERT INTO "t" (data, _metadata) VALUES ($1::jsonb,$2::jsonb),($3::jsonb,$4::jsonb),($5::jsonb,$6::jsonb) ON CONFLICT DO NOTHING`,
			q,
		)
		assert.Len(t, args, 6)
		assert.Equal(t, `{"a":2}`, args[2])
	})

	t.Run("mixed queries not ok", func(t *testing.T) {
		t.Parallel()
		_, _, ok := buildPostgreSQLMultiValuesInsert([]queuedSQL{
			{query: `INSERT INTO "t" (a) VALUES ($1)`, values: []interface{}{1}},
			{query: `UPDATE "t" SET a=$1`, values: []interface{}{2}},
		})
		assert.False(t, ok)
	})
}

func TestBuildClickHouseMultiValuesInsert(t *testing.T) {
	t.Parallel()

	q, args, err := buildClickHouseMultiValuesInsert("events", "data", [][]interface{}{
		{"{\"x\":1}"},
		{"{\"x\":2}"},
	})
	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO events (data) VALUES (?),(?)", q)
	assert.Equal(t, []interface{}{"{\"x\":1}", "{\"x\":2}"}, args)

	q, args, err = buildClickHouseMultiValuesInsert("t", "`a`, `b`", [][]interface{}{
		{1, "x"},
		{2, "y"},
	})
	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO t (`a`, `b`) VALUES (?,?),(?,?)", q)
	assert.Equal(t, []interface{}{1, "x", 2, "y"}, args)
}

func TestCanPostgreSQLCopy(t *testing.T) {
	t.Parallel()

	t.Run("plain insert", func(t *testing.T) {
		t.Parallel()
		base := `INSERT INTO "public"."t" (data, _metadata) VALUES ($1::jsonb, $2::jsonb)`
		cols, rows, ok := canPostgreSQLCopy([]queuedSQL{
			{query: base, values: []interface{}{`{"a":1}`, `{}`}},
			{query: base, values: []interface{}{`{"a":2}`, `{}`}},
		})
		require.True(t, ok)
		assert.Equal(t, []string{"data", "_metadata"}, cols)
		assert.Len(t, rows, 2)
	})

	t.Run("on conflict not copyable", func(t *testing.T) {
		t.Parallel()
		base := `INSERT INTO "t" (data) VALUES ($1::jsonb) ON CONFLICT DO NOTHING`
		_, _, ok := canPostgreSQLCopy([]queuedSQL{{query: base, values: []interface{}{`{}`}}})
		assert.False(t, ok)
	})
}

func TestAddedFilesReadableDirectly(t *testing.T) {
	t.Parallel()
	assert.True(t, addedFilesReadableDirectly(nil))
}
