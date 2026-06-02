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
	"time"

	"github.com/dataflow-operator/dataflow/internal/checkpoint"
	"github.com/stretchr/testify/assert"
)

func TestBuildIncrementalSelect_postgres(t *testing.T) {
	d := postgresDialect{}
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		state   checkpoint.Composite
		want    []string
		notWant []string
	}{
		{
			name:    "first read",
			want:    []string{"COALESCE(updated_at, created_at)", "ORDER BY", `"id"`},
			notWant: []string{"WHERE"},
		},
		{
			name:  "composite",
			state: checkpoint.Composite{ChangeTime: &ts, OrderByValue: int64(5042)},
			want:  []string{`WHERE (COALESCE(updated_at, created_at), "id") > ('2024-01-15T10:00:00`, ", 5042)"},
		},
		{
			name:  "time only legacy",
			state: checkpoint.Composite{ChangeTime: &ts},
			want:  []string{"WHERE COALESCE(updated_at, created_at) > '2024-01-15T10:00:00"},
		},
		{
			name:  "order only legacy",
			state: checkpoint.Composite{OrderByValue: int64(100)},
			want:  []string{`WHERE "id" > 100`, `ORDER BY "id"`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildIncrementalSelect(IncrementalSelectInput{
				FromExpr:           `"public"."events"`,
				ChangeTrackingExpr: "COALESCE(updated_at, created_at)",
				OrderByColumn:      "id",
				State:              tt.state,
				Dialect:            d,
			})
			for _, s := range tt.want {
				assert.Contains(t, got, s)
			}
			for _, s := range tt.notWant {
				assert.NotContains(t, got, s)
			}
		})
	}
}

func TestBuildIncrementalSelect_trino(t *testing.T) {
	d := trinoDialect{}
	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	got := BuildIncrementalSelect(IncrementalSelectInput{
		FromExpr:           "hive.default.events",
		ChangeTrackingExpr: "updated_at",
		OrderByColumn:      "id",
		State:              checkpoint.Composite{ChangeTime: &ts, OrderByValue: int64(42)},
		Dialect:            d,
	})
	assert.Contains(t, got, "WHERE (updated_at, id) > (TIMESTAMP '2024-06-01 12:00:00', 42)")
	assert.Contains(t, got, "ORDER BY updated_at, id")
}

func TestBuildIncrementalSelect_clickhouse(t *testing.T) {
	d := clickHouseDialect{}
	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	got := BuildIncrementalSelect(IncrementalSelectInput{
		FromExpr:           "events",
		ChangeTrackingExpr: "created_at",
		OrderByColumn:      "id",
		State:              checkpoint.Composite{ChangeTime: &ts, OrderByValue: int64(100)},
		Dialect:            d,
	})
	assert.Contains(t, got, "WHERE (created_at, id) > ('2024-06-01 12:00:00', 100)")
	assert.Contains(t, got, "ORDER BY created_at, id")
}

func TestCompositeCheckpointHolder_advance(t *testing.T) {
	h := &CompositeCheckpointHolder{}
	t1 := time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)

	h.Advance(checkpoint.Composite{ChangeTime: &t1, OrderByValue: int64(1)}, true)
	snap := h.Snapshot()
	requireTime := snap.ChangeTime.Equal(t1)
	assert.True(t, requireTime)

	h.Advance(checkpoint.Composite{ChangeTime: &t2, OrderByValue: int64(2)}, true)
	snap = h.Snapshot()
	assert.True(t, snap.ChangeTime.Equal(t2))
	assert.Equal(t, int64(2), snap.OrderByValue)

	h.Advance(checkpoint.Composite{ChangeTime: &t1, OrderByValue: int64(1)}, true)
	snap = h.Snapshot()
	assert.True(t, snap.ChangeTime.Equal(t2))
}

func TestCompositeCheckpointHolder_orderOnlyLegacy(t *testing.T) {
	h := &CompositeCheckpointHolder{}
	h.ApplyInitial([]byte(`{"lastReadOrderByValue":50}`))
	got := BuildIncrementalSelect(IncrementalSelectInput{
		FromExpr:           "t",
		ChangeTrackingExpr: "created_at",
		OrderByColumn:      "id",
		State:              h.Snapshot(),
		Dialect:            clickHouseDialect{},
	})
	assert.Contains(t, got, "WHERE id > 50")
}
