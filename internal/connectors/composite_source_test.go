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
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/dataflow-operator/dataflow/internal/checkpoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	assert.Contains(t, got, "WHERE (created_at, `id`) > ('2024-06-01 12:00:00', 100)")
	assert.Contains(t, got, "ORDER BY created_at, `id`")
}

func TestBuildIncrementalSelect_clickhouse_reservedIdentifier(t *testing.T) {
	d := clickHouseDialect{}
	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	got := BuildIncrementalSelect(IncrementalSelectInput{
		FromExpr:           "events",
		ChangeTrackingExpr: "`order`",
		OrderByColumn:      "id",
		State:              checkpoint.Composite{ChangeTime: &ts, OrderByValue: int64(1)},
		Dialect:            d,
	})
	assert.Contains(t, got, "WHERE (`order`, `id`) >")
}

func TestIncrementalQueryConfig_ResolveReadQuery(t *testing.T) {
	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	state := checkpoint.Composite{ChangeTime: &ts, OrderByValue: int64(10)}

	t.Run("postgres table", func(t *testing.T) {
		cfg := IncrementalQueryConfig{
			DefaultChangeColumn: "updated_at",
			CoalesceUpdatedAt:   true,
			LegacyWrap:          LegacyQueryAsIs,
			Dialect:             postgresDialect{},
			FromTableExpr:       `"public"."events"`,
			State:               state,
		}
		got := cfg.ResolveReadQuery()
		assert.Contains(t, got, `"public"."events"`)
		assert.Contains(t, got, "WHERE (COALESCE(updated_at, created_at)")
	})

	t.Run("postgres legacy query as-is", func(t *testing.T) {
		cfg := IncrementalQueryConfig{
			UserQuery:           "SELECT 1",
			DefaultChangeColumn: "updated_at",
			LegacyWrap:          LegacyQueryAsIs,
			Dialect:             postgresDialect{},
		}
		assert.Equal(t, "SELECT 1", cfg.ResolveReadQuery())
	})

	t.Run("trino incremental query", func(t *testing.T) {
		cfg := IncrementalQueryConfig{
			UserQuery:            "SELECT * FROM t",
			ExplicitChangeColumn: "updated_at",
			DefaultChangeColumn:  "updated_at",
			LegacyWrap:           LegacyQueryOrderByOnly,
			Dialect:              trinoDialect{},
			State:                state,
		}
		got := cfg.ResolveReadQuery()
		assert.Contains(t, got, "FROM (SELECT * FROM t) AS __dataflow_src")
		assert.Contains(t, got, "WHERE (updated_at, id) >")
	})

	t.Run("trino legacy order only", func(t *testing.T) {
		cfg := IncrementalQueryConfig{
			UserQuery:           "SELECT * FROM prices",
			DefaultChangeColumn: "updated_at",
			LegacyWrap:          LegacyQueryOrderByOnly,
			Dialect:             trinoDialect{},
			OrderByColumn:       "price_id",
		}
		got := cfg.ResolveReadQuery()
		assert.Contains(t, got, "__dataflow_src")
		assert.Contains(t, got, "ORDER BY price_id")
		assert.NotContains(t, got, "WHERE")
	})

	t.Run("clickhouse incremental query", func(t *testing.T) {
		cfg := IncrementalQueryConfig{
			UserQuery:            "SELECT * FROM events",
			ExplicitChangeColumn: "created_at",
			DefaultChangeColumn:  "created_at",
			LegacyWrap:           LegacyQueryChangeAndOrderBy,
			Dialect:              clickHouseDialect{},
			State:                state,
		}
		got := cfg.ResolveReadQuery()
		assert.Contains(t, got, "FROM (SELECT * FROM events) AS __dataflow_src")
		assert.Contains(t, got, "WHERE (`created_at`, `id`) >")
	})
}

func TestAppendSQLLimit(t *testing.T) {
	assert.Equal(t, "SELECT 1", AppendSQLLimit("SELECT 1", 0))
	assert.Equal(t, "SELECT 1 LIMIT 100", AppendSQLLimit("SELECT 1", 100))
}

func TestRunIncrementalBatchPoll_postgresUserQueryAdvancesCursor(t *testing.T) {
	userQuery := `SELECT price_id, update_date
FROM price.price
WHERE price_status = 'EXPORTED'`
	cfg := IncrementalQueryConfig{
		UserQuery:            userQuery,
		ExplicitChangeColumn: "update_date",
		DefaultChangeColumn:  "updated_at",
		OrderByColumn:        "price_id",
		CoalesceUpdatedAt:    true,
		LegacyWrap:           LegacyQueryAsIs,
		Dialect:              postgresDialect{},
	}
	ctx := context.Background()
	var queries []string
	t1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)

	stats, err := RunIncrementalBatchPoll(ctx, cfg, 2, func(_ context.Context, query string, info BatchPollInfo) (int, checkpoint.Composite, error) {
		queries = append(queries, query)
		switch len(queries) {
		case 1:
			assert.Equal(t, 1, info.BatchNumber)
			assert.Contains(t, query, "LIMIT 2")
			assert.Contains(t, query, "price_status = 'EXPORTED'")
			assert.NotContains(t, query, `WHERE ("update_date", "price_id") >`)
			return 2, checkpoint.Composite{ChangeTime: &t1, OrderByValue: int64(2)}, nil
		case 2:
			assert.Equal(t, 2, info.BatchNumber)
			assert.Equal(t, 2, info.TotalRows)
			assert.Contains(t, query, `WHERE ("update_date", "price_id") > ('2024-01-01T00:00:00`, ", 2)")
			assert.Contains(t, query, "LIMIT 2")
			return 2, checkpoint.Composite{ChangeTime: &t1, OrderByValue: int64(4)}, nil
		case 3:
			assert.Equal(t, 3, info.BatchNumber)
			assert.Equal(t, 4, info.TotalRows)
			assert.Contains(t, query, ", 4)")
			return 1, checkpoint.Composite{ChangeTime: &t2, OrderByValue: int64(5)}, nil
		default:
			return 0, checkpoint.Composite{}, nil
		}
	})
	require.NoError(t, err)
	assert.Equal(t, 5, stats.TotalRows)
	assert.Equal(t, 3, stats.Batches)
	require.Len(t, queries, 3)
}

func TestRunIncrementalBatchPoll(t *testing.T) {
	cfg := IncrementalQueryConfig{
		FromTableExpr:       "t",
		DefaultChangeColumn: "created_at",
		OrderByColumn:       "id",
		Dialect:             clickHouseDialect{},
	}
	ctx := context.Background()
	var queries []string
	t1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	stats, err := RunIncrementalBatchPoll(ctx, cfg, 2, func(_ context.Context, query string, info BatchPollInfo) (int, checkpoint.Composite, error) {
		queries = append(queries, query)
		switch len(queries) {
		case 1:
			assert.Equal(t, 1, info.BatchNumber)
			assert.Equal(t, 0, info.TotalRows)
			return 2, checkpoint.Composite{ChangeTime: &t1, OrderByValue: int64(2)}, nil
		case 2:
			assert.Equal(t, 2, info.BatchNumber)
			assert.Equal(t, 2, info.TotalRows)
			return 1, checkpoint.Composite{ChangeTime: &t1, OrderByValue: int64(3)}, nil
		default:
			return 0, checkpoint.Composite{}, nil
		}
	})
	require.NoError(t, err)
	require.Len(t, queries, 2)
	assert.Equal(t, 3, stats.TotalRows)
	assert.Equal(t, 2, stats.Batches)
	assert.Contains(t, queries[0], "LIMIT 2")
	assert.Contains(t, queries[1], "LIMIT 2")
	assert.Contains(t, queries[1], ", 2)")

	queries = nil
	stats, err = RunIncrementalBatchPoll(ctx, cfg, 0, func(_ context.Context, query string, _ BatchPollInfo) (int, checkpoint.Composite, error) {
		queries = append(queries, query)
		return 0, checkpoint.Composite{}, nil
	})
	assert.ErrorIs(t, err, ErrSourceExhausted)
	assert.Equal(t, 0, stats.TotalRows)
	require.Len(t, queries, 1)
	assert.NotContains(t, queries[0], "LIMIT")
}

func TestBatchPollLogFields(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	info := BatchPollInfo{
		BatchNumber: 3,
		TotalRows:   20000,
		Limit:       10000,
		StartState: checkpoint.Composite{
			ChangeTime:   &start,
			OrderByValue: int64(100),
		},
	}
	lastRow := checkpoint.Composite{
		ChangeTime:   &end,
		OrderByValue: int64(5042),
	}
	fields := BatchPollLogFields(info, 10000, 850*time.Millisecond, lastRow)

	fieldMap := make(map[string]interface{})
	for i := 0; i < len(fields); i += 2 {
		fieldMap[fields[i].(string)] = fields[i+1]
	}
	assert.Equal(t, 3, fieldMap["batch"])
	assert.Equal(t, 10000, fieldMap["rows"])
	assert.Equal(t, 30000, fieldMap["rows_in_poll"])
	assert.Equal(t, 10000, fieldMap["read_batch_size"])
	assert.Equal(t, int64(850), fieldMap["duration_ms"])
	assert.Equal(t, true, fieldMap["has_more"])
	assert.Equal(t, "2024-01-01T00:00:00Z", fieldMap["from_change_time"])
	assert.Equal(t, int64(100), fieldMap["from_order_by"])
	assert.Equal(t, "2024-06-01T12:00:00Z", fieldMap["to_change_time"])
	assert.Equal(t, int64(5042), fieldMap["to_order_by"])
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

func TestCompositeCheckpointHolder_AdvanceAndSyncFlush(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "df-cp-test", Namespace: "default"},
		Data:       map[string]string{},
	}
	client := fake.NewSimpleClientset(cm)
	store, err := checkpoint.NewConfigMapStoreWithClient(client, "default", "df-cp-test")
	require.NoError(t, err)
	syncStore := checkpoint.NewSyncStore(store, true, 0)

	h := &CompositeCheckpointHolder{}
	h.InitCompositeCheckpoint(syncStore, "postgresql", nil)

	t1 := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	ack := h.MakeAck(&t1, int64(5042), true)
	ack()
	require.NoError(t, syncStore.FlushAfterBatchAck(ctx))

	loaded, err := syncStore.Load(ctx, "postgresql")
	require.NoError(t, err)
	assert.Contains(t, string(loaded), "2024-01-15T10:00:00Z")
	assert.Contains(t, string(loaded), "5042")
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
	assert.Contains(t, got, "WHERE `id` > 50")
}
