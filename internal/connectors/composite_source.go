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
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/dataflow-operator/dataflow/internal/checkpoint"
	"github.com/dataflow-operator/dataflow/internal/types"
)

// SQLDialect formats SQL literals and identifiers for a database engine.
type SQLDialect interface {
	FormatLiteral(v interface{}) string
	QuoteIdentifier(name string) string
}

// LegacyQueryWrap controls stable ORDER BY when UserQuery is set without ExplicitChangeColumn.
type LegacyQueryWrap int

const (
	// LegacyQueryAsIs runs the user query unchanged (PostgreSQL legacy).
	LegacyQueryAsIs LegacyQueryWrap = iota
	// LegacyQueryOrderByOnly wraps the query with ORDER BY orderByColumn only (Trino legacy).
	LegacyQueryOrderByOnly
	// LegacyQueryChangeAndOrderBy wraps with ORDER BY changeColumn, orderByColumn (ClickHouse legacy).
	LegacyQueryChangeAndOrderBy
)

// IncrementalQueryConfig builds read SQL for polling sources (table, incremental query, legacy query).
type IncrementalQueryConfig struct {
	UserQuery            string
	ExplicitChangeColumn string // from spec; empty enables legacy query mode
	DefaultChangeColumn  string // updated_at or created_at
	OrderByColumn        string
	CoalesceUpdatedAt    bool // PostgreSQL only
	LegacyWrap           LegacyQueryWrap
	Dialect              SQLDialect
	FromTableExpr        string // formatted table reference for table mode
	State                checkpoint.Composite
}

// ChangeColumn returns the effective change-tracking column name.
func (c IncrementalQueryConfig) ChangeColumn() string {
	if c.ExplicitChangeColumn != "" {
		return c.ExplicitChangeColumn
	}
	return c.DefaultChangeColumn
}

func (c IncrementalQueryConfig) changeTrackingOrderExpr() string {
	return ResolveChangeTrackingExpr(c.Dialect, c.ChangeColumn(), c.CoalesceUpdatedAt)
}

func (c IncrementalQueryConfig) incrementalSelectInput(fromExpr string) IncrementalSelectInput {
	return IncrementalSelectInput{
		FromExpr:           fromExpr,
		ChangeTrackingExpr: c.changeTrackingOrderExpr(),
		OrderByColumn:      c.OrderByColumn,
		State:              c.State,
		Dialect:            c.Dialect,
	}
}

// ResolveReadQuery returns the SQL for the current poll (table, incremental subquery, or legacy query).
func (c IncrementalQueryConfig) ResolveReadQuery() string {
	if c.UserQuery != "" {
		if c.ExplicitChangeColumn != "" {
			from := "(" + strings.TrimSpace(c.UserQuery) + ") AS " + stableOrderSubqueryAlias
			return BuildIncrementalSelect(c.incrementalSelectInput(from))
		}
		switch c.LegacyWrap {
		case LegacyQueryAsIs:
			return strings.TrimSpace(c.UserQuery)
		case LegacyQueryOrderByOnly:
			return WrapQueryStableOrder(c.UserQuery, ResolveOrderByColumn(c.OrderByColumn))
		case LegacyQueryChangeAndOrderBy:
			return WrapQueryStableOrder(c.UserQuery, c.ChangeColumn(), ResolveOrderByColumn(c.OrderByColumn))
		default:
			return strings.TrimSpace(c.UserQuery)
		}
	}
	return BuildIncrementalSelect(c.incrementalSelectInput(c.FromTableExpr))
}

// AppendSQLLimit appends a LIMIT clause when limit > 0.
func AppendSQLLimit(query string, limit int) string {
	if limit <= 0 {
		return query
	}
	return fmt.Sprintf("%s LIMIT %d", query, limit)
}

// BatchPollInfo describes one batch within an incremental poll cycle.
type BatchPollInfo struct {
	BatchNumber int
	TotalRows   int // rows read in earlier batches of this poll cycle
	Limit       int
	StartState  checkpoint.Composite
}

// PollCycleStats summarizes a completed incremental poll cycle.
type PollCycleStats struct {
	TotalRows int
	Batches   int
}

// RowCheckpoint builds a composite checkpoint from scanned row values.
func RowCheckpoint(changeTime *time.Time, orderBy interface{}) checkpoint.Composite {
	var c checkpoint.Composite
	if orderBy != nil {
		c.OrderByValue = orderBy
	}
	if changeTime != nil {
		t := *changeTime
		c.ChangeTime = &t
	}
	return c
}

func cloneComposite(c checkpoint.Composite) checkpoint.Composite {
	out := checkpoint.Composite{OrderByValue: c.OrderByValue}
	if c.ChangeTime != nil {
		t := *c.ChangeTime
		out.ChangeTime = &t
	}
	return out
}

// BatchPollLogFields returns structured log fields common to incremental source batch logs.
func BatchPollLogFields(info BatchPollInfo, rowCount int, duration time.Duration, lastRow checkpoint.Composite) []interface{} {
	fields := []interface{}{
		"batch", info.BatchNumber,
		"rows", rowCount,
		"rows_in_poll", info.TotalRows + rowCount,
		"read_batch_size", info.Limit,
		"duration_ms", duration.Milliseconds(),
		"has_more", info.Limit > 0 && rowCount >= info.Limit,
	}
	if info.StartState.ChangeTime != nil {
		fields = append(fields, "from_change_time", info.StartState.ChangeTime.UTC().Format(time.RFC3339Nano))
	}
	if info.StartState.OrderByValue != nil {
		fields = append(fields, "from_order_by", info.StartState.OrderByValue)
	}
	if lastRow.ChangeTime != nil {
		fields = append(fields, "to_change_time", lastRow.ChangeTime.UTC().Format(time.RFC3339Nano))
	}
	if lastRow.OrderByValue != nil {
		fields = append(fields, "to_order_by", lastRow.OrderByValue)
	}
	return fields
}

// RunIncrementalBatchPoll executes read queries in batches until fewer than limit rows are returned.
// The in-poll cursor advances from the last row of each batch so subsequent batches do not re-read rows.
// Returns ErrSourceExhausted when no rows were read in the poll cycle.
func RunIncrementalBatchPoll(
	ctx context.Context,
	baseCfg IncrementalQueryConfig,
	limit int,
	execute func(ctx context.Context, query string, info BatchPollInfo) (rowCount int, lastRow checkpoint.Composite, err error),
) (PollCycleStats, error) {
	cursor := cloneComposite(baseCfg.State)
	totalRows := 0
	batches := 0
	for {
		if err := ctx.Err(); err != nil {
			return PollCycleStats{TotalRows: totalRows, Batches: batches}, err
		}
		batches++
		cfg := baseCfg
		cfg.State = cursor
		startState := cloneComposite(cursor)
		query := AppendSQLLimit(cfg.ResolveReadQuery(), limit)
		rowCount, lastRow, err := execute(ctx, query, BatchPollInfo{
			BatchNumber: batches,
			TotalRows:   totalRows,
			Limit:       limit,
			StartState:  startState,
		})
		if err != nil {
			return PollCycleStats{TotalRows: totalRows, Batches: batches}, err
		}
		totalRows += rowCount
		if rowCount > 0 && checkpoint.ShouldAdvance(cursor, lastRow) {
			cursor = cloneComposite(lastRow)
		}
		if limit == 0 || rowCount < limit {
			break
		}
	}
	if totalRows == 0 {
		return PollCycleStats{}, ErrSourceExhausted
	}
	return PollCycleStats{TotalRows: totalRows, Batches: batches}, nil
}

// IncrementalSelectInput builds an incremental SELECT with composite tuple WHERE.
type IncrementalSelectInput struct {
	FromExpr           string
	ChangeTrackingExpr string // empty disables change-tracking in ORDER BY/WHERE
	OrderByColumn      string // logical column name
	State              checkpoint.Composite
	Dialect            SQLDialect
}

// CompositeCheckpointHolder holds composite checkpoint state with optional persistence.
type CompositeCheckpointHolder struct {
	mu         sync.Mutex
	state      checkpoint.Composite
	store      checkpoint.Store
	sourceType string
}

// InitCompositeCheckpoint configures the holder for a source connector.
func (h *CompositeCheckpointHolder) InitCompositeCheckpoint(store checkpoint.Store, sourceType string, initial []byte) {
	h.store = store
	h.sourceType = sourceType
	if len(initial) > 0 {
		h.ApplyInitial(initial)
	}
}

// ApplyInitial restores checkpoint from persisted (normalized) JSON.
func (h *CompositeCheckpointHolder) ApplyInitial(data []byte) {
	c, err := checkpoint.ParseComposite(data)
	if err != nil {
		return
	}
	h.mu.Lock()
	h.state = c
	h.mu.Unlock()
}

// Snapshot returns a copy of the current checkpoint for query building.
func (h *CompositeCheckpointHolder) Snapshot() checkpoint.Composite {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := checkpoint.Composite{OrderByValue: h.state.OrderByValue}
	if h.state.ChangeTime != nil {
		t := *h.state.ChangeTime
		out.ChangeTime = &t
	}
	return out
}

// Advance updates checkpoint after successful sink write.
// When requireTime is true, rows without changeTime are ignored.
func (h *CompositeCheckpointHolder) Advance(next checkpoint.Composite, requireTime bool) {
	if requireTime && next.ChangeTime == nil {
		return
	}
	if next.ChangeTime == nil && next.OrderByValue == nil {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if !checkpoint.ShouldAdvance(h.state, next) {
		return
	}
	if next.ChangeTime != nil {
		t := *next.ChangeTime
		h.state.ChangeTime = &t
	}
	if next.OrderByValue != nil {
		h.state.OrderByValue = next.OrderByValue
	}

	if h.store != nil && (h.state.ChangeTime != nil || h.state.OrderByValue != nil) {
		_ = h.store.Save(context.Background(), h.sourceType, h.state.Marshal())
	}
}

// AssignCompositeSourceAck sets msg.Ack so the composite checkpoint advances after a successful sink write.
// When changeTime is nil but orderByVal is set (e.g. non-timestamp changeTrackingColumn), orderBy-only ack is used.
func AssignCompositeSourceAck(msg *types.Message, cp *CompositeCheckpointHolder, changeTime *time.Time, orderByVal interface{}) {
	if msg == nil || cp == nil {
		return
	}
	if changeTime != nil {
		ct := *changeTime
		msg.Ack = cp.MakeAck(&ct, orderByVal, true)
	} else if orderByVal != nil {
		msg.Ack = cp.MakeAck(nil, orderByVal, false)
	}
}

// MakeAck returns a callback that advances the checkpoint after sink write.
func (h *CompositeCheckpointHolder) MakeAck(changeTime *time.Time, orderBy interface{}, requireTime bool) func() {
	return func() {
		next := checkpoint.Composite{OrderByValue: orderBy}
		if changeTime != nil {
			t := *changeTime
			next.ChangeTime = &t
		}
		h.Advance(next, requireTime)
	}
}

// BuildIncrementalSelect builds SELECT with composite or legacy WHERE clauses.
func BuildIncrementalSelect(in IncrementalSelectInput) string {
	orderCol := in.Dialect.QuoteIdentifier(ResolveOrderByColumn(in.OrderByColumn))
	changeExpr := in.ChangeTrackingExpr

	var orderClause string
	if changeExpr != "" {
		orderClause = changeExpr + ", " + orderCol
	} else {
		orderClause = orderCol
	}

	base := "SELECT * FROM " + in.FromExpr
	state := in.State

	if state.ChangeTime == nil && state.OrderByValue == nil {
		return base + " ORDER BY " + orderClause
	}

	if state.ChangeTime != nil && state.OrderByValue != nil && changeExpr != "" {
		return fmt.Sprintf("%s WHERE (%s, %s) > (%s, %s) ORDER BY %s",
			base, changeExpr, orderCol,
			in.Dialect.FormatLiteral(*state.ChangeTime), in.Dialect.FormatLiteral(state.OrderByValue),
			orderClause)
	}

	if state.ChangeTime != nil && changeExpr != "" {
		return fmt.Sprintf("%s WHERE %s > %s ORDER BY %s",
			base, changeExpr, in.Dialect.FormatLiteral(*state.ChangeTime), orderClause)
	}

	if state.OrderByValue != nil {
		return fmt.Sprintf("%s WHERE %s > %s ORDER BY %s",
			base, orderCol, in.Dialect.FormatLiteral(state.OrderByValue), orderCol)
	}

	return base + " ORDER BY " + orderClause
}

// ResolveChangeTrackingExpr returns the SQL expression for the change-tracking column.
func ResolveChangeTrackingExpr(dialect SQLDialect, column string, coalesceUpdatedAt bool) string {
	if coalesceUpdatedAt && column == "updated_at" {
		return "COALESCE(updated_at, created_at)"
	}
	return dialect.QuoteIdentifier(column)
}

// ChangeTimeFallback supplies PostgreSQL updated_at/created_at indices when the tracking column is absent.
type ChangeTimeFallback struct {
	UseUpdatedAtCreatedAt bool
	CreatedAtIndex        int
	UpdatedAtIndex        int
}

// ExtractRowCheckpoint reads change time and order-by value from a scanned row.
func ExtractRowCheckpoint(
	values []interface{},
	changeIndex, orderIndex int,
	fallback *ChangeTimeFallback,
) (changeTime *time.Time, orderBy interface{}) {
	if changeIndex >= 0 && len(values) > changeIndex {
		if ts, ok := values[changeIndex].(time.Time); ok {
			changeTime = &ts
		}
	}
	if fallback != nil && fallback.UseUpdatedAtCreatedAt && changeTime == nil {
		if fallback.UpdatedAtIndex >= 0 && len(values) > fallback.UpdatedAtIndex {
			if ts, ok := values[fallback.UpdatedAtIndex].(time.Time); ok {
				changeTime = &ts
			}
		}
		if changeTime == nil && fallback.CreatedAtIndex >= 0 && len(values) > fallback.CreatedAtIndex {
			if ts, ok := values[fallback.CreatedAtIndex].(time.Time); ok {
				changeTime = &ts
			}
		}
	}
	if orderIndex >= 0 && len(values) > orderIndex {
		orderBy = values[orderIndex]
	}
	return changeTime, orderBy
}

// postgresDialect formats PostgreSQL SQL literals and identifiers.
type postgresDialect struct{}

func (postgresDialect) QuoteIdentifier(name string) string {
	return quotePostgreSQLIdentifier(name)
}

func (postgresDialect) FormatLiteral(v interface{}) string {
	return formatPostgreSQLLiteral(v)
}

// trinoDialect formats Trino SQL literals and identifiers.
type trinoDialect struct{}

func (trinoDialect) QuoteIdentifier(name string) string {
	return name
}

func (trinoDialect) FormatLiteral(v interface{}) string {
	return formatTrinoLiteral(v)
}

// clickHouseDialect formats ClickHouse SQL literals and identifiers.
type clickHouseDialect struct{}

func (clickHouseDialect) QuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func (clickHouseDialect) FormatLiteral(v interface{}) string {
	return formatClickHouseLiteral(v)
}

func formatTrinoLiteral(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case time.Time:
		return formatTrinoTimestamp(val)
	case *time.Time:
		if val == nil {
			return "NULL"
		}
		return formatTrinoTimestamp(*val)
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	case int, int32, int64, uint32, uint64:
		return fmt.Sprintf("%v", val)
	case float64:
		if val == math.Trunc(val) {
			return fmt.Sprintf("%.0f", val)
		}
		return fmt.Sprintf("%v", val)
	default:
		return "'" + strings.ReplaceAll(fmt.Sprintf("%v", val), "'", "''") + "'"
	}
}

func formatTrinoTimestamp(t time.Time) string {
	ts := t.UTC().Format("2006-01-02 15:04:05")
	if t.Nanosecond() > 0 {
		ts = fmt.Sprintf("%s.%06d", ts, t.Nanosecond()/1000)
	}
	return "TIMESTAMP '" + ts + "'"
}

func formatClickHouseLiteral(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case time.Time:
		return "'" + val.UTC().Format("2006-01-02 15:04:05") + "'"
	case *time.Time:
		if val == nil {
			return "NULL"
		}
		return "'" + val.UTC().Format("2006-01-02 15:04:05") + "'"
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	case int, int32, int64:
		return fmt.Sprintf("%d", val)
	case uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float64:
		if val == math.Trunc(val) {
			return fmt.Sprintf("%.0f", val)
		}
		return fmt.Sprintf("%v", val)
	default:
		return "'" + strings.ReplaceAll(fmt.Sprintf("%v", val), "'", "''") + "'"
	}
}

// extractMapRowCheckpoint reads checkpoint fields from a Trino row map.
func extractMapRowCheckpoint(row map[string]interface{}, changeCol, orderCol string) (changeTime *time.Time, orderBy interface{}) {
	if v, ok := row[changeCol]; ok {
		changeTime = valueToTime(v)
	}
	if v, ok := row[orderCol]; ok {
		orderBy = v
	}
	return changeTime, orderBy
}

func valueToTime(v interface{}) *time.Time {
	switch val := v.(type) {
	case time.Time:
		return &val
	case string:
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05"} {
			if t, err := time.Parse(layout, val); err == nil {
				return &t
			}
		}
	}
	return nil
}
