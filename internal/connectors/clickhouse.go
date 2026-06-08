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
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // register clickhouse driver for database/sql

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/checkpoint"
	"github.com/dataflow-operator/dataflow/internal/constants"
	"github.com/dataflow-operator/dataflow/internal/logkeys"
	"github.com/dataflow-operator/dataflow/internal/retry"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
)

// ClickHouseSourceConnector implements SourceConnector for ClickHouse
type ClickHouseSourceConnector struct {
	baseConnectorRWMutex
	connectorLogger
	connectorMetadata
	config            *v1.ClickHouseSourceSpec
	conn              *sql.DB
	cp                CompositeCheckpointHolder
	channelBufferSize int
}

// NewClickHouseSourceConnector creates a new ClickHouse source connector
func NewClickHouseSourceConnector(config *v1.ClickHouseSourceSpec) *ClickHouseSourceConnector {
	return NewClickHouseSourceConnectorWithOptions(config, nil)
}

// NewClickHouseSourceConnectorWithOptions creates a ClickHouse source connector with optional checkpoint persistence.
func NewClickHouseSourceConnectorWithOptions(config *v1.ClickHouseSourceSpec, opts *SourceConnectorOptions) *ClickHouseSourceConnector {
	c := &ClickHouseSourceConnector{
		config:            config,
		connectorLogger:   connectorLogger{logger: logr.Discard()},
		connectorMetadata: connectorMetadata{connectorType: "clickhouse", connectorRole: "source"},
	}
	if opts != nil {
		sourceType := opts.SourceType
		if sourceType == "" {
			sourceType = "clickhouse"
		}
		c.cp.InitCompositeCheckpoint(opts.CheckpointStore, sourceType, opts.InitialCheckpoint)
		if opts.ChannelBufferSize > 0 {
			c.channelBufferSize = opts.ChannelBufferSize
		} else {
			c.channelBufferSize = constants.DefaultChannelBufferSize
		}
	} else {
		c.channelBufferSize = constants.DefaultChannelBufferSize
	}
	return c
}

// Connect establishes connection to ClickHouse
func (c *ClickHouseSourceConnector) Connect(ctx context.Context) error {
	if !c.guardConnect() {
		return fmt.Errorf("connector is closed")
	}
	defer c.Unlock()

	c.logger.Info("Connecting to ClickHouse", "table", c.config.Table)
	conn, err := sql.Open("clickhouse", c.config.ConnectionString)
	if err != nil {
		c.logger.Error(err, "Failed to open ClickHouse connection", "table", c.config.Table)
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		c.logger.Error(err, "Failed to ping ClickHouse", "table", c.config.Table)
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	c.conn = conn
	c.logger.Info("Successfully connected to ClickHouse", "table", c.config.Table)
	return nil
}

// Read returns a channel of messages from ClickHouse
func (c *ClickHouseSourceConnector) Read(ctx context.Context) (<-chan *types.Message, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("not connected, call Connect first")
	}
	c.logger.Info("Starting to read from ClickHouse", "table", c.config.Table)
	pollInterval := 5 * time.Second
	if c.config.PollInterval != nil {
		pollInterval = time.Duration(*c.config.PollInterval) * time.Second
	}
	return runPollingRead(ctx, pollInterval, c.readRows, c.channelBufferSize, &pollingReadOpts{
		logger: c.logger,
		meta:   &c.connectorMetadata,
	}), nil
}

func (c *ClickHouseSourceConnector) readRows(ctx context.Context, msgChan chan *types.Message) error {
	c.RLock()
	if c.Closed() {
		c.RUnlock()
		return nil
	}
	conn := c.conn
	c.RUnlock()

	if conn == nil {
		return nil
	}

	readBatchSize := 0
	if c.config.ReadBatchSize != nil && *c.config.ReadBatchSize > 0 {
		readBatchSize = int(*c.config.ReadBatchSize)
	}

	cfg := c.incrementalConfig()
	changeCol := cfg.ChangeColumn()
	orderByCol := ResolveOrderByColumn(c.config.OrderByColumn)

	pollStart := time.Now()
	stats, err := RunIncrementalBatchPoll(ctx, cfg, readBatchSize, func(ctx context.Context, query string, info BatchPollInfo) (int, checkpoint.Composite, error) {
		batchStart := time.Now()
		c.logger.V(1).Info("Executing ClickHouse query", "query", query, "table", c.config.Table, "batch", info.BatchNumber)
		var rows *sql.Rows
		err := retry.OnRetryableClickHouse(ctx, 3, 1*time.Second, func() error {
			var qerr error
			rows, qerr = conn.QueryContext(ctx, query)
			return qerr
		})
		if err != nil {
			c.RecordError("read", "query_error")
			return 0, checkpoint.Composite{}, fmt.Errorf("clickhouse query: %w", err)
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			c.RecordError("read", "columns_error")
			return 0, checkpoint.Composite{}, fmt.Errorf("clickhouse columns: %w", err)
		}

		idIndex := ColumnIndex(columns, orderByCol)
		changeIndex := ColumnIndex(columns, changeCol)

		rowCount := 0
		var lastRow checkpoint.Composite
		for rows.Next() {
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				c.logger.Error(err, "Failed to scan row", "table", c.config.Table)
				continue
			}

			rowMap := make(map[string]interface{})
			for i, col := range columns {
				rowMap[col] = values[i]
			}

			changeTime, orderByVal := ExtractRowCheckpoint(values, changeIndex, idIndex, nil)
			if changeTime != nil || orderByVal != nil {
				lastRow = RowCheckpoint(changeTime, orderByVal)
			}

			jsonData, err := json.Marshal(rowMap)
			if err != nil {
				c.logger.Error(err, "Failed to marshal row to JSON", "table", c.config.Table)
				continue
			}

			msg := types.NewMessage(jsonData)
			msg.Metadata["table"] = c.config.Table
			if idIndex >= 0 && len(values) > idIndex {
				SetSourceRowIDMetadata(msg, values[idIndex])
			}
			if changeTime != nil {
				ct := *changeTime
				msg.Ack = c.cp.MakeAck(&ct, orderByVal, true)
			} else if orderByVal != nil {
				msg.Ack = c.cp.MakeAck(nil, orderByVal, false)
			}

			select {
			case msgChan <- msg:
			case <-ctx.Done():
				return rowCount, lastRow, ctx.Err()
			}
			rowCount++
		}
		if rowCount > 0 {
			fields := BatchPollLogFields(info, rowCount, time.Since(batchStart), lastRow)
			fields = append([]interface{}{"table", c.config.Table}, fields...)
			c.logger.Info("ClickHouse poll batch completed", fields...)
		}
		return rowCount, lastRow, nil
	})
	if err != nil {
		return err
	}
	if stats.Batches > 0 {
		c.logger.Info("ClickHouse poll cycle completed",
			"table", c.config.Table,
			"batches", stats.Batches,
			"rows_total", stats.TotalRows,
			"duration_ms", time.Since(pollStart).Milliseconds(),
		)
	}
	return nil
}

func (c *ClickHouseSourceConnector) incrementalConfig() IncrementalQueryConfig {
	return IncrementalQueryConfig{
		UserQuery:            c.config.Query,
		ExplicitChangeColumn: c.config.ChangeTrackingColumn,
		DefaultChangeColumn:  "created_at",
		OrderByColumn:        c.config.OrderByColumn,
		LegacyWrap:           LegacyQueryChangeAndOrderBy,
		Dialect:              clickHouseDialect{},
		FromTableExpr:        c.config.Table,
		State:                c.cp.Snapshot(),
	}
}

func (c *ClickHouseSourceConnector) buildReadQuery() string {
	return c.incrementalConfig().ResolveReadQuery()
}

func (c *ClickHouseSourceConnector) wrapQueryWithStableOrder(userQuery string) string {
	cfg := c.incrementalConfig()
	cfg.UserQuery = userQuery
	cfg.ExplicitChangeColumn = ""
	return cfg.ResolveReadQuery()
}

// Close closes the ClickHouse connection
func (c *ClickHouseSourceConnector) Close() error {
	if c.guardClose() {
		return nil
	}
	defer c.Unlock()

	c.logger.Info("Closing ClickHouse source connection", "table", c.config.Table)
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// ClickHouseSinkConnector implements SinkConnector for ClickHouse
type ClickHouseSinkConnector struct {
	baseConnector
	connectorLogger
	connectorMetadata
	progressRecorder
	rawModeConfig
	flattenMetadataSinkState
	config         *v1.ClickHouseSinkSpec
	conn           *sql.DB
	firstWriteOnce sync.Once
}

// NewClickHouseSinkConnector creates a new ClickHouse sink connector
func NewClickHouseSinkConnector(config *v1.ClickHouseSinkSpec) *ClickHouseSinkConnector {
	return &ClickHouseSinkConnector{
		config:            config,
		connectorLogger:   connectorLogger{logger: logr.Discard()},
		connectorMetadata: connectorMetadata{connectorType: "clickhouse", connectorRole: "sink"},
		rawModeConfig: rawModeConfig{
			RawMode:                      config.RawMode,
			FlattenMetadataColumns:       config.FlattenMetadataColumns,
			FlattenMetadataColumnsPrefix: config.FlattenMetadataColumnsPrefix,
		},
	}
}

// Connect establishes connection to ClickHouse
func (c *ClickHouseSinkConnector) Connect(ctx context.Context) error {
	if !c.guardConnect() {
		return fmt.Errorf("connector is closed")
	}
	defer c.Unlock()

	c.logger.Info("Connecting to ClickHouse", "table", c.config.Table)
	conn, err := sql.Open("clickhouse", c.config.ConnectionString)
	if err != nil {
		c.logger.Error(err, "Failed to open ClickHouse connection", "table", c.config.Table)
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		c.logger.Error(err, "Failed to ping ClickHouse", "table", c.config.Table)
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	c.conn = conn
	c.logger.Info("Successfully connected to ClickHouse", "table", c.config.Table)

	if c.rawMode() && c.flattenMetadataColumns() {
		if err := c.connectFlattenMetadata(ctx); err != nil {
			return fmt.Errorf("failed to prepare flatten metadata table: %w", err)
		}
	} else if c.config.AutoCreateTable != nil && *c.config.AutoCreateTable && c.rawMode() {
		if err := c.ensureTable(ctx); err != nil {
			c.logger.Error(err, "Failed to ensure table exists", "table", c.config.Table)
			return fmt.Errorf("failed to ensure table exists: %w", err)
		}
	}

	return nil
}

func (c *ClickHouseSinkConnector) tableExists(ctx context.Context) (bool, error) {
	var count uint64
	query := fmt.Sprintf("SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '%s'", c.config.Table)
	if err := c.conn.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func (c *ClickHouseSinkConnector) ensureTable(ctx context.Context) error {
	exists, err := c.tableExists(ctx)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}
	if exists {
		c.logger.V(1).Info("Table already exists", "table", c.config.Table)
		return nil
	}

	c.logger.Info("Creating table (raw mode)", "table", c.config.Table)
	createQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			data String,
			created_at DateTime DEFAULT now()
		) ENGINE = MergeTree()
		ORDER BY created_at
	`, c.config.Table)

	if _, err := c.conn.ExecContext(ctx, createQuery); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	c.logger.Info("Table created successfully", "table", c.config.Table)
	return nil
}

// ensureTableFromMessage creates the table from the first message structure (replicates source schema).
func (c *ClickHouseSinkConnector) ensureTableFromMessage(ctx context.Context, data map[string]interface{}) error {
	exists, err := c.tableExists(ctx)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}
	if exists {
		return nil
	}

	// Use "value" content if message is in raw format {"value": {...}, "_metadata": {...}}
	rowData := data
	if v, ok := data["value"].(map[string]interface{}); ok && len(data) <= 2 {
		rowData = v
	}

	columns := make([]string, 0, len(rowData))
	for k := range rowData {
		columns = append(columns, k)
	}
	sort.Strings(columns)
	if len(columns) == 0 {
		return fmt.Errorf("cannot create table from empty message")
	}

	hasColumn := func(name string) bool {
		for _, col := range columns {
			if col == name {
				return true
			}
		}
		return false
	}

	colDefs := make([]string, 0, len(columns)+1)
	for _, col := range columns {
		val := rowData[col]
		chType := inferClickHouseType(val)
		colDefs = append(colDefs, fmt.Sprintf("`%s` %s", col, chType))
	}
	if !hasColumn("created_at") {
		colDefs = append(colDefs, "created_at DateTime DEFAULT now()")
	}

	// ORDER BY: use first column (MergeTree requires ORDER BY)
	orderBy := fmt.Sprintf("`%s`", columns[0])

	c.logger.Info("Creating table from message structure", "table", c.config.Table, "columns", columns, "orderBy", orderBy)
	createQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = MergeTree() ORDER BY %s`, c.config.Table, strings.Join(colDefs, ", "), orderBy)
	if _, err := c.conn.ExecContext(ctx, createQuery); err != nil {
		return fmt.Errorf("failed to create table from message: %w", err)
	}
	c.logger.Info("Table created successfully from message structure", "table", c.config.Table)
	return nil
}

// isRFC3339 returns true if s looks like an RFC3339/ISO8601 timestamp.
func isRFC3339(s string) bool {
	if len(s) < 19 {
		return false
	}
	// Accept formats: 2006-01-02T15:04:05Z, 2006-01-02T15:04:05.123Z, 2006-01-02 15:04:05
	if s[4] == '-' && s[7] == '-' && (s[10] == 'T' || s[10] == ' ') && s[13] == ':' && s[16] == ':' {
		return true
	}
	return false
}

// inferClickHouseType infers ClickHouse column type from value only (no column name heuristics).
// Uses precise value analysis: numeric ranges, decimal places, RFC3339 strings.
func inferClickHouseType(v interface{}) string {
	switch val := v.(type) {
	case nil:
		return "Nullable(String)"
	case bool:
		return "UInt8"
	case int:
		return inferIntType(int64(val))
	case int32:
		return "Int32"
	case int64:
		return inferIntType(val)
	case uint:
		return inferUintType(uint64(val))
	case uint32:
		return "UInt32"
	case uint64:
		return inferUintType(val)
	case float32:
		return inferFloatType(float64(val))
	case float64:
		return inferFloatType(val)
	case string:
		if isRFC3339(val) {
			return "DateTime"
		}
		return "String"
	case map[string]interface{}, []interface{}:
		return "String" // JSON as string
	default:
		return "String"
	}
}

func inferIntType(v int64) string {
	if v >= 0 && v <= 255 {
		return "UInt8"
	}
	if v >= -128 && v <= 127 {
		return "Int8"
	}
	if v >= -2147483648 && v <= 2147483647 {
		return "Int32"
	}
	return "Int64"
}

func inferUintType(v uint64) string {
	if v <= 255 {
		return "UInt8"
	}
	if v <= 65535 {
		return "UInt16"
	}
	if v <= 4294967295 {
		return "UInt32"
	}
	return "UInt64"
}

func inferFloatType(f float64) string {
	if isWholeNumber(f) {
		if f >= 0 {
			if f <= 255 {
				return "UInt8"
			}
			if f <= 65535 {
				return "UInt16"
			}
			if f <= 4294967295 {
				return "UInt32"
			}
			return "UInt64"
		} else {
			if f >= -128 && f <= 127 {
				return "Int8"
			}
			if f >= -32768 && f <= 32767 {
				return "Int16"
			}
			if f >= -2147483648 && f <= 2147483647 {
				return "Int32"
			}
			return "Int64"
		}
	}
	// Has decimal part: check if 2 decimal places (typical for price/currency)
	if hasAtMostTwoDecimalPlaces(f) {
		return "Decimal(10, 2)"
	}
	return "Float64"
}

// hasAtMostTwoDecimalPlaces returns true if f has at most 2 decimal places (e.g. 99.99, 100.00).
func hasAtMostTwoDecimalPlaces(f float64) bool {
	scaled := f * 100
	var rounded int64
	if scaled >= 0 {
		rounded = int64(scaled + 0.5)
	} else {
		rounded = int64(scaled - 0.5)
	}
	reconstructed := float64(rounded) / 100
	diff := f - reconstructed
	if diff < 0 {
		diff = -diff
	}
	return diff < 1e-9
}

func toFloat64(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint64:
		return float64(x), true
	default:
		return 0, false
	}
}

func isWholeNumber(f float64) bool {
	return f == float64(int64(f))
}

func (c *ClickHouseSinkConnector) flushBatchRaw(ctx context.Context, msgs []*types.Message) error {
	insertQuery := fmt.Sprintf("INSERT INTO %s (data) VALUES (?)", c.config.Table)
	tx, err := c.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	stmt, err := tx.PrepareContext(ctx, insertQuery)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()
	for _, m := range msgs {
		var data map[string]interface{}
		if err := json.Unmarshal(m.Data, &data); err != nil {
			tx.Rollback()
			return err
		}
		jsonData, _ := json.Marshal(data)
		if _, err := stmt.ExecContext(ctx, string(jsonData)); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to exec: %w", err)
		}
		if m.Ack != nil {
			m.Ack()
		}
	}
	return tx.Commit()
}

func (c *ClickHouseSinkConnector) flushBatchColumnar(ctx context.Context, msgs []*types.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	var firstData map[string]interface{}
	if err := json.Unmarshal(msgs[0].Data, &firstData); err != nil {
		return err
	}

	// Auto-create table from first message if needed
	if c.config.AutoCreateTable != nil && *c.config.AutoCreateTable {
		exists, err := c.tableExists(ctx)
		if err != nil {
			return fmt.Errorf("failed to check table: %w", err)
		}
		if !exists {
			if err := c.ensureTableFromMessage(ctx, firstData); err != nil {
				return err
			}
		}
	}

	rowData := firstData
	if v, ok := firstData["value"].(map[string]interface{}); ok && len(firstData) <= 2 {
		rowData = v
	}
	columns := make([]string, 0, len(rowData))
	for k := range rowData {
		columns = append(columns, k)
	}
	sort.Strings(columns)
	if !contains(columns, "created_at") {
		columns = append(columns, "created_at")
	}

	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	colsQuoted := make([]string, len(columns))
	for i, col := range columns {
		colsQuoted[i] = fmt.Sprintf("`%s`", col)
	}
	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", c.config.Table, strings.Join(colsQuoted, ", "), strings.Join(placeholders, ", "))

	tx, err := c.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	stmt, err := tx.PrepareContext(ctx, insertQuery)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, m := range msgs {
		var data map[string]interface{}
		if err := json.Unmarshal(m.Data, &data); err != nil {
			tx.Rollback()
			return err
		}
		rowData := data
		if v, ok := data["value"].(map[string]interface{}); ok && len(data) <= 2 {
			rowData = v
		}
		values := buildInsertValues(columns, rowData, time.Now)
		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to exec: %w", err)
		}
		if m.Ack != nil {
			m.Ack()
		}
	}
	return tx.Commit()
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

// buildInsertValues constructs values for INSERT from rowData. For created_at, preserves
// source value when present; otherwise uses nowFn() (for backward compatibility).
func buildInsertValues(columns []string, rowData map[string]interface{}, nowFn func() time.Time) []interface{} {
	values := make([]interface{}, len(columns))
	for i, col := range columns {
		if col == "created_at" {
			if v, ok := rowData[col]; ok && v != nil {
				values[i] = v
			} else {
				values[i] = nowFn()
			}
		} else if v, ok := rowData[col]; ok {
			values[i] = v
		} else {
			values[i] = nil
		}
	}
	return values
}

// Write writes messages to ClickHouse
func (c *ClickHouseSinkConnector) Write(ctx context.Context, messages <-chan *types.Message) error {
	if c.conn == nil {
		return fmt.Errorf("not connected, call Connect first")
	}

	cfg := NewBatchWriteConfig(c.config.BatchSize, c.config.BatchFlushIntervalSeconds, 100)
	batchSize := cfg.MaxBatchSize
	if c.config.BatchSize != nil {
		batchSize = int(*c.config.BatchSize)
	}
	flushIntervalSec := 10
	if c.config.BatchFlushIntervalSeconds != nil {
		flushIntervalSec = int(*c.config.BatchFlushIntervalSeconds)
	}
	c.logger.Info("Starting to write messages to ClickHouse", "table", c.config.Table, "batchSize", batchSize, "flushIntervalSeconds", flushIntervalSec)

	messageCount := 0
	return RunBatchWriteLoop(ctx, messages, cfg, BatchWriteOptions{
		Logger:    c.logger,
		LogFields: []any{"table", c.config.Table},
		OnFlush: func(batchCtx context.Context, msgs []*types.Message) error {
			if len(msgs) == 0 {
				return nil
			}
			var flushFn func(context.Context, []*types.Message) error
			if c.rawMode() && c.flattenMetadataColumns() {
				flushFn = c.flushBatchFlattened
			} else if c.rawMode() {
				flushFn = c.flushBatchRaw
			} else {
				flushFn = c.flushBatchColumnar
			}
			return retry.OnRetryableClickHouse(batchCtx, retry.ClickHouseMaxAttempts, retry.ClickHouseInitialBackoff, func() error {
				return flushFn(batchCtx, msgs)
			})
		},
		OnMessage: func(msg *types.Message) bool {
			messageCount++
			var data map[string]interface{}
			if err := json.Unmarshal(msg.Data, &data); err != nil {
				c.logger.Error(err, "Failed to unmarshal message", logkeys.MessageID, types.MessageID(msg), "table", c.config.Table)
				return false
			}
			c.logger.V(1).Info("Received message for ClickHouse", logkeys.MessageID, types.MessageID(msg), "messageNumber", messageCount, "table", c.config.Table, "fields", getKeys(data))
			return true
		},
		OnAck: func(msgs []*types.Message) {
			c.AckMessagesAndNotifyProgress(msgs)
		},
	})
}

// Close closes the ClickHouse connection
func (c *ClickHouseSinkConnector) Close() error {
	if c.guardClose() {
		return nil
	}
	defer c.Unlock()

	c.logger.Info("Closing ClickHouse sink connection", "table", c.config.Table)
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
