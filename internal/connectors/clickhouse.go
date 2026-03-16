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
	config          *v1.ClickHouseSourceSpec
	conn            *sql.DB
	lastReadID      int64      // Track last read ID to avoid duplicates
	lastReadTime    *time.Time // Track last read time to avoid duplicates
	readStateMu     sync.Mutex // protects lastReadID, lastReadTime (separate from conn to avoid blocking Connect/Close)
	checkpointStore   checkpoint.Store
	sourceType        string
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
		c.checkpointStore = opts.CheckpointStore
		c.sourceType = opts.SourceType
		if c.sourceType == "" {
			c.sourceType = "clickhouse"
		}
		if len(opts.InitialCheckpoint) > 0 {
			c.applyInitialCheckpoint(opts.InitialCheckpoint)
		}
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

// applyInitialCheckpoint restores lastReadID and lastReadTime from persisted checkpoint.
func (c *ClickHouseSourceConnector) applyInitialCheckpoint(data []byte) {
	var m struct {
		LastReadID   int64  `json:"lastReadID"`
		LastReadTime string `json:"lastReadTime"`
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return
	}
	c.readStateMu.Lock()
	defer c.readStateMu.Unlock()
	if m.LastReadID > 0 && m.LastReadID > c.lastReadID {
		c.lastReadID = m.LastReadID
	}
	if m.LastReadTime != "" {
		t, err := time.Parse("2006-01-02 15:04:05", m.LastReadTime)
		if err != nil {
			t, err = time.Parse(time.RFC3339, m.LastReadTime)
			if err != nil {
				return
			}
		}
		if c.lastReadTime == nil || t.After(*c.lastReadTime) {
			c.lastReadTime = &t
		}
	}
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
	return runPollingRead(ctx, pollInterval, c.readRows, c.channelBufferSize), nil
}

func (c *ClickHouseSourceConnector) readRows(ctx context.Context, msgChan chan *types.Message) {
	// Read conn and state under RLock; release before long-running query so Connect/Close are not blocked
	c.RLock()
	if c.Closed() {
		c.RUnlock()
		return
	}
	conn := c.conn
	c.RUnlock()

	if conn == nil {
		return
	}

	c.readStateMu.Lock()
	lastReadID := c.lastReadID
	var lastReadTime *time.Time
	if c.lastReadTime != nil {
		t := *c.lastReadTime
		lastReadTime = &t
	}
	c.readStateMu.Unlock()

	var query string
	if c.config.Query != "" {
		query = c.config.Query
	} else {
		if lastReadID > 0 {
			query = fmt.Sprintf("SELECT * FROM %s WHERE id > %d ORDER BY id", c.config.Table, lastReadID)
		} else if lastReadTime != nil {
			query = fmt.Sprintf("SELECT * FROM %s WHERE created_at > '%s' ORDER BY created_at",
				c.config.Table, lastReadTime.Format("2006-01-02 15:04:05"))
		} else {
			query = fmt.Sprintf("SELECT * FROM %s", c.config.Table)
		}
	}

	c.logger.V(1).Info("Executing ClickHouse query", "query", query, "table", c.config.Table)
	var rows *sql.Rows
	err := retry.OnRetryableClickHouse(ctx, 3, 1*time.Second, func() error {
		var qerr error
		rows, qerr = conn.QueryContext(ctx, query)
		return qerr
	})
	if err != nil {
		c.logger.Error(err, "Failed to execute ClickHouse query", "query", query, "table", c.config.Table)
		return
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		c.logger.Error(err, "Failed to get columns", "table", c.config.Table)
		return
	}

	var idIndex = -1
	var createdAtIndex = -1
	for i, col := range columns {
		if col == "id" {
			idIndex = i
		}
		if col == "created_at" {
			createdAtIndex = i
		}
	}

	var maxReadID int64 = lastReadID
	var maxReadTime *time.Time
	if lastReadTime != nil {
		t := *lastReadTime
		maxReadTime = &t
	}

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

		if idIndex >= 0 {
			if id, ok := values[idIndex].(uint64); ok {
				if int64(id) > maxReadID {
					maxReadID = int64(id)
				}
			} else if id, ok := values[idIndex].(int64); ok {
				if id > maxReadID {
					maxReadID = id
				}
			} else if id, ok := values[idIndex].(int32); ok {
				if int64(id) > maxReadID {
					maxReadID = int64(id)
				}
			}
		}
		if createdAtIndex >= 0 {
			if ts, ok := values[createdAtIndex].(time.Time); ok {
				if maxReadTime == nil || ts.After(*maxReadTime) {
					t := ts
					maxReadTime = &t
				}
			}
		}

		jsonData, err := json.Marshal(rowMap)
		if err != nil {
			c.logger.Error(err, "Failed to marshal row to JSON", "table", c.config.Table)
			continue
		}

		msg := types.NewMessage(jsonData)
		msg.Metadata["table"] = c.config.Table
		if idIndex >= 0 && len(values) > idIndex {
			msg.Metadata["id"] = values[idIndex]
		}
		// Ack advances checkpoint only after sink successfully writes; prevents data loss on crash
		rowID, rowTime := c.extractRowCheckpoint(values, idIndex, createdAtIndex)
		if rowID > 0 || rowTime != nil {
			msg.Ack = func() { c.advanceCheckpoint(rowID, rowTime) }
		}

		select {
		case msgChan <- msg:
		case <-ctx.Done():
			return
		}
	}
}

// extractRowCheckpoint returns (id, created_at) for checkpoint advancement.
func (c *ClickHouseSourceConnector) extractRowCheckpoint(values []interface{}, idIndex, createdAtIndex int) (int64, *time.Time) {
	var rowID int64
	if idIndex >= 0 && len(values) > idIndex {
		switch v := values[idIndex].(type) {
		case uint64:
			rowID = int64(v)
		case int64:
			rowID = v
		case int32:
			rowID = int64(v)
		}
	}
	var rowTime *time.Time
	if createdAtIndex >= 0 && len(values) > createdAtIndex {
		if ts, ok := values[createdAtIndex].(time.Time); ok {
			rowTime = &ts
		}
	}
	return rowID, rowTime
}

// advanceCheckpoint updates lastReadID/lastReadTime only after sink successfully wrote the message.
// Called from Ack callback (different goroutine).
func (c *ClickHouseSourceConnector) advanceCheckpoint(rowID int64, rowTime *time.Time) {
	c.readStateMu.Lock()
	if rowID > 0 && rowID > c.lastReadID {
		c.lastReadID = rowID
	}
	if rowTime != nil && (c.lastReadTime == nil || rowTime.After(*c.lastReadTime)) {
		t := *rowTime
		c.lastReadTime = &t
	}
	lastID := c.lastReadID
	lastTime := c.lastReadTime
	c.readStateMu.Unlock()

	if c.checkpointStore != nil {
		m := map[string]interface{}{"lastReadID": lastID}
		if lastTime != nil {
			m["lastReadTime"] = lastTime.Format("2006-01-02 15:04:05")
		}
		data, _ := json.Marshal(m)
		_ = c.checkpointStore.Save(context.Background(), c.sourceType, data)
	}
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
	rawModeConfig
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
		rawModeConfig:     rawModeConfig{RawMode: config.RawMode},
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

	// Only create table in Connect when rawMode (structure known). Non-rawMode defers to first write.
	if c.config.AutoCreateTable != nil && *c.config.AutoCreateTable && c.rawMode() {
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

	batchSize := 100
	if c.config.BatchSize != nil {
		batchSize = int(*c.config.BatchSize)
	}
	maxBatchSize := batchSize
	if batchSize == 0 {
		maxBatchSize = constants.MaxBatchSizeWhenTimerOnly
	}

	flushIntervalSec := 10
	if c.config.BatchFlushIntervalSeconds != nil {
		flushIntervalSec = int(*c.config.BatchFlushIntervalSeconds)
	}
	useTimer := flushIntervalSec > 0
	flushInterval := time.Duration(flushIntervalSec) * time.Second

	c.logger.Info("Starting to write messages to ClickHouse", "table", c.config.Table, "batchSize", batchSize, "flushIntervalSeconds", flushIntervalSec)
	messageCount := 0
	var batch []*types.Message
	var flushTimer *time.Timer

	stopTimer := func() {
		if flushTimer != nil {
			flushTimer.Stop()
			flushTimer = nil
		}
	}

	flushBatch := func(batchCtx context.Context, msgs []*types.Message) error {
		if len(msgs) == 0 {
			return nil
		}
		if c.rawMode() {
			return c.flushBatchRaw(batchCtx, msgs)
		}
		return c.flushBatchColumnar(batchCtx, msgs)
	}

	doFlush := func(toFlush []*types.Message) error {
		stopTimer()
		if len(toFlush) == 0 {
			return nil
		}
		// Use non-cancelled context for batch execution when ctx is done (e.g. Ctrl+C).
		// Connection may close on context cancellation; a fresh context allows the flush to complete.
		batchCtx := ctx
		if batchCtx.Err() != nil {
			var cancel context.CancelFunc
			batchCtx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
		}
		if err := retry.OnRetryableClickHouse(batchCtx, retry.ClickHouseMaxAttempts, retry.ClickHouseInitialBackoff, func() error {
			return flushBatch(batchCtx, toFlush)
		}); err != nil {
			return err
		}
		return nil
	}

	for {
		if useTimer && len(batch) > 0 && flushTimer == nil {
			flushTimer = time.NewTimer(flushInterval)
		}

		if useTimer && flushTimer != nil {
			select {
			case <-ctx.Done():
				stopTimer()
				if len(batch) > 0 {
					c.logger.Info("Context cancelled, flushing batch", "batchSize", len(batch), "table", c.config.Table)
					if err := doFlush(batch); err != nil {
						return err
					}
				}
				return ctx.Err()
			case <-flushTimer.C:
				flushTimer = nil
				if len(batch) == 0 {
					continue
				}
				c.logger.V(1).Info("Flush interval reached, sending batch", "batchSize", len(batch), "table", c.config.Table)
				toFlush := batch
				batch = nil
				if err := doFlush(toFlush); err != nil {
					c.logger.Error(err, "Failed to send batch on timer", "batchSize", len(toFlush), "table", c.config.Table)
					return err
				}
			case msg, ok := <-messages:
				if !ok {
					stopTimer()
					if len(batch) > 0 {
						c.logger.Info("Message channel closed, flushing batch", "batchSize", len(batch), "totalMessages", messageCount, "table", c.config.Table)
						if err := doFlush(batch); err != nil {
							return err
						}
					}
					c.logger.Info("Message channel closed", "totalMessages", messageCount, "table", c.config.Table)
					return nil
				}

				messageCount++
				var data map[string]interface{}
				if err := json.Unmarshal(msg.Data, &data); err != nil {
					c.logger.Error(err, "Failed to unmarshal message", logkeys.MessageID, types.MessageID(msg), "table", c.config.Table)
					continue
				}

				c.logger.V(1).Info("Received message for ClickHouse", logkeys.MessageID, types.MessageID(msg), "messageNumber", messageCount, "table", c.config.Table, "fields", getKeys(data))

				batch = append(batch, msg)

				if len(batch) >= maxBatchSize {
					c.logger.V(1).Info("Batch size reached, sending batch", "batchSize", len(batch), "table", c.config.Table)
					toFlush := batch
					batch = nil
					if err := doFlush(toFlush); err != nil {
						c.logger.Error(err, "Failed to send batch", "batchSize", len(toFlush), "table", c.config.Table)
						return err
					}
				}
			}
		} else {
			select {
			case <-ctx.Done():
				stopTimer()
				if len(batch) > 0 {
					c.logger.Info("Context cancelled, flushing batch", "batchSize", len(batch), "table", c.config.Table)
					if err := doFlush(batch); err != nil {
						return err
					}
				}
				return ctx.Err()
			case msg, ok := <-messages:
				if !ok {
					stopTimer()
					if len(batch) > 0 {
						c.logger.Info("Message channel closed, flushing batch", "batchSize", len(batch), "totalMessages", messageCount, "table", c.config.Table)
						if err := doFlush(batch); err != nil {
							return err
						}
					}
					c.logger.Info("Message channel closed", "totalMessages", messageCount, "table", c.config.Table)
					return nil
				}

				messageCount++
				var data map[string]interface{}
				if err := json.Unmarshal(msg.Data, &data); err != nil {
					c.logger.Error(err, "Failed to unmarshal message", logkeys.MessageID, types.MessageID(msg), "table", c.config.Table)
					continue
				}

				c.logger.V(1).Info("Received message for ClickHouse", logkeys.MessageID, types.MessageID(msg), "messageNumber", messageCount, "table", c.config.Table, "fields", getKeys(data))

				batch = append(batch, msg)

				if len(batch) >= maxBatchSize {
					c.logger.V(1).Info("Batch size reached, sending batch", "batchSize", len(batch), "table", c.config.Table)
					toFlush := batch
					batch = nil
					if err := doFlush(toFlush); err != nil {
						c.logger.Error(err, "Failed to send batch", "batchSize", len(toFlush), "table", c.config.Table)
						return err
					}
				}
			}
		}
	}
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
