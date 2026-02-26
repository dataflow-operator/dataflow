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
	"sync"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // register clickhouse driver for database/sql

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/constants"
	"github.com/dataflow-operator/dataflow/internal/logkeys"
	"github.com/dataflow-operator/dataflow/internal/retry"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
)

// ClickHouseSourceConnector implements SourceConnector for ClickHouse
type ClickHouseSourceConnector struct {
	baseConnectorRWMutex
	config       *v1.ClickHouseSourceSpec
	conn         *sql.DB
	logger       logr.Logger
	lastReadID   int64      // Track last read ID to avoid duplicates
	lastReadTime *time.Time // Track last read time to avoid duplicates
	readStateMu  sync.Mutex // protects lastReadID, lastReadTime (separate from conn to avoid blocking Connect/Close)
}

// NewClickHouseSourceConnector creates a new ClickHouse source connector
func NewClickHouseSourceConnector(config *v1.ClickHouseSourceSpec) *ClickHouseSourceConnector {
	return &ClickHouseSourceConnector{
		config: config,
		logger: logr.Discard(),
	}
}

// SetLogger sets the logger for the connector
func (c *ClickHouseSourceConnector) SetLogger(logger logr.Logger) {
	c.logger = logger
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
	msgChan := make(chan *types.Message, constants.DefaultChannelBufferSize)

	go func() {
		defer close(msgChan)

		pollInterval := 5 * time.Second
		if c.config.PollInterval != nil {
			pollInterval = time.Duration(*c.config.PollInterval) * time.Second
		}

		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()

		c.readRows(ctx, msgChan)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.readRows(ctx, msgChan)
			}
		}
	}()

	return msgChan, nil
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
	rows, err := conn.QueryContext(ctx, query)
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

		select {
		case msgChan <- msg:
		case <-ctx.Done():
			c.readStateMu.Lock()
			if maxReadID > c.lastReadID {
				c.lastReadID = maxReadID
			}
			if maxReadTime != nil && (c.lastReadTime == nil || maxReadTime.After(*c.lastReadTime)) {
				c.lastReadTime = maxReadTime
			}
			c.readStateMu.Unlock()
			return
		}
	}

	c.readStateMu.Lock()
	if maxReadID > c.lastReadID {
		c.lastReadID = maxReadID
	}
	if maxReadTime != nil && (c.lastReadTime == nil || maxReadTime.After(*c.lastReadTime)) {
		c.lastReadTime = maxReadTime
	}
	c.readStateMu.Unlock()
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
	config         *v1.ClickHouseSinkSpec
	conn           *sql.DB
	logger         logr.Logger
	firstWriteOnce sync.Once
}

// NewClickHouseSinkConnector creates a new ClickHouse sink connector
func NewClickHouseSinkConnector(config *v1.ClickHouseSinkSpec) *ClickHouseSinkConnector {
	return &ClickHouseSinkConnector{
		config: config,
		logger: logr.Discard(),
	}
}

// SetLogger sets the logger for the connector
func (c *ClickHouseSinkConnector) SetLogger(logger logr.Logger) {
	c.logger = logger
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

	if c.config.AutoCreateTable != nil && *c.config.AutoCreateTable {
		if err := c.ensureTable(ctx); err != nil {
			c.logger.Error(err, "Failed to ensure table exists", "table", c.config.Table)
			return fmt.Errorf("failed to ensure table exists: %w", err)
		}
	}

	return nil
}

func (c *ClickHouseSinkConnector) ensureTable(ctx context.Context) error {
	// Check if table exists
	var count uint64
	query := fmt.Sprintf("SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '%s'", c.config.Table)
	if err := c.conn.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}

	if count > 0 {
		c.logger.V(1).Info("Table already exists", "table", c.config.Table)
		return nil
	}

	c.logger.Info("Creating table", "table", c.config.Table)
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

// Write writes messages to ClickHouse
func (c *ClickHouseSinkConnector) Write(ctx context.Context, messages <-chan *types.Message) error {
	if c.conn == nil {
		return fmt.Errorf("not connected, call Connect first")
	}

	batchSize := 100
	if c.config.BatchSize != nil {
		batchSize = int(*c.config.BatchSize)
	}

	c.logger.Info("Starting to write messages to ClickHouse", "table", c.config.Table, "batchSize", batchSize)
	messageCount := 0
	var batch []*types.Message
	insertQuery := fmt.Sprintf("INSERT INTO %s (data) VALUES (?)", c.config.Table)

	flushBatch := func(msgs []*types.Message) error {
		if len(msgs) == 0 {
			return nil
		}
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

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				if err := retry.OnTimeout(ctx, retry.DefaultMaxAttempts, retry.DefaultInitialBackoff, func() error {
					return flushBatch(batch)
				}); err != nil {
					return err
				}
			}
			return ctx.Err()
		case msg, ok := <-messages:
			if !ok {
				if len(batch) > 0 {
					c.logger.Info("Message channel closed, flushing batch", "batchSize", len(batch), "totalMessages", messageCount, "table", c.config.Table)
					if err := retry.OnTimeout(ctx, retry.DefaultMaxAttempts, retry.DefaultInitialBackoff, func() error {
						return flushBatch(batch)
					}); err != nil {
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

			if len(batch) >= batchSize {
				c.logger.V(1).Info("Batch size reached, sending batch", "batchSize", len(batch), "table", c.config.Table)
				toFlush := batch
				batch = nil
				if err := retry.OnTimeout(ctx, retry.DefaultMaxAttempts, retry.DefaultInitialBackoff, func() error {
					return flushBatch(toFlush)
				}); err != nil {
					c.logger.Error(err, "Failed to send batch", "batchSize", len(toFlush), "table", c.config.Table)
					return err
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
