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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/checkpoint"
	"github.com/dataflow-operator/dataflow/internal/constants"
	"github.com/dataflow-operator/dataflow/internal/logkeys"
	"github.com/dataflow-operator/dataflow/internal/retry"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
)

// TrinoSourceConnector implements SourceConnector for Trino
type TrinoSourceConnector struct {
	baseConnector
	connectorLogger
	connectorMetadata
	config            *v1.TrinoSourceSpec
	client            *trinoClient
	lastReadID        interface{} // advanced only on Ack (after sink write)
	readStateMu       sync.Mutex  // protects lastReadID
	checkpointStore   checkpoint.Store
	sourceType        string
	channelBufferSize int
}

// NewTrinoSourceConnector creates a new Trino source connector
func NewTrinoSourceConnector(config *v1.TrinoSourceSpec) *TrinoSourceConnector {
	return NewTrinoSourceConnectorWithOptions(config, nil)
}

// NewTrinoSourceConnectorWithOptions creates a Trino source connector with optional checkpoint persistence.
func NewTrinoSourceConnectorWithOptions(config *v1.TrinoSourceSpec, opts *SourceConnectorOptions) *TrinoSourceConnector {
	t := &TrinoSourceConnector{
		config:            config,
		connectorLogger:   connectorLogger{logger: logr.Discard()},
		connectorMetadata: connectorMetadata{connectorType: "trino", connectorRole: "source"},
	}
	if opts != nil {
		t.checkpointStore = opts.CheckpointStore
		t.sourceType = opts.SourceType
		if t.sourceType == "" {
			t.sourceType = "trino"
		}
		if len(opts.InitialCheckpoint) > 0 {
			t.applyInitialCheckpoint(opts.InitialCheckpoint)
		}
		if opts.ChannelBufferSize > 0 {
			t.channelBufferSize = opts.ChannelBufferSize
		} else {
			t.channelBufferSize = constants.DefaultChannelBufferSize
		}
	} else {
		t.channelBufferSize = constants.DefaultChannelBufferSize
	}
	return t
}

// applyInitialCheckpoint restores lastReadID from persisted checkpoint.
func (t *TrinoSourceConnector) applyInitialCheckpoint(data []byte) {
	var m struct {
		LastReadID interface{} `json:"lastReadID"`
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return
	}
	if m.LastReadID != nil {
		t.readStateMu.Lock()
		t.lastReadID = m.LastReadID
		t.readStateMu.Unlock()
	}
}

// Connect establishes connection to Trino
func (t *TrinoSourceConnector) Connect(ctx context.Context) error {
	if !t.guardConnect() {
		return fmt.Errorf("connector is closed")
	}
	defer t.Unlock()

	t.logger.Info("Connecting to Trino",
		"serverURL", t.config.ServerURL,
		"catalog", t.config.Catalog,
		"schema", t.config.Schema,
		"table", t.config.Table)

	client, err := newTrinoClient(ctx, trinoClientConfig{
		ServerURL: t.config.ServerURL,
		Catalog:   t.config.Catalog,
		Schema:    t.config.Schema,
		Keycloak:  t.config.Keycloak,
	}, t.logger)
	if err != nil {
		return err
	}
	if err := client.testConnection(ctx); err != nil {
		return fmt.Errorf("failed to connect to Trino: %w", err)
	}
	t.client = client

	t.logger.Info("Successfully connected to Trino")
	return nil
}

// Read returns a channel of messages from Trino
func (t *TrinoSourceConnector) Read(ctx context.Context) (<-chan *types.Message, error) {
	if t.client == nil {
		return nil, fmt.Errorf("not connected, call Connect first")
	}
	pollInterval := 5 * time.Second
	if t.config.PollInterval != nil {
		pollInterval = time.Duration(*t.config.PollInterval) * time.Second
	}
	return runPollingRead(ctx, pollInterval, t.readRows, t.channelBufferSize, &pollingReadOpts{
		logger: t.logger,
		meta:   &t.connectorMetadata,
	}), nil
}

func (t *TrinoSourceConnector) readRows(ctx context.Context, msgChan chan *types.Message) error {
	t.readStateMu.Lock()
	lastReadID := t.lastReadID
	t.readStateMu.Unlock()

	var query string
	if t.config.Query != "" {
		query = t.config.Query
		t.logger.Info("Using custom query from configuration", "query", query)
	} else {
		// Build query to read from table
		query = fmt.Sprintf("SELECT * FROM %s.%s.%s", t.config.Catalog, t.config.Schema, t.config.Table)
		if lastReadID != nil {
			// Add WHERE clause to filter already read rows (assuming id column exists)
			query = fmt.Sprintf("%s WHERE id > %v ORDER BY id", query, lastReadID)
			t.logger.Info("Built query with lastReadID filter", "query", query, "lastReadID", lastReadID)
		} else {
			query = fmt.Sprintf("%s ORDER BY id", query)
			t.logger.Info("Built query without filter", "query", query)
		}
	}

	rows, err := t.client.executeQuery(ctx, query)
	if err != nil {
		t.RecordError("read", "query_error")
		return err
	}
	if len(rows) == 0 {
		return ErrSourceExhausted
	}

	for _, row := range rows {
		// Ack advances checkpoint only after sink successfully writes; prevents gaps on crash
		var rowID interface{}
		if id, ok := row["id"]; ok {
			rowID = id
		}

		jsonData, err := json.Marshal(row)
		if err != nil {
			t.logger.Error(err, "Failed to marshal row")
			continue
		}

		msg := types.NewMessage(jsonData)
		msg.Metadata["catalog"] = t.config.Catalog
		msg.Metadata["schema"] = t.config.Schema
		msg.Metadata["table"] = t.config.Table
		if rowID != nil {
			rid := rowID
			msg.Ack = func() { t.advanceCheckpoint(rid) }
		}

		select {
		case msgChan <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// advanceCheckpoint updates lastReadID only after sink successfully wrote the message.
func (t *TrinoSourceConnector) advanceCheckpoint(rowID interface{}) {
	t.readStateMu.Lock()
	t.lastReadID = rowID
	toSave := rowID
	t.readStateMu.Unlock()

	if t.checkpointStore != nil && toSave != nil {
		data, _ := json.Marshal(map[string]interface{}{"lastReadID": toSave})
		_ = t.checkpointStore.Save(context.Background(), t.sourceType, data)
	}
}

// Close closes the Trino connection
func (t *TrinoSourceConnector) Close() error {
	if t.guardClose() {
		return nil
	}
	defer t.Unlock()

	t.logger.Info("Closing Trino source connection", "catalog", t.config.Catalog, "schema", t.config.Schema)
	return nil
}

// TrinoSinkConnector implements SinkConnector for Trino
type TrinoSinkConnector struct {
	baseConnector
	connectorLogger
	connectorMetadata
	progressRecorder
	rawModeConfig
	config       *v1.TrinoSinkSpec
	client       *trinoClient
	tableColumns []TableColumnInfo // Cached table columns with types
	columnsMu    sync.RWMutex
}

// NewTrinoSinkConnector creates a new Trino sink connector
func NewTrinoSinkConnector(config *v1.TrinoSinkSpec) *TrinoSinkConnector {
	return &TrinoSinkConnector{
		config:            config,
		connectorLogger:   connectorLogger{logger: logr.Discard()},
		connectorMetadata: connectorMetadata{connectorType: "trino", connectorRole: "sink"},
		rawModeConfig:     rawModeConfig{RawMode: config.RawMode},
	}
}

// Connect establishes connection to Trino
func (t *TrinoSinkConnector) Connect(ctx context.Context) error {
	if !t.guardConnect() {
		return fmt.Errorf("connector is closed")
	}
	defer t.Unlock()

	t.logger.Info("Connecting to Trino",
		"serverURL", t.config.ServerURL,
		"catalog", t.config.Catalog,
		"schema", t.config.Schema,
		"table", t.config.Table)

	client, err := newTrinoClient(ctx, trinoClientConfig{
		ServerURL: t.config.ServerURL,
		Catalog:   t.config.Catalog,
		Schema:    t.config.Schema,
		Keycloak:  t.config.Keycloak,
	}, t.logger)
	if err != nil {
		return err
	}
	t.client = client

	// Connect with retry on transient errors (503/502 from proxy, Trino overload, timeouts)
	err = retry.OnRetryableTrino(ctx, retry.TrinoMaxAttempts, retry.TrinoInitialBackoff, func() error {
		// Only create table in Connect when rawMode (structure known). Non-rawMode defers to first write or table must exist.
		if t.config.AutoCreateTable != nil && *t.config.AutoCreateTable && t.rawMode() {
			if err := t.ensureTable(ctx); err != nil {
				return fmt.Errorf("failed to ensure table exists: %w", err)
			}
		}
		columns, err := t.getTableColumns(ctx)
		if err != nil {
			return fmt.Errorf("failed to get table columns: %w", err)
		}
		t.columnsMu.Lock()
		t.tableColumns = columns
		t.columnsMu.Unlock()
		if err := t.client.testConnection(ctx); err != nil {
			return fmt.Errorf("failed to connect to Trino: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	t.logger.Info("Successfully connected to Trino", "tableColumns", t.tableColumns)
	return nil
}

// TableColumnInfo represents column information from the table
type TableColumnInfo struct {
	Name string
	Type string
}

// getTableColumns returns the list of column names and types for the table
func (t *TrinoSinkConnector) getTableColumns(ctx context.Context) ([]TableColumnInfo, error) {
	query := fmt.Sprintf(
		"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' ORDER BY ordinal_position",
		t.config.Schema,
		t.config.Table,
	)

	rows, err := t.client.executeQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get table columns: %w", err)
	}

	if len(rows) == 0 {
		return nil, fmt.Errorf("table %s.%s.%s does not exist or has no columns", t.config.Catalog, t.config.Schema, t.config.Table)
	}

	columns := make([]TableColumnInfo, 0, len(rows))
	for _, row := range rows {
		colName, nameOk := row["column_name"].(string)
		colType, typeOk := row["data_type"].(string)
		if nameOk && typeOk {
			columns = append(columns, TableColumnInfo{
				Name: colName,
				Type: colType,
			})
		}
	}

	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = col.Name
	}
	t.logger.Info("Retrieved table columns", "table", fmt.Sprintf("%s.%s.%s", t.config.Catalog, t.config.Schema, t.config.Table), "columns", columnNames)
	return columns, nil
}

// ensureTable creates the table if it doesn't exist (rawMode: single data column for JSON storage).
func (t *TrinoSinkConnector) ensureTable(ctx context.Context) error {
	// Check if table exists (escape single quotes for SQL safety)
	escapedSchema := strings.ReplaceAll(t.config.Schema, "'", "''")
	escapedTable := strings.ReplaceAll(t.config.Table, "'", "''")
	checkQuery := fmt.Sprintf(
		"SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'",
		escapedSchema,
		escapedTable,
	)

	rows, err := t.client.executeQuery(ctx, checkQuery)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}

	if len(rows) > 0 {
		// Table exists
		return nil
	}

	// Create table for rawMode: "data" and "_metadata" columns for JSON strings.
	// VARCHAR(1048576) - sufficient for typical JSON messages; Hive uses STRING via connector mapping.
	// WITH (format = 'ORC') - explicit format for Hive; Iceberg also supports ORC.
	quotedCatalog := quoteTrinoIdentifier(t.config.Catalog)
	quotedSchema := quoteTrinoIdentifier(t.config.Schema)
	quotedTable := quoteTrinoIdentifier(t.config.Table)
	createQuery := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s.%s.%s (\"data\" VARCHAR(1048576), \"_metadata\" VARCHAR(1048576)) WITH (format = 'ORC')",
		quotedCatalog,
		quotedSchema,
		quotedTable,
	)

	_, err = t.client.executeQuery(ctx, createQuery)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	t.logger.Info("Table created successfully", "table", t.config.Table)
	return nil
}

// quoteTrinoIdentifier quotes identifier for Trino SQL if it contains special characters.
func quoteTrinoIdentifier(name string) string {
	if name == "" {
		return `""`
	}
	// Quote if contains non-alphanumeric (except underscore)
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_') {
			return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
		}
	}
	return name
}

// unwrapMessageDataForColumns returns the map to use for column lookups.
// If msgData is {"value": {...}, "_metadata": {...}}, returns the inner value map.
// Otherwise returns msgData as-is (plain columnar format).
func unwrapMessageDataForColumns(msgData map[string]interface{}) map[string]interface{} {
	if v, ok := msgData["value"].(map[string]interface{}); ok && len(msgData) <= 2 {
		return v
	}
	return msgData
}

// extractDataAndMetadata extracts data and _metadata from a rawMode message.
// If msg.Data is {"value": ..., "_metadata": ...}, uses those; otherwise uses whole data and msg.Metadata.
func extractDataAndMetadata(msg *types.Message) (dataStr, metaStr string) {
	var parsed map[string]interface{}
	if err := json.Unmarshal(msg.Data, &parsed); err == nil {
		if v, ok := parsed["value"]; ok && len(parsed) <= 2 {
			// Raw format: {"value": ..., "_metadata": ...}
			dataJSON, _ := json.Marshal(v)
			dataStr = string(dataJSON)
			if m, ok := parsed["_metadata"]; ok {
				metaJSON, _ := json.Marshal(m)
				metaStr = string(metaJSON)
			}
			return dataStr, metaStr
		}
	}
	// Plain format: use whole msg.Data as data, msg.Metadata as _metadata
	dataStr = string(msg.Data)
	meta := map[string]interface{}{}
	if msg.Metadata != nil {
		for k, v := range msg.Metadata {
			meta[k] = v
		}
	}
	metaJSON, _ := json.Marshal(meta)
	metaStr = string(metaJSON)
	return dataStr, metaStr
}

// executeBatchRaw inserts messages as JSON strings into dataColumnName and _metadata columns (rawMode).
// dataColumnName is "data" or "value" depending on table schema.
func (t *TrinoSinkConnector) executeBatchRaw(ctx context.Context, batch []*types.Message, dataColumnName string) error {
	if len(batch) == 0 {
		return nil
	}
	valueRows := make([]string, 0, len(batch))
	for _, msg := range batch {
		dataStr, metaStr := extractDataAndMetadata(msg)
		dataEscaped := strings.ReplaceAll(dataStr, "'", "''")
		metaEscaped := strings.ReplaceAll(metaStr, "'", "''")
		valueRows = append(valueRows, fmt.Sprintf("('%s', '%s')", dataEscaped, metaEscaped))
	}
	query := fmt.Sprintf(
		"INSERT INTO %s.%s.%s (\"%s\", \"_metadata\") VALUES %s",
		t.config.Catalog,
		t.config.Schema,
		t.config.Table,
		dataColumnName,
		strings.Join(valueRows, ", "),
	)
	t.logger.Info("Executing batch insert (raw mode)", "batchSize", len(batch), "table", fmt.Sprintf("%s.%s.%s", t.config.Catalog, t.config.Schema, t.config.Table))
	_, err := t.client.executeQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute batch insert: %w", err)
	}
	t.logger.Info("Batch inserted successfully", "count", len(batch))
	return nil
}

// Write writes messages to Trino
func (t *TrinoSinkConnector) Write(ctx context.Context, messages <-chan *types.Message) error {
	if t.client == nil {
		return fmt.Errorf("not connected, call Connect first")
	}

	batchSize := 1
	if t.config.BatchSize != nil {
		batchSize = int(*t.config.BatchSize)
	}
	maxBatchSize := batchSize
	if batchSize == 0 {
		maxBatchSize = constants.MaxBatchSizeWhenTimerOnly
	}

	flushIntervalSec := 10
	if t.config.BatchFlushIntervalSeconds != nil {
		flushIntervalSec = int(*t.config.BatchFlushIntervalSeconds)
	}
	useTimer := flushIntervalSec > 0
	flushInterval := time.Duration(flushIntervalSec) * time.Second

	t.logger.Info("Starting to write messages to Trino", "batchSize", batchSize, "flushIntervalSeconds", flushIntervalSec, "table", fmt.Sprintf("%s.%s.%s", t.config.Catalog, t.config.Schema, t.config.Table))

	batch := make([]*types.Message, 0, maxBatchSize)
	messageCount := 0
	var flushTimer *time.Timer

	stopTimer := func() {
		if flushTimer != nil {
			flushTimer.Stop()
			flushTimer = nil
		}
	}

	doFlush := func() error {
		stopTimer()
		if len(batch) == 0 {
			return nil
		}
		if err := retry.OnRetryableTrino(ctx, retry.TrinoMaxAttempts, retry.TrinoInitialBackoff, func() error {
			return t.executeBatch(ctx, batch)
		}); err != nil {
			return err
		}
		firstMsgID := ""
		if len(batch) > 0 {
			firstMsgID = types.MessageID(batch[0])
		}
		t.logger.Info("Committing source offsets after successful batch", "batchSize", len(batch), logkeys.MessageID, firstMsgID)
		for i, m := range batch {
			if m.Ack != nil {
				m.Ack()
			} else if i == 0 {
				t.logger.V(1).Info("Message has no Ack callback", logkeys.MessageID, types.MessageID(m))
			}
		}
		t.logger.Info("Committed source offsets after successful batch", "batchSize", len(batch), logkeys.MessageID, firstMsgID)
		t.notifyProgress()
		batch = make([]*types.Message, 0, maxBatchSize)
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
				t.logger.Info("Context cancelled, flushing batch", "batchSize", len(batch))
				if len(batch) > 0 {
					if err := doFlush(); err != nil {
						return err
					}
				}
				return ctx.Err()
			case <-flushTimer.C:
				flushTimer = nil
				if len(batch) == 0 {
					continue
				}
				t.logger.Info("Flush interval reached, executing batch", "batchSize", len(batch))
				if err := doFlush(); err != nil {
					t.logger.Error(err, "Failed to execute batch on timer", "batchSize", len(batch))
					return err
				}
			case msg, ok := <-messages:
				if !ok {
					stopTimer()
					t.logger.Info("Message channel closed, flushing batch", "batchSize", len(batch), "totalMessages", messageCount)
					if len(batch) > 0 {
						if err := doFlush(); err != nil {
							return err
						}
					}
					return nil
				}

				messageCount++
				t.logger.Info("Received message for Trino", logkeys.MessageID, types.MessageID(msg), "messageNumber", messageCount, "messageSize", len(msg.Data), "batchSize", len(batch)+1)

				batch = append(batch, msg)

				if len(batch) >= maxBatchSize {
					t.logger.Info("Batch size reached, executing batch", "batchSize", len(batch))
					if err := doFlush(); err != nil {
						firstMsgID := ""
						if len(batch) > 0 {
							firstMsgID = types.MessageID(batch[0])
						}
						t.logger.Error(err, "Failed to execute batch", logkeys.MessageID, firstMsgID, "batchSize", len(batch))
						return err
					}
				}
			}
		} else {
			select {
			case <-ctx.Done():
				stopTimer()
				t.logger.Info("Context cancelled, flushing batch", "batchSize", len(batch))
				if len(batch) > 0 {
					if err := doFlush(); err != nil {
						return err
					}
				}
				return ctx.Err()
			case msg, ok := <-messages:
				if !ok {
					stopTimer()
					t.logger.Info("Message channel closed, flushing batch", "batchSize", len(batch), "totalMessages", messageCount)
					if len(batch) > 0 {
						if err := doFlush(); err != nil {
							return err
						}
					}
					return nil
				}

				messageCount++
				t.logger.Info("Received message for Trino", logkeys.MessageID, types.MessageID(msg), "messageNumber", messageCount, "messageSize", len(msg.Data), "batchSize", len(batch)+1)

				batch = append(batch, msg)

				if len(batch) >= maxBatchSize {
					t.logger.Info("Batch size reached, executing batch", "batchSize", len(batch))
					if err := doFlush(); err != nil {
						firstMsgID := ""
						if len(batch) > 0 {
							firstMsgID = types.MessageID(batch[0])
						}
						t.logger.Error(err, "Failed to execute batch", logkeys.MessageID, firstMsgID, "batchSize", len(batch))
						return err
					}
				}
			}
		}
	}
}

// hasRawModeColumns checks if the table has (data or value) and _metadata columns (required for raw mode).
// Returns (ok, dataColumnName) where dataColumnName is "data" or "value" (prefers "data" when both exist).
func (t *TrinoSinkConnector) hasRawModeColumns(columns []TableColumnInfo) (bool, string) {
	hasData, hasValue, hasMeta := false, false, false
	for _, col := range columns {
		switch col.Name {
		case "data":
			hasData = true
		case "value":
			hasValue = true
		case "_metadata":
			hasMeta = true
		}
	}
	if !hasMeta {
		return false, ""
	}
	if hasData {
		return true, "data"
	}
	if hasValue {
		return true, "value"
	}
	return false, ""
}

func (t *TrinoSinkConnector) executeBatch(ctx context.Context, batch []*types.Message) error {
	if len(batch) == 0 {
		return nil
	}

	// Get table columns (use cached if available, otherwise fetch)
	t.columnsMu.RLock()
	tableColumns := t.tableColumns
	t.columnsMu.RUnlock()

	if len(tableColumns) == 0 {
		// Refresh columns if not cached
		var err error
		tableColumns, err = t.getTableColumns(ctx)
		if err != nil {
			return fmt.Errorf("failed to get table columns: %w", err)
		}
		t.columnsMu.Lock()
		t.tableColumns = tableColumns
		t.columnsMu.Unlock()
	}

	// Use raw mode only when configured AND table has (data or value)/_metadata columns.
	// If rawMode is true but table has different schema (e.g. columnar), fall back to schema-based insert.
	if t.rawMode() {
		ok, dataCol := t.hasRawModeColumns(tableColumns)
		if ok {
			return t.executeBatchRaw(ctx, batch, dataCol)
		}
		t.logger.Info("rawMode is true but target table does not have data/_metadata columns, using schema-based insert",
			"table", fmt.Sprintf("%s.%s.%s", t.config.Catalog, t.config.Schema, t.config.Table))
	}

	// Create a map for fast lookup of table columns by name
	tableColumnsMap := make(map[string]TableColumnInfo)
	for _, col := range tableColumns {
		tableColumnsMap[col.Name] = col
	}

	// Parse all messages to determine which columns from messages exist in table
	// We need to check all messages, not just the first one, because different messages
	// might have different fields (especially after flatten transformation)
	allMessageKeys := make(map[string]bool)

	for i, msg := range batch {
		var msgData map[string]interface{}
		if err := json.Unmarshal(msg.Data, &msgData); err != nil {
			t.logger.Error(err, "Failed to parse message JSON", logkeys.MessageID, types.MessageID(msg), "messageIndex", i, "message", string(msg.Data))
			return fmt.Errorf("failed to parse message JSON at index %d: %w", i, err)
		}

		// Collect all keys from all messages (unwrap {"value": {...}} format if present)
		dataForColumns := unwrapMessageDataForColumns(msgData)
		for k := range dataForColumns {
			allMessageKeys[k] = true
		}
	}

	// Log what keys we found in messages
	messageKeysList := make([]string, 0, len(allMessageKeys))
	for k := range allMessageKeys {
		messageKeysList = append(messageKeysList, k)
	}
	t.logger.Info("Keys found in batch messages",
		"batchSize", len(batch),
		"messageKeys", messageKeysList,
		"totalUniqueKeys", len(messageKeysList))

	// Log first message content for debugging
	if len(batch) > 0 {
		var firstMsgData map[string]interface{}
		if err := json.Unmarshal(batch[0].Data, &firstMsgData); err == nil {
			// Log first message structure (limit size to avoid huge logs)
			dataForPreview := unwrapMessageDataForColumns(firstMsgData)
			firstMsgJSON, _ := json.Marshal(dataForPreview)
			msgPreview := string(firstMsgJSON)
			if len(msgPreview) > 500 {
				msgPreview = msgPreview[:500] + "..."
			}
			firstMsgKeys := make([]string, 0, len(dataForPreview))
			for k := range dataForPreview {
				firstMsgKeys = append(firstMsgKeys, k)
			}
			t.logger.Info("First message in batch (preview)",
				"messageSize", len(batch[0].Data),
				"messagePreview", msgPreview,
				"messageKeys", firstMsgKeys)
		}
	}

	// Use ALL table columns - this ensures all columns are included in INSERT
	// Missing values from messages will be set to NULL
	columnsToUse := tableColumns

	// Log which columns exist in messages vs table columns
	columnsInMessages := make([]string, 0)
	columnsMissingInMessages := make([]string, 0)
	for _, col := range tableColumns {
		if allMessageKeys[col.Name] {
			columnsInMessages = append(columnsInMessages, col.Name)
		} else {
			columnsMissingInMessages = append(columnsMissingInMessages, col.Name)
		}
	}

	if len(columnsMissingInMessages) > 0 {
		t.logger.Info("Some table columns are missing in messages, will use NULL for them",
			"columnsInMessages", columnsInMessages,
			"columnsMissingInMessages", columnsMissingInMessages,
			"totalTableColumns", len(tableColumns))
	} else {
		t.logger.Info("All table columns found in messages",
			"columns", columnsInMessages)
	}

	// Log any extra keys in messages that are not in table (might indicate a problem)
	extraKeys := make([]string, 0)
	for k := range allMessageKeys {
		found := false
		for _, col := range tableColumns {
			if col.Name == k {
				found = true
				break
			}
		}
		if !found {
			extraKeys = append(extraKeys, k)
		}
	}
	if len(extraKeys) > 0 {
		t.logger.Info("Some message keys are not in table columns (will be ignored)",
			"extraKeys", extraKeys)
	}

	// Quote column names that might be reserved keywords
	quotedColumns := make([]string, len(columnsToUse))
	for i, col := range columnsToUse {
		quotedColumns[i] = fmt.Sprintf(`"%s"`, col.Name)
	}
	columnsStr := strings.Join(quotedColumns, ", ")

	// Build VALUES for each message
	var valueRows []string
	for i, msg := range batch {
		// Parse JSON message
		var data map[string]interface{}
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			t.logger.Error(err, "Failed to parse message JSON", logkeys.MessageID, types.MessageID(msg), "messageIndex", i, "message", string(msg.Data))
			return fmt.Errorf("failed to parse message JSON: %w", err)
		}

		// Unwrap {"value": {...}} format if present (e.g. from Kafka with rawMode source)
		data = unwrapMessageDataForColumns(data)

		// Build values for this row - use values from message or NULL
		values := make([]string, len(columnsToUse))
		for j, col := range columnsToUse {
			if val, exists := data[col.Name]; exists {
				// Value exists in message - format according to column type
				values[j] = t.formatValueForType(val, col.Type)
			} else {
				// Value doesn't exist, use typed NULL so Trino can infer column type
				values[j] = t.nullLiteralForTrinoType(col.Type)
			}
		}
		valueRows = append(valueRows, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
	}

	query := fmt.Sprintf(
		"INSERT INTO %s.%s.%s (%s) VALUES %s",
		t.config.Catalog,
		t.config.Schema,
		t.config.Table,
		columnsStr,
		strings.Join(valueRows, ", "),
	)

	t.logger.Info("Executing batch insert", "batchSize", len(batch), "table", fmt.Sprintf("%s.%s.%s", t.config.Catalog, t.config.Schema, t.config.Table), "columns", columnsToUse)

	_, err := t.client.executeQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute batch insert: %w", err)
	}

	t.logger.Info("Batch inserted successfully", "count", len(batch))
	return nil
}

// nullLiteralForTrinoType returns a typed NULL literal for INSERT so Trino can infer the column type (e.g. CAST(NULL AS bigint)).
func (t *TrinoSinkConnector) nullLiteralForTrinoType(columnType string) string {
	return "CAST(NULL AS " + strings.TrimSpace(columnType) + ")"
}

// formatValueForType formats a value for SQL insertion according to column type
func (t *TrinoSinkConnector) formatValueForType(val interface{}, columnType string) string {
	if val == nil {
		return "NULL"
	}

	// Normalize column type (remove size/precision info)
	normalizedType := strings.ToLower(columnType)
	if idx := strings.Index(normalizedType, "("); idx > 0 {
		normalizedType = normalizedType[:idx]
	}
	normalizedType = strings.TrimSpace(normalizedType)

	switch normalizedType {
	case "bigint", "integer", "int", "smallint", "tinyint":
		// Integer types - convert to integer
		switch v := val.(type) {
		case int, int8, int16, int32, int64:
			return fmt.Sprintf("%d", v)
		case uint, uint8, uint16, uint32, uint64:
			return fmt.Sprintf("%d", v)
		case float32, float64:
			// Convert float to int
			return fmt.Sprintf("%.0f", v)
		default:
			// Try to convert to int
			if f, ok := val.(float64); ok {
				return fmt.Sprintf("%.0f", f)
			}
			return fmt.Sprintf("%d", int64(0))
		}

	case "double", "real", "float", "decimal", "numeric":
		// Floating point types
		switch v := val.(type) {
		case float32, float64:
			return fmt.Sprintf("%g", v)
		case int, int8, int16, int32, int64:
			return fmt.Sprintf("%d", v)
		case uint, uint8, uint16, uint32, uint64:
			return fmt.Sprintf("%d", v)
		default:
			return fmt.Sprintf("%g", 0.0)
		}

	case "boolean", "bool":
		// Boolean type
		switch v := val.(type) {
		case bool:
			if v {
				return "true"
			}
			return "false"
		default:
			return "false"
		}

	case "timestamp", "timestamp with time zone", "timestamp without time zone":
		// Timestamp types - try to parse and format
		// Trino requires timestamp format: YYYY-MM-DD HH:MM:SS or YYYY-MM-DDTHH:MM:SS (without timezone offset)
		var parsedTime time.Time
		var err error

		switch v := val.(type) {
		case string:
			// Try to parse various timestamp formats
			// Try RFC3339 first (includes timezone)
			parsedTime, err = time.Parse(time.RFC3339, v)
			if err != nil {
				// Try RFC3339Nano
				parsedTime, err = time.Parse(time.RFC3339Nano, v)
			}
			if err != nil {
				// Try common formats without timezone
				formats := []string{
					"2006-01-02T15:04:05",
					"2006-01-02 15:04:05",
					"2006-01-02T15:04:05.999999",
					"2006-01-02 15:04:05.999999",
				}
				for _, format := range formats {
					parsedTime, err = time.Parse(format, v)
					if err == nil {
						break
					}
				}
			}
			if err != nil {
				// If parsing fails, try to use as-is but remove timezone offset
				// This handles cases like "2026-01-16T13:55:03+08:00"
				if strings.Contains(v, "+") || strings.Contains(v, "-") {
					// Remove timezone offset (everything after + or - at the end)
					parts := strings.Split(v, "+")
					if len(parts) > 1 {
						v = parts[0]
					} else {
						parts = strings.Split(v, "-")
						if len(parts) > 4 { // YYYY-MM-DD-HH:MM:SS format
							v = strings.Join(parts[:3], "-") + "T" + strings.Join(parts[3:], ":")
						}
					}
					// Remove 'Z' suffix if present
					v = strings.TrimSuffix(v, "Z")
					escaped := strings.ReplaceAll(v, "'", "''")
					return fmt.Sprintf("TIMESTAMP '%s'", escaped)
				}
				// Fallback: use string as-is
				escaped := strings.ReplaceAll(v, "'", "''")
				return fmt.Sprintf("TIMESTAMP '%s'", escaped)
			}
		case float64:
			// Unix timestamp (seconds or milliseconds)
			if v > 1e10 {
				// Likely milliseconds
				parsedTime = time.Unix(int64(v/1000), 0).UTC()
			} else {
				// Likely seconds
				parsedTime = time.Unix(int64(v), 0).UTC()
			}
		case int64:
			// Unix timestamp
			if v > 1e10 {
				// Likely milliseconds
				parsedTime = time.Unix(v/1000, 0).UTC()
			} else {
				// Likely seconds
				parsedTime = time.Unix(v, 0).UTC()
			}
		default:
			// Convert to string and try to parse
			strVal := fmt.Sprintf("%v", v)
			parsedTime, err = time.Parse(time.RFC3339, strVal)
			if err != nil {
				// Fallback: use string as-is
				escaped := strings.ReplaceAll(strVal, "'", "''")
				return fmt.Sprintf("TIMESTAMP '%s'", escaped)
			}
		}

		// Format timestamp for Trino: YYYY-MM-DD HH:MM:SS (without timezone)
		// Trino doesn't accept RFC3339 format with timezone offset
		timestampStr := parsedTime.UTC().Format("2006-01-02 15:04:05")
		// If there are microseconds, add them
		if parsedTime.Nanosecond() > 0 {
			microseconds := parsedTime.Nanosecond() / 1000
			timestampStr = fmt.Sprintf("%s.%06d", timestampStr, microseconds)
		}
		return fmt.Sprintf("TIMESTAMP '%s'", timestampStr)

	case "date":
		// Date type
		switch v := val.(type) {
		case string:
			escaped := strings.ReplaceAll(v, "'", "''")
			return fmt.Sprintf("DATE '%s'", escaped)
		default:
			strVal := fmt.Sprintf("%v", v)
			escaped := strings.ReplaceAll(strVal, "'", "''")
			return fmt.Sprintf("DATE '%s'", escaped)
		}

	case "row", "array":
		// ROW and ARRAY: serialize value to JSON and use CAST(JSON '...' AS columnType)
		return t.formatComplexTypeAsCastJSON(val, columnType)

	case "varchar", "char", "text", "string":
		// String types
		switch v := val.(type) {
		case string:
			escaped := strings.ReplaceAll(v, "'", "''")
			return fmt.Sprintf("'%s'", escaped)
		case []byte:
			escaped := strings.ReplaceAll(string(v), "'", "''")
			return fmt.Sprintf("'%s'", escaped)
		case map[string]interface{}, []interface{}:
			// For complex types, convert to JSON string
			jsonBytes, err := json.Marshal(v)
			if err != nil {
				t.logger.Error(err, "Failed to marshal value to JSON", "value", v)
				return "NULL"
			}
			escaped := strings.ReplaceAll(string(jsonBytes), "'", "''")
			return fmt.Sprintf("'%s'", escaped)
		default:
			// Convert to string
			escaped := strings.ReplaceAll(fmt.Sprintf("%v", v), "'", "''")
			return fmt.Sprintf("'%s'", escaped)
		}

	default:
		// Unknown type - treat as string
		switch v := val.(type) {
		case string:
			escaped := strings.ReplaceAll(v, "'", "''")
			return fmt.Sprintf("'%s'", escaped)
		case []byte:
			escaped := strings.ReplaceAll(string(v), "'", "''")
			return fmt.Sprintf("'%s'", escaped)
		default:
			escaped := strings.ReplaceAll(fmt.Sprintf("%v", v), "'", "''")
			return fmt.Sprintf("'%s'", escaped)
		}
	}
}

// formatComplexTypeAsCastJSON serializes val to JSON and returns CAST(JSON '...' AS columnType) for ROW/ARRAY columns.
func (t *TrinoSinkConnector) formatComplexTypeAsCastJSON(val interface{}, columnType string) string {
	jsonBytes, err := json.Marshal(val)
	if err != nil {
		t.logger.Error(err, "Failed to marshal value to JSON for ROW/ARRAY", "value", val)
		return "NULL"
	}
	escaped := strings.ReplaceAll(string(jsonBytes), "'", "''")
	return "CAST(JSON '" + escaped + "' AS " + strings.TrimSpace(columnType) + ")"
}

// Close closes the Trino connection
func (t *TrinoSinkConnector) Close() error {
	if t.guardClose() {
		return nil
	}
	defer t.Unlock()

	t.logger.Info("Closing Trino sink connection", "catalog", t.config.Catalog, "schema", t.config.Schema, "table", t.config.Table)
	return nil
}

// TrinoQueryResponse represents a response from Trino REST API
type TrinoQueryResponse struct {
	ID      string          `json:"id"`
	InfoURI string          `json:"infoUri"`
	NextURI string          `json:"nextUri"`
	Data    [][]interface{} `json:"data"` // Trino returns data as array of arrays
	Columns []TrinoColumn   `json:"columns"`
	Stats   TrinoStats      `json:"stats"`
	Error   *TrinoError     `json:"error,omitempty"` // Error information if query failed
}

// TrinoError represents error information from Trino
type TrinoError struct {
	Message       string                 `json:"message"`
	ErrorName     string                 `json:"errorName"`
	ErrorCode     int                    `json:"errorCode"`
	ErrorLocation *TrinoErrorLocation    `json:"errorLocation,omitempty"`
	FailureInfo   map[string]interface{} `json:"failureInfo,omitempty"`
}

// TrinoErrorLocation represents the location of an error in a query
type TrinoErrorLocation struct {
	LineNumber   int `json:"lineNumber"`
	ColumnNumber int `json:"columnNumber"`
}

// TrinoColumn represents a column in Trino query response
type TrinoColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// TrinoStats represents statistics about a Trino query
type TrinoStats struct {
	State           string `json:"state"`
	Queued          bool   `json:"queued"`
	Scheduled       bool   `json:"scheduled"`
	Nodes           int    `json:"nodes"`
	TotalSplits     int    `json:"totalSplits"`
	QueuedSplits    int    `json:"queuedSplits"`
	RunningSplits   int    `json:"runningSplits"`
	CompletedSplits int    `json:"completedSplits"`
}
