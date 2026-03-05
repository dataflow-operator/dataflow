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
	"sort"
	"sync"
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/constants"
	"github.com/dataflow-operator/dataflow/internal/logkeys"
	"github.com/dataflow-operator/dataflow/internal/metrics"
	"github.com/dataflow-operator/dataflow/internal/retry"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5"
)

// PostgreSQLSourceConnector implements SourceConnector for PostgreSQL
type PostgreSQLSourceConnector struct {
	baseConnector
	connectorLogger
	connectorMetadata
	config             *v1.PostgreSQLSourceSpec
	conn               *pgx.Conn
	lastReadChangeTime *time.Time // Track last change time for CDC (ChangeTrackingColumn or COALESCE(updated_at, created_at))
}

// NewPostgreSQLSourceConnector creates a new PostgreSQL source connector
func NewPostgreSQLSourceConnector(config *v1.PostgreSQLSourceSpec) *PostgreSQLSourceConnector {
	return &PostgreSQLSourceConnector{
		config:          config,
		connectorLogger: connectorLogger{logger: logr.Discard()},
	}
}

// Connect establishes connection to PostgreSQL
func (p *PostgreSQLSourceConnector) Connect(ctx context.Context) error {
	if !p.guardConnect() {
		return fmt.Errorf("connector is closed")
	}
	defer p.Unlock()

	p.logger.Info("Connecting to PostgreSQL", "table", p.config.Table)
	conn, err := pgx.Connect(ctx, p.config.ConnectionString)
	if err != nil {
		if p.namespace != "" && p.name != "" {
			metrics.RecordConnectorError(p.namespace, p.name, "postgresql", "source", "connect", "connection_error")
		}
		p.logger.Error(err, "Failed to connect to PostgreSQL", "table", p.config.Table)
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	p.conn = conn
	p.logger.Info("Successfully connected to PostgreSQL", "table", p.config.Table)

	if p.namespace != "" && p.name != "" {
		metrics.SetConnectorConnectionStatus(p.namespace, p.name, "postgresql", "source", true)
	}

	// Auto-create table if enabled (source)
	if p.config.AutoCreateTable != nil && *p.config.AutoCreateTable {
		if err := p.ensureSourceTable(ctx); err != nil {
			if p.namespace != "" && p.name != "" {
				metrics.RecordConnectorError(p.namespace, p.name, "postgresql", "source", "connect", "ensure_table_error")
			}
			p.logger.Error(err, "Failed to ensure source table exists", "table", p.config.Table)
			return fmt.Errorf("failed to ensure source table exists: %w", err)
		}
	}

	return nil
}

// ensureSourceTable creates the table if it doesn't exist (CDC-friendly schema)
func (p *PostgreSQLSourceConnector) ensureSourceTable(ctx context.Context) error {
	var exists bool
	schema, tableName := ParseTableRef(p.config.Table)
	checkQuery := `SELECT EXISTS (
		SELECT FROM information_schema.tables
		WHERE table_schema = $1
		AND table_name = $2
	)`
	err := p.conn.QueryRow(ctx, checkQuery, schema, tableName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}
	if exists {
		p.logger.V(1).Info("Source table already exists", "table", p.config.Table)
		return nil
	}
	p.logger.Info("Creating source table", "table", p.config.Table)
	createQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`, p.config.Table)
	_, err = p.conn.Exec(ctx, createQuery)
	if err != nil {
		return fmt.Errorf("failed to create source table: %w", err)
	}
	p.logger.Info("Source table created successfully", "table", p.config.Table)
	return nil
}

// Read returns a channel of messages from PostgreSQL
func (p *PostgreSQLSourceConnector) Read(ctx context.Context) (<-chan *types.Message, error) {
	if p.conn == nil {
		return nil, fmt.Errorf("not connected, call Connect first")
	}

	p.logger.Info("Starting to read from PostgreSQL", "table", p.config.Table)
	msgChan := make(chan *types.Message, constants.DefaultChannelBufferSize)

	go func() {
		defer close(msgChan)

		pollInterval := 5 * time.Second
		if p.config.PollInterval != nil {
			pollInterval = time.Duration(*p.config.PollInterval) * time.Second
		}

		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()

		// Initial read
		p.readRows(ctx, msgChan)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.readRows(ctx, msgChan)
			}
		}
	}()

	return msgChan, nil
}

func (p *PostgreSQLSourceConnector) readRows(ctx context.Context, msgChan chan *types.Message) {
	p.Lock()
	defer p.Unlock()

	readBatchSize := 0
	if p.config.ReadBatchSize != nil && *p.config.ReadBatchSize > 0 {
		readBatchSize = int(*p.config.ReadBatchSize)
	}

	for {
		var query string
		if p.config.Query != "" {
			query = p.config.Query
		} else {
			query = p.buildReadQuery()
		}

		if readBatchSize > 0 {
			query = fmt.Sprintf("%s LIMIT %d", query, readBatchSize)
		}

		p.logger.V(1).Info("Executing PostgreSQL query", "query", query, "table", p.config.Table)
		rows, err := p.conn.Query(ctx, query)
		if err != nil {
			if p.namespace != "" && p.name != "" {
				metrics.RecordConnectorError(p.namespace, p.name, "postgresql", "source", "read", "query_error")
			}
			p.logger.Error(err, "Failed to execute PostgreSQL query", "query", query, "table", p.config.Table)
			return
		}

		fieldNames := rows.FieldDescriptions()
		var idIndex, createdAtIndex, updatedAtIndex, changeTrackingIndex = -1, -1, -1, -1
		changeCol := p.getChangeTrackingColumn()
		for i, field := range fieldNames {
			switch field.Name {
			case "id":
				idIndex = i
			case "created_at":
				createdAtIndex = i
			case "updated_at":
				updatedAtIndex = i
			}
			if field.Name == changeCol {
				changeTrackingIndex = i
			}
		}

		rowCount := 0
		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				if p.namespace != "" && p.name != "" {
					metrics.RecordConnectorError(p.namespace, p.name, "postgresql", "source", "read", "scan_error")
				}
				p.logger.Error(err, "Failed to read row values", "table", p.config.Table)
				rows.Close()
				return
			}

			rowMap := make(map[string]interface{})
			for i, field := range fieldNames {
				rowMap[field.Name] = values[i]
			}

			// Determine operation for CDC (insert vs update)
			operation := "insert"
			if updatedAtIndex >= 0 && createdAtIndex >= 0 && len(values) > updatedAtIndex && len(values) > createdAtIndex {
				var updatedAt, createdAt *time.Time
				if ts, ok := values[updatedAtIndex].(time.Time); ok {
					updatedAt = &ts
				}
				if ts, ok := values[createdAtIndex].(time.Time); ok {
					createdAt = &ts
				}
				if updatedAt != nil && createdAt != nil && updatedAt.After(*createdAt) {
					operation = "update"
				}
			}

			// Update last read state
			p.updateLastReadState(values, createdAtIndex, updatedAtIndex, changeTrackingIndex)

			jsonData, err := json.Marshal(rowMap)
			if err != nil {
				if p.namespace != "" && p.name != "" {
					metrics.RecordConnectorError(p.namespace, p.name, "postgresql", "source", "read", "marshal_error")
				}
				p.logger.Error(err, "Failed to marshal row to JSON", "table", p.config.Table)
				continue
			}

			if p.config.RawMode != nil && *p.config.RawMode {
				metadata := map[string]interface{}{"table": p.config.Table}
				if idIndex >= 0 && len(values) > idIndex {
					metadata["id"] = values[idIndex]
				}
				metadata["operation"] = operation
				jsonData, err = buildRawModeJSON(rowMap, metadata)
				if err != nil {
					if p.namespace != "" && p.name != "" {
						metrics.RecordConnectorError(p.namespace, p.name, "postgresql", "source", "read", "marshal_error")
					}
					p.logger.Error(err, "Failed to build raw mode message", "table", p.config.Table)
					continue
				}
			}

			msg := types.NewMessage(jsonData)
			msg.Metadata["table"] = p.config.Table
			if idIndex >= 0 && len(values) > idIndex {
				msg.Metadata["id"] = values[idIndex]
			}
			msg.Metadata["operation"] = operation

			select {
			case msgChan <- msg:
				if p.namespace != "" && p.name != "" {
					metrics.RecordConnectorMessageRead(p.namespace, p.name, "postgresql", "source")
				}
			case <-ctx.Done():
				rows.Close()
				return
			}
			rowCount++
		}
		rows.Close()

		// If we got fewer rows than batch size, no more data in this poll cycle
		if readBatchSize == 0 || rowCount < readBatchSize {
			if rowCount > 0 {
				p.logger.Info("PostgreSQL poll completed", "table", p.config.Table, "rows", rowCount)
			}
			break
		}
	}
}

func (p *PostgreSQLSourceConnector) getChangeTrackingColumn() string {
	if p.config.ChangeTrackingColumn != "" {
		return p.config.ChangeTrackingColumn
	}
	return "updated_at"
}

func (p *PostgreSQLSourceConnector) buildReadQuery() string {
	changeCol := p.getChangeTrackingColumn()
	var orderExpr string
	if changeCol == "updated_at" {
		orderExpr = "COALESCE(updated_at, created_at)"
	} else {
		orderExpr = fmt.Sprintf(`"%s"`, changeCol)
	}
	if p.lastReadChangeTime != nil {
		// RFC3339Nano preserves sub-second precision to avoid re-reading or skipping rows at boundaries
		return fmt.Sprintf("SELECT * FROM %s WHERE %s > '%s' ORDER BY %s, id",
			p.config.Table, orderExpr, p.lastReadChangeTime.UTC().Format(time.RFC3339Nano), orderExpr)
	}
	return fmt.Sprintf("SELECT * FROM %s ORDER BY %s, id", p.config.Table, orderExpr)
}

func (p *PostgreSQLSourceConnector) updateLastReadState(values []interface{}, createdAtIndex, updatedAtIndex, changeTrackingIndex int) {
	// Use change tracking column for lastReadChangeTime
	var changeTime *time.Time
	if changeTrackingIndex >= 0 && len(values) > changeTrackingIndex {
		if ts, ok := values[changeTrackingIndex].(time.Time); ok {
			changeTime = &ts
		}
	}
	// Fallback to COALESCE(updated_at, created_at) when using default column
	if changeTime == nil && p.getChangeTrackingColumn() == "updated_at" {
		if updatedAtIndex >= 0 && len(values) > updatedAtIndex {
			if ts, ok := values[updatedAtIndex].(time.Time); ok {
				changeTime = &ts
			}
		}
		if changeTime == nil && createdAtIndex >= 0 && len(values) > createdAtIndex {
			if ts, ok := values[createdAtIndex].(time.Time); ok {
				changeTime = &ts
			}
		}
	}
	if changeTime != nil && (p.lastReadChangeTime == nil || changeTime.After(*p.lastReadChangeTime)) {
		p.lastReadChangeTime = changeTime
	}
}

// Close closes the PostgreSQL connection
func (p *PostgreSQLSourceConnector) Close() error {
	if p.guardClose() {
		return nil
	}
	defer p.Unlock()

	if p.namespace != "" && p.name != "" {
		metrics.SetConnectorConnectionStatus(p.namespace, p.name, "postgresql", "source", false)
	}
	p.logger.Info("Closing PostgreSQL source connection", "table", p.config.Table)
	if p.conn != nil {
		return p.conn.Close(context.Background())
	}
	return nil
}

// PostgreSQLSinkConnector implements SinkConnector for PostgreSQL
type PostgreSQLSinkConnector struct {
	baseConnector
	connectorLogger
	connectorMetadata
	config            *v1.PostgreSQLSinkSpec
	conn              *pgx.Conn
	firstWriteOnce    sync.Once
	tableExistsCached *bool // Cache to avoid N queries per message (tableExists + hasJSONB check)
	hasJSONBCached    *bool
}

// NewPostgreSQLSinkConnector creates a new PostgreSQL sink connector
func NewPostgreSQLSinkConnector(config *v1.PostgreSQLSinkSpec) *PostgreSQLSinkConnector {
	return &PostgreSQLSinkConnector{
		config:          config,
		connectorLogger: connectorLogger{logger: logr.Discard()},
	}
}

// Connect establishes connection to PostgreSQL
func (p *PostgreSQLSinkConnector) Connect(ctx context.Context) error {
	if !p.guardConnect() {
		return fmt.Errorf("connector is closed")
	}
	defer p.Unlock()

	p.logger.Info("Connecting to PostgreSQL", "table", p.config.Table)
	conn, err := pgx.Connect(ctx, p.config.ConnectionString)
	if err != nil {
		if p.namespace != "" && p.name != "" {
			metrics.RecordConnectorError(p.namespace, p.name, "postgresql", "sink", "connect", "connection_error")
		}
		p.logger.Error(err, "Failed to connect to PostgreSQL", "table", p.config.Table)
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	p.conn = conn
	p.logger.Info("Successfully connected to PostgreSQL", "table", p.config.Table)

	if p.namespace != "" && p.name != "" {
		metrics.SetConnectorConnectionStatus(p.namespace, p.name, "postgresql", "sink", true)
	}

	// Auto-create table if enabled and RawMode (structure known at Connect time)
	if p.config.AutoCreateTable != nil && *p.config.AutoCreateTable && p.rawMode() {
		if err := p.ensureTable(ctx); err != nil {
			if p.namespace != "" && p.name != "" {
				metrics.RecordConnectorError(p.namespace, p.name, "postgresql", "sink", "connect", "ensure_table_error")
			}
			p.logger.Error(err, "Failed to ensure table exists", "table", p.config.Table)
			return fmt.Errorf("failed to ensure table exists: %w", err)
		}
	}

	return nil
}

func (p *PostgreSQLSinkConnector) rawMode() bool {
	return p.config.RawMode != nil && *p.config.RawMode
}

func (p *PostgreSQLSinkConnector) tableExists(ctx context.Context) (bool, error) {
	if p.tableExistsCached != nil {
		return *p.tableExistsCached, nil
	}
	var exists bool
	schema, tableName := ParseTableRef(p.config.Table)
	checkQuery := `SELECT EXISTS (
		SELECT FROM information_schema.tables
		WHERE table_schema = $1
		AND table_name = $2
	)`
	err := p.conn.QueryRow(ctx, checkQuery, schema, tableName).Scan(&exists)
	if err == nil {
		p.tableExistsCached = &exists
	}
	return exists, err
}

func (p *PostgreSQLSinkConnector) hasJSONBColumn(ctx context.Context) (bool, error) {
	if p.hasJSONBCached != nil {
		return *p.hasJSONBCached, nil
	}
	schema, tableName := ParseTableRef(p.config.Table)
	checkQuery := `SELECT EXISTS (
		SELECT FROM information_schema.columns
		WHERE table_schema = $1
		AND table_name = $2
		AND column_name = 'data'
		AND data_type = 'jsonb'
	)`
	var hasJSONB bool
	err := p.conn.QueryRow(ctx, checkQuery, schema, tableName).Scan(&hasJSONB)
	if err == nil {
		p.hasJSONBCached = &hasJSONB
	}
	return hasJSONB, err
}

// ensureTable creates the table if it doesn't exist (RawMode: value + _metadata structure)
func (p *PostgreSQLSinkConnector) ensureTable(ctx context.Context) error {
	exists, err := p.tableExists(ctx)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}
	if exists {
		p.logger.V(1).Info("Table already exists", "table", p.config.Table)
		return nil
	}

	p.logger.Info("Creating table (raw mode)", "table", p.config.Table)
	createQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			value JSONB NOT NULL,
			_metadata JSONB,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			deleted_at TIMESTAMP
		)
	`, p.config.Table)

	_, err = p.conn.Exec(ctx, createQuery)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	trueVal := true
	p.tableExistsCached = &trueVal
	p.hasJSONBCached = nil // raw mode table has value, not data column
	p.logger.Info("Table created successfully", "table", p.config.Table)

	indexQuery := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_value ON %s USING GIN (value)`, p.config.Table, p.config.Table)
	_, err = p.conn.Exec(ctx, indexQuery)
	if err != nil {
		p.logger.Info("Failed to create index (non-critical)", "table", p.config.Table, "error", err)
	}

	return nil
}

// ensureTableFromMessage creates the table from the first message structure (replicates source schema)
func (p *PostgreSQLSinkConnector) ensureTableFromMessage(ctx context.Context, data map[string]interface{}) error {
	exists, err := p.tableExists(ctx)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}
	if exists {
		return nil
	}

	columns := make([]string, 0, len(data))
	for k := range data {
		columns = append(columns, k)
	}
	sort.Strings(columns)
	if len(columns) == 0 {
		return fmt.Errorf("cannot create table from empty message")
	}

	hasColumn := func(name string) bool {
		for _, c := range columns {
			if c == name {
				return true
			}
		}
		return false
	}

	colDefs := make([]string, 0, len(columns)+4)
	for _, col := range columns {
		val := data[col]
		pgType := inferPostgreSQLType(val)
		def := fmt.Sprintf(`"%s" %s`, col, pgType)
		if col == "id" {
			def += " PRIMARY KEY"
		}
		colDefs = append(colDefs, def)
	}
	if !hasColumn("created_at") {
		colDefs = append(colDefs, "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
	}
	if !hasColumn("updated_at") {
		colDefs = append(colDefs, "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
	}
	if p.config.SoftDeleteColumn != nil && *p.config.SoftDeleteColumn != "" && !hasColumn(*p.config.SoftDeleteColumn) {
		colDefs = append(colDefs, *p.config.SoftDeleteColumn+" TIMESTAMP")
	}

	p.logger.Info("Creating table from message structure", "table", p.config.Table, "columns", columns)
	createQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s)`, p.config.Table, joinStrings(colDefs, ", "))
	_, err = p.conn.Exec(ctx, createQuery)
	if err != nil {
		return fmt.Errorf("failed to create table from message: %w", err)
	}
	trueVal := true
	p.tableExistsCached = &trueVal
	p.hasJSONBCached = nil // table created from message has regular columns, not data JSONB
	p.logger.Info("Table created successfully from message structure", "table", p.config.Table)
	return nil
}

func inferPostgreSQLType(v interface{}) string {
	switch v.(type) {
	case nil:
		return "TEXT"
	case bool:
		return "BOOLEAN"
	case int, int32, int64:
		return "BIGINT"
	case float64:
		// Always use NUMERIC for float64 to preserve decimal precision (e.g. price 10.50).
		// Using BIGINT for whole-number floats would truncate decimals in subsequent rows.
		return "NUMERIC"
	case string:
		return "TEXT"
	case map[string]interface{}, []interface{}:
		return "JSONB"
	default:
		return "TEXT"
	}
}

func joinStrings(ss []string, sep string) string {
	if len(ss) == 0 {
		return ""
	}
	s := ss[0]
	for i := 1; i < len(ss); i++ {
		s += sep + ss[i]
	}
	return s
}

// Write writes messages to PostgreSQL
func (p *PostgreSQLSinkConnector) Write(ctx context.Context, messages <-chan *types.Message) error {
	if p.conn == nil {
		return fmt.Errorf("not connected, call Connect first")
	}

	batchSize := 1
	if p.config.BatchSize != nil {
		batchSize = int(*p.config.BatchSize)
	}
	maxBatchSize := batchSize
	if batchSize == 0 {
		maxBatchSize = constants.MaxBatchSizeWhenTimerOnly
	}

	flushIntervalSec := 10
	if p.config.BatchFlushIntervalSeconds != nil {
		flushIntervalSec = int(*p.config.BatchFlushIntervalSeconds)
	}
	useTimer := flushIntervalSec > 0
	flushInterval := time.Duration(flushIntervalSec) * time.Second

	p.logger.Info("Starting to write messages to PostgreSQL", "table", p.config.Table, "batchSize", batchSize, "flushIntervalSeconds", flushIntervalSec)
	batch := &pgx.Batch{}
	batchMessages := make([]*types.Message, 0, maxBatchSize)
	count := 0
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
		if count == 0 {
			return nil
		}
		// Use non-cancelled context for batch execution when ctx is done (e.g. Ctrl+C).
		// pgx closes the connection on context cancellation, causing "conn closed" during
		// br.Close() deallocation. A fresh context allows the flush to complete.
		batchCtx := ctx
		if batchCtx.Err() != nil {
			var cancel context.CancelFunc
			batchCtx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
		}
		if err := retry.OnTimeout(batchCtx, retry.DefaultMaxAttempts, retry.DefaultInitialBackoff, func() error {
			return p.executeBatch(batchCtx, batch)
		}); err != nil {
			if p.namespace != "" && p.name != "" {
				metrics.RecordConnectorError(p.namespace, p.name, "postgresql", "sink", "write", "batch_error")
			}
			return err
		}
		for _, m := range batchMessages {
			if p.namespace != "" && p.name != "" {
				route := getRouteFromMessage(m)
				metrics.RecordConnectorMessageWritten(p.namespace, p.name, "postgresql", "sink", route)
			}
			if m.Ack != nil {
				m.Ack()
			}
		}
		batch = &pgx.Batch{}
		batchMessages = make([]*types.Message, 0, maxBatchSize)
		count = 0
		return nil
	}

	for {
		if useTimer && count > 0 && flushTimer == nil {
			flushTimer = time.NewTimer(flushInterval)
		}

		if useTimer && flushTimer != nil {
			select {
			case <-ctx.Done():
				stopTimer()
				if batch.Len() > 0 {
					p.logger.Info("Context cancelled, flushing batch", "batchSize", batch.Len(), "table", p.config.Table)
					if err := doFlush(); err != nil {
						return err
					}
				}
				return ctx.Err()
			case <-flushTimer.C:
				flushTimer = nil
				if count == 0 {
					continue
				}
				p.logger.V(1).Info("Flush interval reached, executing batch", "batchSize", count, "table", p.config.Table)
				if err := doFlush(); err != nil {
					return err
				}
			case msg, ok := <-messages:
				if !ok {
					stopTimer()
					if batch.Len() > 0 {
						p.logger.Info("Message channel closed, flushing batch", "batchSize", batch.Len(), "totalMessages", messageCount, "table", p.config.Table)
						if err := doFlush(); err != nil {
							return err
						}
					}
					p.logger.Info("Message channel closed", "totalMessages", messageCount, "table", p.config.Table)
					return nil
				}
				messageCount++
				if p.trySoftDelete(msg, batch, &batchMessages, &count) {
					if count >= maxBatchSize {
						if err := doFlush(); err != nil {
							p.logger.Error(err, "Failed to execute batch", "batchSize", count, "table", p.config.Table)
							return err
						}
					}
					continue
				}
				var data map[string]interface{}
				if err := json.Unmarshal(msg.Data, &data); err != nil {
					if p.namespace != "" && p.name != "" {
						metrics.RecordConnectorError(p.namespace, p.name, "postgresql", "sink", "write", "unmarshal_error")
					}
					p.logger.Error(err, "Failed to unmarshal message", logkeys.MessageID, types.MessageID(msg), "table", p.config.Table)
					continue
				}

				p.logger.V(1).Info("Received message for PostgreSQL", logkeys.MessageID, types.MessageID(msg), "messageNumber", messageCount, "table", p.config.Table, "fields", getKeys(data))
				if op, _ := msg.Metadata["operation"].(string); op == "update" {
					p.logger.Info("Applying update (upsert)", logkeys.MessageID, types.MessageID(msg), "messageNumber", messageCount, "table", p.config.Table)
				}

				query, values, err := p.buildInsertForMessage(ctx, data, msg)
				if err != nil {
					if p.namespace != "" && p.name != "" {
						metrics.RecordConnectorError(p.namespace, p.name, "postgresql", "sink", "write", "build_insert_error")
					}
					p.logger.Error(err, "Failed to build insert", logkeys.MessageID, types.MessageID(msg), "table", p.config.Table)
					continue
				}

				batch.Queue(query, values...)
				batchMessages = append(batchMessages, msg)
				count++

				if count >= maxBatchSize {
					p.logger.V(1).Info("Batch size reached, executing batch", "batchSize", count, "table", p.config.Table)
					if err := doFlush(); err != nil {
						p.logger.Error(err, "Failed to execute batch", "batchSize", count, "table", p.config.Table)
						return err
					}
				}
			}
		} else {
			select {
			case <-ctx.Done():
				stopTimer()
				if batch.Len() > 0 {
					p.logger.Info("Context cancelled, flushing batch", "batchSize", batch.Len(), "table", p.config.Table)
					if err := doFlush(); err != nil {
						return err
					}
				}
				return ctx.Err()
			case msg, ok := <-messages:
				if !ok {
					stopTimer()
					if batch.Len() > 0 {
						p.logger.Info("Message channel closed, flushing batch", "batchSize", batch.Len(), "totalMessages", messageCount, "table", p.config.Table)
						if err := doFlush(); err != nil {
							return err
						}
					}
					p.logger.Info("Message channel closed", "totalMessages", messageCount, "table", p.config.Table)
					return nil
				}

				messageCount++
				if p.trySoftDelete(msg, batch, &batchMessages, &count) {
					if count >= maxBatchSize {
						if err := doFlush(); err != nil {
							p.logger.Error(err, "Failed to execute batch", "batchSize", count, "table", p.config.Table)
							return err
						}
					}
					continue
				}
				var data map[string]interface{}
				if err := json.Unmarshal(msg.Data, &data); err != nil {
					if p.namespace != "" && p.name != "" {
						metrics.RecordConnectorError(p.namespace, p.name, "postgresql", "sink", "write", "unmarshal_error")
					}
					p.logger.Error(err, "Failed to unmarshal message", logkeys.MessageID, types.MessageID(msg), "table", p.config.Table)
					continue
				}

				p.logger.V(1).Info("Received message for PostgreSQL", logkeys.MessageID, types.MessageID(msg), "messageNumber", messageCount, "table", p.config.Table, "fields", getKeys(data))
				if op, _ := msg.Metadata["operation"].(string); op == "update" {
					p.logger.Info("Applying update (upsert)", logkeys.MessageID, types.MessageID(msg), "messageNumber", messageCount, "table", p.config.Table)
				}

				query, values, err := p.buildInsertForMessage(ctx, data, msg)
				if err != nil {
					if p.namespace != "" && p.name != "" {
						metrics.RecordConnectorError(p.namespace, p.name, "postgresql", "sink", "write", "build_insert_error")
					}
					p.logger.Error(err, "Failed to build insert", logkeys.MessageID, types.MessageID(msg), "table", p.config.Table)
					continue
				}

				batch.Queue(query, values...)
				batchMessages = append(batchMessages, msg)
				count++

				if count >= maxBatchSize {
					p.logger.V(1).Info("Batch size reached, executing batch", "batchSize", count, "table", p.config.Table)
					if err := doFlush(); err != nil {
						p.logger.Error(err, "Failed to execute batch", "batchSize", count, "table", p.config.Table)
						return err
					}
				}
			}
		}
	}
}

// trySoftDelete handles operation=delete with SoftDeleteColumn. Returns true if message was handled.
func (p *PostgreSQLSinkConnector) trySoftDelete(msg *types.Message, batch *pgx.Batch, batchMessages *[]*types.Message, count *int) bool {
	if p.config.SoftDeleteColumn == nil || *p.config.SoftDeleteColumn == "" {
		return false
	}
	op, _ := msg.Metadata["operation"].(string)
	if op != "delete" {
		return false
	}
	// Get id from metadata or from data
	var idVal interface{}
	if msg.Metadata["id"] != nil {
		idVal = msg.Metadata["id"]
	} else {
		var data map[string]interface{}
		if err := json.Unmarshal(msg.Data, &data); err == nil && data["id"] != nil {
			idVal = data["id"]
		}
	}
	if idVal == nil {
		p.logger.Info("Soft delete skipped: no id in message", logkeys.MessageID, types.MessageID(msg), "table", p.config.Table)
		return false
	}
	conflictKey := "id"
	if p.config.ConflictKey != nil && *p.config.ConflictKey != "" {
		conflictKey = *p.config.ConflictKey
	}
	query := fmt.Sprintf("UPDATE %s SET %s = CURRENT_TIMESTAMP WHERE %s = $1", p.config.Table, *p.config.SoftDeleteColumn, conflictKey)
	batch.Queue(query, idVal)
	*batchMessages = append(*batchMessages, msg)
	*count++
	p.logger.V(1).Info("Soft delete queued", "id", idVal, "table", p.config.Table)
	return true
}

func getKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// buildInsertForMessage ensures table if needed and builds INSERT query and values for the message.
// msg is used when rawMode wraps plain data: msg.Metadata becomes _metadata.
func (p *PostgreSQLSinkConnector) buildInsertForMessage(ctx context.Context, data map[string]interface{}, msg *types.Message) (query string, values []interface{}, err error) {
	if p.config.AutoCreateTable != nil && *p.config.AutoCreateTable {
		exists, e := p.tableExists(ctx)
		if e == nil && !exists {
			if p.rawMode() {
				if e := p.ensureTable(ctx); e != nil {
					return "", nil, e
				}
			} else {
				if e := p.ensureTableFromMessage(ctx, data); e != nil {
					return "", nil, e
				}
			}
		}
	}

	upsertMode := p.config.UpsertMode != nil && *p.config.UpsertMode

	if p.rawMode() {
		var valueJSON, metaJSON []byte
		if data["value"] != nil {
			// Source sent raw format: {"value": ..., "_metadata": ...}
			valueJSON, _ = json.Marshal(data["value"])
			metaJSON, _ = json.Marshal(data["_metadata"])
		} else {
			// Source sent plain format: {"id": 1, "name": "foo", ...} — wrap as value, use msg.Metadata for _metadata
			valueJSON, _ = json.Marshal(data)
			meta := map[string]interface{}{}
			if msg != nil && msg.Metadata != nil {
				for k, v := range msg.Metadata {
					meta[k] = v
				}
			}
			metaJSON, _ = json.Marshal(meta)
		}
		if upsertMode {
			query = fmt.Sprintf("INSERT INTO %s (value, _metadata) VALUES ($1::jsonb, $2::jsonb) ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value, _metadata = EXCLUDED._metadata", p.config.Table)
		} else {
			query = fmt.Sprintf("INSERT INTO %s (value, _metadata) VALUES ($1::jsonb, $2::jsonb)", p.config.Table)
		}
		values = []interface{}{string(valueJSON), string(metaJSON)}
		return query, values, nil
	}

	hasJSONB, e := p.hasJSONBColumn(ctx)
	if e == nil && hasJSONB {
		if upsertMode {
			query = fmt.Sprintf("INSERT INTO %s (data) VALUES ($1::jsonb) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data", p.config.Table)
		} else {
			query = fmt.Sprintf("INSERT INTO %s (data) VALUES ($1::jsonb) ON CONFLICT DO NOTHING", p.config.Table)
		}
		jsonData, _ := json.Marshal(data)
		values = []interface{}{string(jsonData)}
		return query, values, nil
	}

	columns := make([]string, 0, len(data))
	colValues := make([]interface{}, 0, len(data))
	for col, val := range data {
		columns = append(columns, col)
		colValues = append(colValues, val)
	}
	sort.Strings(columns) // stable order for consistent INSERTs and correct upsert
	placeholders := make([]string, 0, len(columns))
	colValues = make([]interface{}, 0, len(columns))
	for i, col := range columns {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		colValues = append(colValues, data[col])
	}
	if len(columns) == 0 {
		return "", nil, fmt.Errorf("empty message, no columns to insert")
	}
	columnList := fmt.Sprintf(`"%s"`, columns[0])
	for i := 1; i < len(columns); i++ {
		columnList += fmt.Sprintf(`, "%s"`, columns[i])
	}
	placeholderList := "$1"
	for i := 2; i <= len(placeholders); i++ {
		placeholderList += fmt.Sprintf(", $%d", i)
	}
	if upsertMode {
		conflictKey := "id"
		if p.config.ConflictKey != nil && *p.config.ConflictKey != "" {
			conflictKey = *p.config.ConflictKey
		}
		updateClauses := make([]string, 0)
		for _, col := range columns {
			if col != conflictKey {
				updateClauses = append(updateClauses, fmt.Sprintf(`"%s" = EXCLUDED."%s"`, col, col))
			}
		}
		if len(updateClauses) == 0 {
			query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO NOTHING", p.config.Table, columnList, placeholderList, conflictKey)
		} else {
			updateClause := updateClauses[0]
			for i := 1; i < len(updateClauses); i++ {
				updateClause += ", " + updateClauses[i]
			}
			query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s", p.config.Table, columnList, placeholderList, conflictKey, updateClause)
		}
	} else {
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING", p.config.Table, columnList, placeholderList)
	}
	values = colValues
	return query, values, nil
}

func (p *PostgreSQLSinkConnector) executeBatch(ctx context.Context, batch *pgx.Batch) error {
	p.logger.V(1).Info("Executing batch", "batchSize", batch.Len(), "table", p.config.Table)
	br := p.conn.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < batch.Len(); i++ {
		_, err := br.Exec()
		if err != nil {
			p.logger.Error(err, "Batch statement failed", "statementIndex", i, "batchSize", batch.Len(), "table", p.config.Table)
			return fmt.Errorf("batch execution error: %w", err)
		}
	}
	p.firstWriteOnce.Do(func() {
		p.logger.Info("First message written to sink", "table", p.config.Table)
	})
	p.logger.V(1).Info("Batch executed successfully", "count", batch.Len(), "table", p.config.Table)
	return nil
}

// Close closes the PostgreSQL connection
func (p *PostgreSQLSinkConnector) Close() error {
	if p.guardClose() {
		return nil
	}
	defer p.Unlock()

	if p.namespace != "" && p.name != "" {
		metrics.SetConnectorConnectionStatus(p.namespace, p.name, "postgresql", "sink", false)
	}
	p.logger.Info("Closing PostgreSQL sink connection", "table", p.config.Table)
	if p.conn != nil {
		return p.conn.Close(context.Background())
	}
	return nil
}
