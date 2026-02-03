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
	"sync"
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/retry"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/jackc/pgx/v5"
)

// PostgreSQLSourceConnector implements SourceConnector for PostgreSQL
type PostgreSQLSourceConnector struct {
	config       *v1.PostgreSQLSourceSpec
	conn         *pgx.Conn
	closed       bool
	mu           sync.Mutex
	lastReadID   int64      // Track last read ID to avoid duplicates
	lastReadTime *time.Time // Track last read time to avoid duplicates
}

// NewPostgreSQLSourceConnector creates a new PostgreSQL source connector
func NewPostgreSQLSourceConnector(config *v1.PostgreSQLSourceSpec) *PostgreSQLSourceConnector {
	return &PostgreSQLSourceConnector{
		config: config,
	}
}

// Connect establishes connection to PostgreSQL
func (p *PostgreSQLSourceConnector) Connect(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return fmt.Errorf("connector is closed")
	}

	conn, err := pgx.Connect(ctx, p.config.ConnectionString)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	p.conn = conn
	return nil
}

// Read returns a channel of messages from PostgreSQL
func (p *PostgreSQLSourceConnector) Read(ctx context.Context) (<-chan *types.Message, error) {
	if p.conn == nil {
		return nil, fmt.Errorf("not connected, call Connect first")
	}

	msgChan := make(chan *types.Message, 100)

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
	p.mu.Lock()
	defer p.mu.Unlock()

	var query string
	if p.config.Query != "" {
		query = p.config.Query
	} else {
		// Add filtering to avoid reading the same rows multiple times
		// Use ID-based filtering if available, otherwise use timestamp
		if p.lastReadID > 0 {
			query = fmt.Sprintf("SELECT * FROM %s WHERE id > %d ORDER BY id", p.config.Table, p.lastReadID)
		} else if p.lastReadTime != nil {
			query = fmt.Sprintf("SELECT * FROM %s WHERE created_at > '%s' ORDER BY created_at, id",
				p.config.Table, p.lastReadTime.Format(time.RFC3339))
		} else {
			// First read - get all rows
			query = fmt.Sprintf("SELECT * FROM %s ORDER BY id", p.config.Table)
		}
	}

	rows, err := p.conn.Query(ctx, query)
	if err != nil {
		return
	}
	defer rows.Close()

	fieldNames := rows.FieldDescriptions()
	var idIndex = -1
	var createdAtIndex = -1
	for i, field := range fieldNames {
		if field.Name == "id" {
			idIndex = i
		}
		if field.Name == "created_at" {
			createdAtIndex = i
		}
	}

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			continue
		}

		rowMap := make(map[string]interface{})
		for i, field := range fieldNames {
			rowMap[field.Name] = values[i]
		}

		// Update last read ID or timestamp
		if idIndex >= 0 {
			if id, ok := values[idIndex].(int64); ok {
				if id > p.lastReadID {
					p.lastReadID = id
				}
			} else if id, ok := values[idIndex].(int32); ok {
				if int64(id) > p.lastReadID {
					p.lastReadID = int64(id)
				}
			}
		}
		if createdAtIndex >= 0 && p.lastReadTime != nil {
			if ts, ok := values[createdAtIndex].(time.Time); ok {
				if ts.After(*p.lastReadTime) {
					p.lastReadTime = &ts
				}
			}
		} else if createdAtIndex >= 0 && p.lastReadTime == nil {
			if ts, ok := values[createdAtIndex].(time.Time); ok {
				p.lastReadTime = &ts
			}
		}

		jsonData, err := json.Marshal(rowMap)
		if err != nil {
			continue
		}

		msg := types.NewMessage(jsonData)
		msg.Metadata["table"] = p.config.Table

		select {
		case msgChan <- msg:
		case <-ctx.Done():
			return
		}
	}
}

// Close closes the PostgreSQL connection
func (p *PostgreSQLSourceConnector) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true
	if p.conn != nil {
		return p.conn.Close(context.Background())
	}
	return nil
}

// PostgreSQLSinkConnector implements SinkConnector for PostgreSQL
type PostgreSQLSinkConnector struct {
	config *v1.PostgreSQLSinkSpec
	conn   *pgx.Conn
	closed bool
	mu     sync.Mutex
}

// NewPostgreSQLSinkConnector creates a new PostgreSQL sink connector
func NewPostgreSQLSinkConnector(config *v1.PostgreSQLSinkSpec) *PostgreSQLSinkConnector {
	return &PostgreSQLSinkConnector{
		config: config,
	}
}

// Connect establishes connection to PostgreSQL
func (p *PostgreSQLSinkConnector) Connect(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return fmt.Errorf("connector is closed")
	}

	conn, err := pgx.Connect(ctx, p.config.ConnectionString)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	p.conn = conn

	// Auto-create table if enabled
	if p.config.AutoCreateTable != nil && *p.config.AutoCreateTable {
		if err := p.ensureTable(ctx); err != nil {
			return fmt.Errorf("failed to ensure table exists: %w", err)
		}
	}

	return nil
}

// ensureTable creates the table if it doesn't exist
func (p *PostgreSQLSinkConnector) ensureTable(ctx context.Context) error {
	// Check if table exists
	var exists bool
	checkQuery := `SELECT EXISTS (
		SELECT FROM information_schema.tables
		WHERE table_schema = 'public'
		AND table_name = $1
	)`
	err := p.conn.QueryRow(ctx, checkQuery, p.config.Table).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}

	if exists {
		return nil
	}

	// Create table with flexible schema for JSON-like data
	// Using JSONB to handle dynamic fields
	createQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			data JSONB NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`, p.config.Table)

	_, err = p.conn.Exec(ctx, createQuery)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Create index on data for better query performance
	indexQuery := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_data ON %s USING GIN (data)`, p.config.Table, p.config.Table)
	_, err = p.conn.Exec(ctx, indexQuery)
	if err != nil {
		// Index creation failure is not critical
		fmt.Printf("WARNING: Failed to create index: %v\n", err)
	}

	return nil
}

// Write writes messages to PostgreSQL
func (p *PostgreSQLSinkConnector) Write(ctx context.Context, messages <-chan *types.Message) error {
	if p.conn == nil {
		return fmt.Errorf("not connected, call Connect first")
	}

	batchSize := 1 // Write immediately for testing, can be increased for production
	if p.config.BatchSize != nil {
		batchSize = int(*p.config.BatchSize)
	}

	batch := &pgx.Batch{}
	count := 0

	for {
		select {
		case <-ctx.Done():
			if batch.Len() > 0 {
				fmt.Printf("DEBUG: Executing final batch on context done, size: %d\n", batch.Len())
				return retry.OnTimeout(ctx, retry.DefaultMaxAttempts, retry.DefaultInitialBackoff, func() error {
					return p.executeBatch(ctx, batch)
				})
			}
			return ctx.Err()
		case msg, ok := <-messages:
			if !ok {
				if batch.Len() > 0 {
					fmt.Printf("DEBUG: Executing final batch on channel close, size: %d\n", batch.Len())
					return retry.OnTimeout(ctx, retry.DefaultMaxAttempts, retry.DefaultInitialBackoff, func() error {
						return p.executeBatch(ctx, batch)
					})
				}
				fmt.Printf("DEBUG: Channel closed, no batch to execute\n")
				return nil
			}

			var data map[string]interface{}
			if err := json.Unmarshal(msg.Data, &data); err != nil {
				fmt.Printf("ERROR: Failed to unmarshal message: %v\n", err)
				continue
			}

			fmt.Printf("DEBUG: Received message, fields: %v\n", getKeys(data))

			// Use JSONB storage if auto-created table, otherwise use column-based
			var query string
			var values []interface{}

			// Check if upsert mode is enabled
			upsertMode := p.config.UpsertMode != nil && *p.config.UpsertMode

			// Check if table has JSONB column (auto-created tables)
			var hasJSONB bool
			checkJSONBQuery := `SELECT EXISTS (
				SELECT FROM information_schema.columns
				WHERE table_schema = 'public'
				AND table_name = $1
				AND column_name = 'data'
				AND data_type = 'jsonb'
			)`
			err := p.conn.QueryRow(ctx, checkJSONBQuery, p.config.Table).Scan(&hasJSONB)
			if err == nil && hasJSONB {
				// Use JSONB storage
				if upsertMode {
					// UPSERT: update data field on conflict with PRIMARY KEY
					query = fmt.Sprintf("INSERT INTO %s (data) VALUES ($1::jsonb) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data", p.config.Table)
				} else {
					query = fmt.Sprintf("INSERT INTO %s (data) VALUES ($1::jsonb) ON CONFLICT DO NOTHING", p.config.Table)
				}
				jsonData, _ := json.Marshal(data)
				values = []interface{}{string(jsonData)}
			} else {
				// Use column-based storage (original logic)
				columns := make([]string, 0, len(data))
				colValues := make([]interface{}, 0, len(data))
				placeholders := make([]string, 0, len(data))

				idx := 1
				for col, val := range data {
					columns = append(columns, col)
					colValues = append(colValues, val)
					placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
					idx++
				}

				if len(columns) == 0 {
					continue // Skip empty messages
				}

				// Build column list for INSERT
				columnList := fmt.Sprintf(`"%s"`, columns[0])
				for i := 1; i < len(columns); i++ {
					columnList += fmt.Sprintf(`, "%s"`, columns[i])
				}

				// Build placeholder list for VALUES
				placeholderList := "$1"
				for i := 2; i <= len(placeholders); i++ {
					placeholderList += fmt.Sprintf(", $%d", i)
				}

				if upsertMode {
					// Determine conflict key
					conflictKey := "id" // Default to PRIMARY KEY
					if p.config.ConflictKey != nil && *p.config.ConflictKey != "" {
						conflictKey = *p.config.ConflictKey
					}

					// Build UPDATE clause for all columns except the conflict key
					updateClauses := make([]string, 0)
					for _, col := range columns {
						if col != conflictKey {
							updateClauses = append(updateClauses, fmt.Sprintf(`"%s" = EXCLUDED."%s"`, col, col))
						}
					}

					if len(updateClauses) == 0 {
						// If only conflict key is present, just do nothing on conflict
						query = fmt.Sprintf(
							"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO NOTHING",
							p.config.Table, columnList, placeholderList, conflictKey,
						)
					} else {
						updateClause := updateClauses[0]
						for i := 1; i < len(updateClauses); i++ {
							updateClause += ", " + updateClauses[i]
						}
						query = fmt.Sprintf(
							"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
							p.config.Table, columnList, placeholderList, conflictKey, updateClause,
						)
					}
				} else {
					query = fmt.Sprintf(
						"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING",
						p.config.Table, columnList, placeholderList,
					)
				}
				values = colValues
			}

			batch.Queue(query, values...)
			count++
			fmt.Printf("DEBUG: Queued message %d/%d in batch\n", count, batchSize)

			if count >= batchSize {
				fmt.Printf("DEBUG: Batch size reached, executing batch\n")
				if err := retry.OnTimeout(ctx, retry.DefaultMaxAttempts, retry.DefaultInitialBackoff, func() error {
					return p.executeBatch(ctx, batch)
				}); err != nil {
					fmt.Printf("ERROR: Batch execution failed: %v\n", err)
					return err
				}
				batch = &pgx.Batch{}
				count = 0
			}
		}
	}
}

func getKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func (p *PostgreSQLSinkConnector) executeBatch(ctx context.Context, batch *pgx.Batch) error {
	fmt.Printf("DEBUG: Executing batch with %d statements\n", batch.Len())
	br := p.conn.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < batch.Len(); i++ {
		_, err := br.Exec()
		if err != nil {
			fmt.Printf("ERROR: Batch statement %d failed: %v\n", i, err)
			return fmt.Errorf("batch execution error: %w", err)
		}
	}
	fmt.Printf("DEBUG: Batch executed successfully, %d statements\n", batch.Len())
	return nil
}

// Close closes the PostgreSQL connection
func (p *PostgreSQLSinkConnector) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true
	if p.conn != nil {
		return p.conn.Close(context.Background())
	}
	return nil
}
