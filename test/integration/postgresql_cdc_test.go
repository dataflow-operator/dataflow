//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/connectors"
	"github.com/dataflow-operator/dataflow/internal/types"
)

func TestPostgreSQLCDCSourceIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	postgresContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithCmd("postgres",
			"-c", "wal_level=logical",
			"-c", "max_replication_slots=4",
			"-c", "max_wal_senders=4",
		),
	)
	require.NoError(t, err)
	defer func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate postgres container: %v", err)
		}
	}()

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	var conn *pgx.Conn
	for i, delay := 0, 500*time.Millisecond; i < 10; i++ {
		conn, err = pgx.Connect(ctx, connStr)
		if err == nil && conn.Ping(ctx) == nil {
			break
		}
		if conn != nil {
			conn.Close(ctx)
			conn = nil
		}
		time.Sleep(delay)
		delay *= 2
	}
	require.NotNil(t, conn)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, `ALTER USER CURRENT_USER WITH REPLICATION`)
	require.NoError(t, err)

	tableName := "cdc_events"
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			value INTEGER NOT NULL DEFAULT 0
		)
	`, tableName))
	require.NoError(t, err)

	snapshotNever := "never"
	sourceSpec := &v1.PostgreSQLCDCSourceSpec{
		ConnectionString: connStr,
		SlotName:         "dataflow_cdc_test_slot",
		PublicationName:  "dataflow_cdc_test_pub",
		Tables:           []string{"public." + tableName},
		SnapshotMode:     snapshotNever,
	}
	source := connectors.NewPostgreSQLCDCSourceConnector(sourceSpec)
	require.NoError(t, source.Connect(ctx))
	defer source.Close()

	msgChan, err := source.Read(ctx)
	require.NoError(t, err)

	readDone := make(chan []*types.Message, 1)
	go func() {
		var messages []*types.Message
		timeout := time.After(30 * time.Second)
		for {
			select {
			case msg, ok := <-msgChan:
				if !ok {
					readDone <- messages
					return
				}
				messages = append(messages, msg)
			case <-timeout:
				readDone <- messages
				return
			}
		}
	}()

	time.Sleep(2 * time.Second)

	var id int
	err = conn.QueryRow(ctx, fmt.Sprintf(`INSERT INTO %s (name, value) VALUES ('insert_row', 1) RETURNING id`, tableName)).Scan(&id)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, fmt.Sprintf(`UPDATE %s SET name = 'updated_row', value = 2 WHERE id = $1`, tableName), id)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE id = $1`, tableName), id)
	require.NoError(t, err)

	messages := <-readDone
	require.NotEmpty(t, messages)

	ops := map[string]int{}
	for _, msg := range messages {
		op, _ := msg.Metadata["operation"].(string)
		ops[op]++
		var row map[string]interface{}
		require.NoError(t, json.Unmarshal(msg.Data, &row))
		assert.Equal(t, "public."+tableName, msg.Metadata["table"])
	}
	assert.GreaterOrEqual(t, ops["insert"], 1, "expected insert event")
	assert.GreaterOrEqual(t, ops["update"], 1, "expected update event")
	assert.GreaterOrEqual(t, ops["delete"], 1, "expected delete event")
}

func TestPostgreSQLCDCInitialSnapshotIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	postgresContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithCmd("postgres",
			"-c", "wal_level=logical",
			"-c", "max_replication_slots=4",
			"-c", "max_wal_senders=4",
		),
	)
	require.NoError(t, err)
	defer func() {
		_ = postgresContainer.Terminate(ctx)
	}()

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, `ALTER USER CURRENT_USER WITH REPLICATION`)
	require.NoError(t, err)

	tableName := "cdc_snapshot"
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL
		)
	`, tableName))
	require.NoError(t, err)
	_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name) VALUES ('existing'), ('existing2')`, tableName))
	require.NoError(t, err)

	sourceSpec := &v1.PostgreSQLCDCSourceSpec{
		ConnectionString: connStr,
		SlotName:         "dataflow_cdc_snapshot_slot",
		PublicationName:  "dataflow_cdc_snapshot_pub",
		Tables:           []string{"public." + tableName},
		SnapshotMode:     "initial",
	}
	source := connectors.NewPostgreSQLCDCSourceConnector(sourceSpec)
	require.NoError(t, source.Connect(ctx))
	defer source.Close()

	msgChan, err := source.Read(ctx)
	require.NoError(t, err)

	var snapshotRows int
	timeout := time.After(20 * time.Second)
readLoop:
	for {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				break readLoop
			}
			if op, _ := msg.Metadata["operation"].(string); op == "insert" {
				snapshotRows++
			}
			if snapshotRows >= 2 {
				break readLoop
			}
		case <-timeout:
			break readLoop
		}
	}
	assert.GreaterOrEqual(t, snapshotRows, 2, "initial snapshot should emit existing rows")
}

func TestPostgreSQLCDCMultiTableIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	postgresContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithCmd("postgres",
			"-c", "wal_level=logical",
			"-c", "max_replication_slots=4",
			"-c", "max_wal_senders=4",
		),
	)
	require.NoError(t, err)
	defer func() { _ = postgresContainer.Terminate(ctx) }()

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, `ALTER USER CURRENT_USER WITH REPLICATION`)
	require.NoError(t, err)

	for _, ddl := range []string{
		`CREATE TABLE cdc_orders (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL)`,
		`CREATE TABLE cdc_customers (id SERIAL PRIMARY KEY, email VARCHAR(100) NOT NULL)`,
	} {
		_, err = conn.Exec(ctx, ddl)
		require.NoError(t, err)
	}

	snapshotNever := "never"
	sourceSpec := &v1.PostgreSQLCDCSourceSpec{
		ConnectionString: connStr,
		SlotName:         "dataflow_cdc_multi_slot",
		PublicationName:  "dataflow_cdc_multi_pub",
		Tables:           []string{"public.cdc_orders", "public.cdc_customers"},
		SnapshotMode:     snapshotNever,
	}
	source := connectors.NewPostgreSQLCDCSourceConnector(sourceSpec)
	require.NoError(t, source.Connect(ctx))
	defer source.Close()

	msgChan, err := source.Read(ctx)
	require.NoError(t, err)

	readDone := make(chan []*types.Message, 1)
	go func() {
		var messages []*types.Message
		timeout := time.After(30 * time.Second)
		for {
			select {
			case msg, ok := <-msgChan:
				if !ok {
					readDone <- messages
					return
				}
				messages = append(messages, msg)
				if len(messages) >= 2 {
					readDone <- messages
					return
				}
			case <-timeout:
				readDone <- messages
				return
			}
		}
	}()

	time.Sleep(2 * time.Second)

	_, err = conn.Exec(ctx, `INSERT INTO cdc_orders (name) VALUES ('order1')`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `INSERT INTO cdc_customers (email) VALUES ('a@example.com')`)
	require.NoError(t, err)

	messages := <-readDone
	require.Len(t, messages, 2)

	tablesSeen := map[string]bool{}
	for _, msg := range messages {
		table, _ := msg.Metadata["table"].(string)
		tablesSeen[table] = true
		assert.Equal(t, "insert", msg.Metadata["operation"])
	}
	assert.True(t, tablesSeen["public.cdc_orders"])
	assert.True(t, tablesSeen["public.cdc_customers"])
}

func TestPostgreSQLCDCSnapshotThenStreamIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	postgresContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithCmd("postgres",
			"-c", "wal_level=logical",
			"-c", "max_replication_slots=4",
			"-c", "max_wal_senders=4",
		),
	)
	require.NoError(t, err)
	defer func() { _ = postgresContainer.Terminate(ctx) }()

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, `ALTER USER CURRENT_USER WITH REPLICATION`)
	require.NoError(t, err)

	tableName := "cdc_snap_stream"
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL
		)
	`, tableName))
	require.NoError(t, err)
	_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name) VALUES ('seed')`, tableName))
	require.NoError(t, err)

	sourceSpec := &v1.PostgreSQLCDCSourceSpec{
		ConnectionString: connStr,
		SlotName:         "dataflow_cdc_snap_stream_slot",
		PublicationName:  "dataflow_cdc_snap_stream_pub",
		Tables:           []string{"public." + tableName},
		SnapshotMode:     "initial",
	}
	source := connectors.NewPostgreSQLCDCSourceConnector(sourceSpec)
	require.NoError(t, source.Connect(ctx))
	defer source.Close()

	msgChan, err := source.Read(ctx)
	require.NoError(t, err)

	var snapshotDone bool
	var streamInsert bool
	timeout := time.After(45 * time.Second)
readLoop:
	for {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				break readLoop
			}
			op, _ := msg.Metadata["operation"].(string)
			if op == "insert" && !snapshotDone {
				snapshotDone = true
				_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name) VALUES ('after_snapshot')`, tableName))
				require.NoError(t, err)
				continue
			}
			if op == "insert" && snapshotDone {
				var row map[string]interface{}
				require.NoError(t, json.Unmarshal(msg.Data, &row))
				if name, _ := row["name"].(string); name == "after_snapshot" {
					streamInsert = true
					break readLoop
				}
			}
		case <-timeout:
			break readLoop
		}
	}
	assert.True(t, snapshotDone, "expected initial snapshot row")
	assert.True(t, streamInsert, "expected streaming insert after snapshot")
}
