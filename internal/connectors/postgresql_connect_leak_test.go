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
	"errors"
	"fmt"
	"testing"
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestPostgreSQLConnect_InvalidConnectionStringLeavesConnNil(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	invalidConn := "postgres://invalid-host:5432/nodb?connect_timeout=1"

	t.Run("source", func(t *testing.T) {
		t.Parallel()
		p := NewPostgreSQLSourceConnector(&v1.PostgreSQLSourceSpec{
			ConnectionString: invalidConn,
			Table:            "events",
		})
		p.SetLogger(logr.Discard())
		err := p.Connect(ctx)
		require.Error(t, err)
		assert.Nil(t, p.conn)
	})

	t.Run("sink", func(t *testing.T) {
		t.Parallel()
		p := NewPostgreSQLSinkConnector(&v1.PostgreSQLSinkSpec{
			ConnectionString: invalidConn,
			Table:            "events",
		})
		p.SetLogger(logr.Discard())
		err := p.Connect(ctx)
		require.Error(t, err)
		assert.Nil(t, p.conn)
	})

	t.Run("cdc", func(t *testing.T) {
		t.Parallel()
		c := NewPostgreSQLCDCSourceConnector(&v1.PostgreSQLCDCSourceSpec{
			ConnectionString: invalidConn,
			SlotName:         "slot",
			PublicationName:  "pub",
			Tables:           []string{"public.events"},
		})
		c.SetLogger(logr.Discard())
		err := c.Connect(ctx)
		require.Error(t, err)
		assert.Nil(t, c.sqlConn)
	})
}

func TestPostgreSQLConnect_PartialFailureReleasesConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker testcontainers")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	postgresContainer, connStr, adminConn := startPostgreSQLForConnectLeakTest(t, ctx)
	if postgresContainer == nil {
		return
	}
	defer func() {
		adminConn.Close(ctx)
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate postgres container: %v", err)
		}
	}()

	setupErr := errors.New("simulated post-connect setup failure")
	replErr := errors.New("simulated replication connect failure")

	t.Run("source", func(t *testing.T) {
		before := countPostgreSQLBackends(t, ctx, adminConn)
		p := NewPostgreSQLSourceConnector(&v1.PostgreSQLSourceSpec{
			ConnectionString: connStr,
			Table:            "events",
		})
		p.SetLogger(logr.Discard())
		p.testPostConnectSetup = func(context.Context) error { return setupErr }

		err := p.Connect(ctx)
		require.ErrorIs(t, err, setupErr)
		assert.Nil(t, p.conn)
		require.NoError(t, p.Close())

		assert.Eventually(t, func() bool {
			return countPostgreSQLBackends(t, ctx, adminConn) <= before
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("sink", func(t *testing.T) {
		before := countPostgreSQLBackends(t, ctx, adminConn)
		p := NewPostgreSQLSinkConnector(&v1.PostgreSQLSinkSpec{
			ConnectionString: connStr,
			Table:            "events",
		})
		p.SetLogger(logr.Discard())
		p.testPostConnectSetup = func(context.Context) error { return setupErr }

		err := p.Connect(ctx)
		require.ErrorIs(t, err, setupErr)
		assert.Nil(t, p.conn)
		require.NoError(t, p.Close())

		assert.Eventually(t, func() bool {
			return countPostgreSQLBackends(t, ctx, adminConn) <= before
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("cdc_replication_failure", func(t *testing.T) {
		_, err := adminConn.Exec(ctx, `
			CREATE TABLE IF NOT EXISTS cdc_connect_leak (
				id SERIAL PRIMARY KEY,
				name TEXT NOT NULL
			)
		`)
		require.NoError(t, err)

		before := countPostgreSQLBackends(t, ctx, adminConn)
		snapshotNever := "never"
		c := NewPostgreSQLCDCSourceConnector(&v1.PostgreSQLCDCSourceSpec{
			ConnectionString: connStr,
			SlotName:         "cdc_connect_leak_slot",
			PublicationName:  "cdc_connect_leak_pub",
			Tables:           []string{"public.cdc_connect_leak"},
			SnapshotMode:     snapshotNever,
		})
		c.SetLogger(logr.Discard())
		c.testConnectRepl = func(context.Context, string) (*pgconn.PgConn, error) {
			return nil, replErr
		}

		err = c.Connect(ctx)
		require.ErrorIs(t, err, replErr)
		assert.Nil(t, c.sqlConn)
		require.NoError(t, c.Close())

		assert.Eventually(t, func() bool {
			return countPostgreSQLBackends(t, ctx, adminConn) <= before
		}, 10*time.Second, 100*time.Millisecond)
	})
}

func startPostgreSQLForConnectLeakTest(t *testing.T, ctx context.Context) (*postgres.PostgresContainer, string, *pgx.Conn) {
	t.Helper()

	var (
		postgresContainer *postgres.PostgresContainer
		err               error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("docker unavailable: %v", r)
			}
		}()
		postgresContainer, err = postgres.RunContainer(ctx,
			testcontainers.WithCmd("postgres",
				"-c", "wal_level=logical",
				"-c", "max_replication_slots=4",
				"-c", "max_wal_senders=4",
			),
		)
	}()
	if err != nil {
		t.Skipf("requires Docker testcontainers: %v", err)
		return nil, "", nil
	}

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	var adminConn *pgx.Conn
	for i, delay := 0, 500*time.Millisecond; i < 10; i++ {
		adminConn, err = pgx.Connect(ctx, connStr)
		if err == nil && adminConn.Ping(ctx) == nil {
			break
		}
		if adminConn != nil {
			adminConn.Close(ctx)
			adminConn = nil
		}
		time.Sleep(delay)
		delay *= 2
	}
	require.NotNil(t, adminConn, "postgres container did not become ready")

	_, err = adminConn.Exec(ctx, `ALTER USER CURRENT_USER WITH REPLICATION`)
	require.NoError(t, err)

	return postgresContainer, connStr, adminConn
}

func countPostgreSQLBackends(t *testing.T, ctx context.Context, conn *pgx.Conn) int {
	t.Helper()
	var count int
	err := conn.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_stat_activity
		WHERE datname = current_database()
		  AND pid <> pg_backend_pid()
	`).Scan(&count)
	require.NoError(t, err)
	return count
}

func TestPostgreSQLCDCSourceConnector_connectReplication_UsesTestHook(t *testing.T) {
	t.Parallel()
	c := NewPostgreSQLCDCSourceConnector(&v1.PostgreSQLCDCSourceSpec{
		ConnectionString: "postgres://unused",
		SlotName:         "slot",
		PublicationName:  "pub",
	})
	c.testConnectRepl = func(_ context.Context, connString string) (*pgconn.PgConn, error) {
		return nil, fmt.Errorf("hook called with %q", connString)
	}

	_, err := c.connectReplication(context.Background(), "postgres://example?replication=database")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "hook called")
}
