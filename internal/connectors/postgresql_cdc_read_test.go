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
	"testing"
	"time"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ SourceReadErrors = (*PostgreSQLCDCSourceConnector)(nil)

func TestPostgreSQLCDCRead_ReadErrorsNilBeforeRead(t *testing.T) {
	t.Parallel()

	c := NewPostgreSQLCDCSourceConnector(&v1.PostgreSQLCDCSourceSpec{
		SlotName:        "slot",
		PublicationName: "pub",
		Tables:          []string{"public.orders"},
	})
	assert.Nil(t, c.ReadErrors())
}

func TestPostgreSQLCDCRead_FatalErrorOnReadErrors(t *testing.T) {
	t.Parallel()

	fatalErr := errors.New("receive replication message: connection reset")
	c := NewPostgreSQLCDCSourceConnector(&v1.PostgreSQLCDCSourceSpec{
		SlotName:        "slot",
		PublicationName: "pub",
		Tables:          []string{"public.orders"},
	})
	c.sqlConn = &pgx.Conn{}
	c.testReadLoop = func(context.Context, chan *types.Message) error {
		return fatalErr
	}

	ctx := context.Background()
	msgCh, err := c.Read(ctx)
	require.NoError(t, err)
	require.NotNil(t, msgCh)

	readErrors := c.ReadErrors()
	require.NotNil(t, readErrors)

	select {
	case got := <-readErrors:
		require.Error(t, got)
		assert.ErrorIs(t, got, fatalErr)
		assert.Contains(t, got.Error(), "postgres CDC read error")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for fatal CDC read error on ReadErrors")
	}

	for range msgCh {
	}
}

func TestPostgreSQLCDCRead_ContextCancelDoesNotSendReadError(t *testing.T) {
	t.Parallel()

	c := NewPostgreSQLCDCSourceConnector(&v1.PostgreSQLCDCSourceSpec{
		SlotName:        "slot",
		PublicationName: "pub",
		Tables:          []string{"public.orders"},
	})
	c.sqlConn = &pgx.Conn{}
	c.testReadLoop = func(ctx context.Context, _ chan *types.Message) error {
		<-ctx.Done()
		return ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	msgCh, err := c.Read(ctx)
	require.NoError(t, err)

	cancel()

	for range msgCh {
	}

	readErrors := c.ReadErrors()
	require.NotNil(t, readErrors)

	select {
	case err := <-readErrors:
		t.Fatalf("expected no error on context cancel, got %v", err)
	case <-time.After(200 * time.Millisecond):
	}
}
