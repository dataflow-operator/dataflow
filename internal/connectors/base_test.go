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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildRawModeJSON(t *testing.T) {
	value := map[string]interface{}{"id": 1, "name": "test"}
	metadata := map[string]interface{}{"table": "users", "offset": int64(42)}

	data, err := buildRawModeJSON(value, metadata)
	require.NoError(t, err)

	var parsed map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &parsed))

	assert.Contains(t, parsed, "value")
	assert.Contains(t, parsed, "_metadata")

	meta := parsed["_metadata"].(map[string]interface{})
	assert.Equal(t, "users", meta["table"])
	assert.Equal(t, float64(42), meta["offset"])

	val := parsed["value"].(map[string]interface{})
	assert.Equal(t, float64(1), val["id"])
	assert.Equal(t, "test", val["name"])
}

func TestBaseConnector_GuardConnect_WhenNotClosed(t *testing.T) {
	var b baseConnector
	ok := b.guardConnect()
	require.True(t, ok)
	b.Unlock()
}

func TestBaseConnector_GuardConnect_WhenClosed(t *testing.T) {
	var b baseConnector
	b.closed = true
	ok := b.guardConnect()
	require.False(t, ok)
}

func TestBaseConnector_GuardClose_WhenNotClosed(t *testing.T) {
	var b baseConnector
	alreadyClosed := b.guardClose()
	require.False(t, alreadyClosed)
	assert.True(t, b.closed)
	b.Unlock()
}

func TestBaseConnector_GuardClose_WhenAlreadyClosed(t *testing.T) {
	var b baseConnector
	b.closed = true
	alreadyClosed := b.guardClose()
	require.True(t, alreadyClosed)
}

func TestBaseConnector_LockUnlock(t *testing.T) {
	var b baseConnector
	b.Lock()
	// No deadlock - we hold the lock
	b.Unlock()
}

func TestBaseConnectorRWMutex_GuardConnect_WhenNotClosed(t *testing.T) {
	var b baseConnectorRWMutex
	ok := b.guardConnect()
	require.True(t, ok)
	b.Unlock()
}

func TestBaseConnectorRWMutex_GuardConnect_WhenClosed(t *testing.T) {
	var b baseConnectorRWMutex
	b.closed = true
	ok := b.guardConnect()
	require.False(t, ok)
}

func TestBaseConnectorRWMutex_GuardClose_WhenNotClosed(t *testing.T) {
	var b baseConnectorRWMutex
	alreadyClosed := b.guardClose()
	require.False(t, alreadyClosed)
	assert.True(t, b.closed)
	b.Unlock()
}

func TestBaseConnectorRWMutex_RLock_Closed(t *testing.T) {
	var b baseConnectorRWMutex
	b.RLock()
	assert.False(t, b.Closed())
	b.RUnlock()

	// Set closed under write lock
	b.guardClose()
	b.Unlock()

	b.RLock()
	assert.True(t, b.Closed())
	b.RUnlock()
}

func TestBaseConnectorRWMutex_ConcurrentRLock(t *testing.T) {
	var b baseConnectorRWMutex
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.RLock()
			_ = b.Closed()
			b.RUnlock()
		}()
	}
	wg.Wait()
}

func TestParseTableRef(t *testing.T) {
	tests := []struct {
		table      string
		wantSchema string
		wantName   string
	}{
		{"public.users", "public", "users"},
		{"myschema.mytable", "myschema", "mytable"},
		{"a.b.c", "a.b", "c"},
		{"users", "public", "users"},
		{"", "public", ""},
	}
	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			schema, name := ParseTableRef(tt.table)
			assert.Equal(t, tt.wantSchema, schema)
			assert.Equal(t, tt.wantName, name)
		})
	}
}

func TestQuotePostgreSQLIdentifier(t *testing.T) {
	tests := []struct {
		id   string
		want string
	}{
		{"users", `"users"`},
		{"kafka-to-postgres-raw-events", `"kafka-to-postgres-raw-events"`},
		{"table-name", `"table-name"`},
		{`col"umn`, `"col""umn"`},
		{"simple", `"simple"`},
	}
	for _, tt := range tests {
		t.Run(tt.id, func(t *testing.T) {
			got := quotePostgreSQLIdentifier(tt.id)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestQuotePostgreSQLTableRef(t *testing.T) {
	tests := []struct {
		table string
		want  string
	}{
		{"users", `"public"."users"`},
		{"kafka-to-postgres-raw-events", `"public"."kafka-to-postgres-raw-events"`},
		{"public.events", `"public"."events"`},
		{"myschema.my-table", `"myschema"."my-table"`},
	}
	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			got := QuotePostgreSQLTableRef(tt.table)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPollFailureWait(t *testing.T) {
	base := time.Second
	assert.Equal(t, time.Second, pollFailureWait(base, 1))
	assert.Equal(t, 2*time.Second, pollFailureWait(base, 2))
	assert.Equal(t, 4*time.Second, pollFailureWait(base, 3))
	assert.Equal(t, maxPollingReadBackoff, pollFailureWait(base, 32))
}

func TestShouldLogPollFailure(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	assert.True(t, shouldLogPollFailure(start, start, 1, time.Second))
	assert.False(t, shouldLogPollFailure(start.Add(10*time.Second), start, 2, time.Second))
	assert.True(t, shouldLogPollFailure(start.Add(31*time.Second), start, 2, time.Second))
	assert.True(t, shouldLogPollFailure(start.Add(time.Second), start, 10, time.Second))
}

func TestRunPollingRead_RepeatedErrorsUntilCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	readFn := func(ctx context.Context, ch chan *types.Message) error {
		n := calls.Add(1)
		if n >= 3 {
			cancel()
		}
		return errors.New("poll failed")
	}
	ch := runPollingRead(ctx, 5*time.Millisecond, readFn, 4, nil)
	for range ch {
	}
	require.GreaterOrEqual(t, calls.Load(), int32(3))
}

func TestRunPollingRead_SuccessResetsBackoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var calls atomic.Int32
	readFn := func(ctx context.Context, ch chan *types.Message) error {
		n := calls.Add(1)
		if n == 1 {
			return errors.New("first failure")
		}
		return nil
	}
	ch := runPollingRead(ctx, 10*time.Millisecond, readFn, 4, nil)
	for range ch {
	}
	require.GreaterOrEqual(t, calls.Load(), int32(2))
}
