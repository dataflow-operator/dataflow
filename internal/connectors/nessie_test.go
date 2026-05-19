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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildNessieIcebergURI(t *testing.T) {
	tests := []struct {
		name      string
		baseURL   string
		branch    string
		warehouse string
		want      string
	}{
		{"default only", "http://nessie:19120", "", "", "http://nessie:19120/iceberg"},
		{"with branch", "http://nessie:19120", "main", "", "http://nessie:19120/iceberg/main"},
		{"with warehouse", "http://nessie:19120", "", "wh", "http://nessie:19120/iceberg|wh"},
		{"branch and warehouse", "https://nessie.example.com", "dev", "sales", "https://nessie.example.com/iceberg/dev|sales"},
		{"trailing slash base", "http://nessie:19120/", "main", "", "http://nessie:19120/iceberg/main"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildNessieIcebergURI(tt.baseURL, tt.branch, tt.warehouse)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNewNessieSourceConnector(t *testing.T) {
	cfg := &v1.NessieSourceSpec{
		BaseURL:   "http://localhost:19120",
		Namespace: "ns",
		Table:     "t1",
	}
	conn := NewNessieSourceConnector(cfg)
	require.NotNil(t, conn)
	assert.Equal(t, cfg, conn.config)
	conn.SetLogger(logr.Discard())
}

func TestNewNessieSinkConnector(t *testing.T) {
	cfg := &v1.NessieSinkSpec{
		BaseURL:   "http://localhost:19120",
		Namespace: "ns",
		Table:     "t1",
	}
	conn := NewNessieSinkConnector(cfg)
	require.NotNil(t, conn)
	assert.Equal(t, cfg, conn.config)
	conn.SetLogger(logr.Discard())
}

func TestRunNessiePreflight_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/config":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"defaultBranch":"main"}`))
		case "/api/v2/trees/main":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"name":"main"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	err := runNessiePreflight(context.Background(), nessiePreflightConfig{
		baseURL: server.URL,
		branch:  "main",
	})
	require.NoError(t, err)
}

func TestRunNessiePreflight_BranchNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/config":
			w.WriteHeader(http.StatusOK)
		case "/api/v2/trees/missing":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("branch not found"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	err := runNessiePreflight(context.Background(), nessiePreflightConfig{
		baseURL: server.URL,
		branch:  "missing",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "branch \"missing\"")
	assert.Contains(t, err.Error(), "branch not found")
}

func TestRunNessiePreflight_AuthHeaders(t *testing.T) {
	t.Run("bearer token", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if got := r.Header.Get("Authorization"); got != "Bearer token-123" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(fmt.Sprintf("missing bearer header: %q", got)))
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		err := runNessiePreflight(context.Background(), nessiePreflightConfig{
			baseURL:     server.URL,
			bearerToken: "token-123",
		})
		require.NoError(t, err)
	})

	t.Run("basic auth", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			username, password, ok := r.BasicAuth()
			if !ok || username != "alice" || password != "secret" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte("missing basic auth"))
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		err := runNessiePreflight(context.Background(), nessiePreflightConfig{
			baseURL: server.URL,
			basicAuth: &v1.BasicAuthConfig{
				Username: "alice",
				Password: "secret",
			},
		})
		require.NoError(t, err)
	})
}

func TestResolveNessieAuthentication(t *testing.T) {
	basicCfg := &v1.BasicAuthConfig{Username: "alice", Password: "secret"}

	tests := []struct {
		name      string
		authType  v1.NessieAuthenticationType
		token     string
		basic     *v1.BasicAuthConfig
		wantToken string
		wantBasic string
	}{
		{
			name:      "auto prefers bearer token",
			authType:  "",
			token:     "tok",
			basic:     basicCfg,
			wantToken: "tok",
		},
		{
			name:      "auto falls back to basic",
			authType:  "",
			basic:     basicCfg,
			wantBasic: "Basic YWxpY2U6c2VjcmV0",
		},
		{
			name:      "bearer mode uses token",
			authType:  v1.NessieAuthenticationBearer,
			token:     "tok",
			basic:     basicCfg,
			wantToken: "tok",
		},
		{
			name:      "basic mode uses basic",
			authType:  v1.NessieAuthenticationBasic,
			token:     "tok",
			basic:     basicCfg,
			wantBasic: "Basic YWxpY2U6c2VjcmV0",
		},
		{
			name:     "none mode disables auth",
			authType: v1.NessieAuthenticationNone,
			token:    "tok",
			basic:    basicCfg,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotToken, gotBasic := resolveNessieAuthentication(tt.authType, tt.token, tt.basic)
			assert.Equal(t, tt.wantToken, gotToken)
			assert.Equal(t, tt.wantBasic, gotBasic)
		})
	}
}

func TestRunNessiePreflight_InvalidBaseURL(t *testing.T) {
	err := runNessiePreflight(context.Background(), nessiePreflightConfig{
		baseURL: "://bad",
	})
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "invalid baseURL") || strings.Contains(err.Error(), "must include scheme and host"))
}

func TestIsRetryableNessieSnapshotConflict(t *testing.T) {
	t.Run("nil error", func(t *testing.T) {
		assert.False(t, isRetryableNessieSnapshotConflict(nil))
	})

	t.Run("direct conflict message", func(t *testing.T) {
		err := errors.New("Requirement failed: snapshot id changed: expected 1 != 2")
		assert.True(t, isRetryableNessieSnapshotConflict(err))
	})

	t.Run("wrapped conflict message", func(t *testing.T) {
		err := fmt.Errorf("append table: %w", errors.New("requirement failed: SNAPSHOT ID CHANGED: expected 10 != 11"))
		assert.True(t, isRetryableNessieSnapshotConflict(err))
	})

	t.Run("non conflict error", func(t *testing.T) {
		err := errors.New("append table: network timeout")
		assert.False(t, isRetryableNessieSnapshotConflict(err))
	})
}

func TestIsRetryableNessieAppendError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "direct eof",
			err:  io.EOF,
			want: true,
		},
		{
			name: "wrapped eof",
			err:  fmt.Errorf("append table: %w", io.EOF),
			want: true,
		},
		{
			name: "post eof string",
			err:  errors.New(`append table: Post "http://nessie/iceberg/v1/ns/tables/t": EOF`),
			want: true,
		},
		{
			name: "transient connection reset",
			err:  errors.New("append table: connection reset by peer"),
			want: true,
		},
		{
			name: "non retryable validation",
			err:  errors.New("append table: invalid schema evolution"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isRetryableNessieAppendError(tt.err))
		})
	}
}

func TestNessieIcebergSchema(t *testing.T) {
	t.Run("single_data_column", func(t *testing.T) {
		schema := nessieIcebergSchema(false)
		require.NotNil(t, schema)
		assert.Equal(t, 1, schema.NumFields())
		_, ok := schema.FindFieldByName("data")
		assert.True(t, ok)
	})
	t.Run("data_and_metadata", func(t *testing.T) {
		schema := nessieIcebergSchema(true)
		require.NotNil(t, schema)
		assert.Equal(t, 2, schema.NumFields())
		_, okData := schema.FindFieldByName("data")
		_, okMeta := schema.FindFieldByName("_metadata")
		assert.True(t, okData)
		assert.True(t, okMeta)
	})
}

func TestMessagesToArrowTable_RawMode(t *testing.T) {
	t.Run("plain_message_with_metadata", func(t *testing.T) {
		msg := types.NewMessage([]byte(`{"id":1,"event":"login"}`))
		msg.Metadata["offset"] = int64(100)
		msg.Metadata["topic"] = "events"
		msg.Metadata["partition"] = int32(0)

		tbl, err := messagesToArrowTable([]*types.Message{msg}, true)
		require.NoError(t, err)
		defer tbl.Release()
		require.Equal(t, int64(2), tbl.NumCols())
		require.Equal(t, int64(1), tbl.NumRows())

		dataCol := tbl.Column(0).Data().Chunk(0).(*array.String)
		metaCol := tbl.Column(1).Data().Chunk(0).(*array.String)
		assert.JSONEq(t, `{"id":1,"event":"login"}`, dataCol.Value(0))

		var meta map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(metaCol.Value(0)), &meta))
		assert.Equal(t, float64(100), meta["offset"])
		assert.Equal(t, "events", meta["topic"])
		assert.Equal(t, float64(0), meta["partition"])
	})

	t.Run("prewrapped_value_metadata", func(t *testing.T) {
		msg := types.NewMessage([]byte(`{"value":{"id":1},"_metadata":{"offset":10,"topic":"t1"}}`))
		tbl, err := messagesToArrowTable([]*types.Message{msg}, true)
		require.NoError(t, err)
		defer tbl.Release()

		dataCol := tbl.Column(0).Data().Chunk(0).(*array.String)
		metaCol := tbl.Column(1).Data().Chunk(0).(*array.String)
		assert.JSONEq(t, `{"id":1}`, dataCol.Value(0))
		assert.JSONEq(t, `{"offset":10,"topic":"t1"}`, metaCol.Value(0))
	})

	t.Run("non_raw_single_column", func(t *testing.T) {
		msg := types.NewMessage([]byte(`{"id":1}`))
		msg.Metadata["offset"] = int64(1)
		tbl, err := messagesToArrowTable([]*types.Message{msg}, false)
		require.NoError(t, err)
		defer tbl.Release()
		require.Equal(t, int64(1), tbl.NumCols())
		dataCol := tbl.Column(0).Data().Chunk(0).(*array.String)
		assert.JSONEq(t, `{"id":1}`, dataCol.Value(0))
	})
}

func TestValidateNessieRawModeSchema(t *testing.T) {
	t.Run("nil_table", func(t *testing.T) {
		err := validateNessieRawModeSchema(nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil")
	})
}
