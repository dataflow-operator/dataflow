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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// eofOnceRoundTripper fails the first GET to a Trino nextURI path with unexpected EOF, then delegates.
type eofOnceRoundTripper struct {
	base       http.RoundTripper
	eofEmitted atomic.Bool
}

func (rt *eofOnceRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodGet && isTrinoNextURIPath(req.URL.Path) && !rt.eofEmitted.Swap(true) {
		return nil, fmt.Errorf(`Get %q: unexpected EOF`, req.URL.String())
	}
	return rt.base.RoundTrip(req)
}

func isTrinoNextURIPath(path string) bool {
	return strings.HasPrefix(path, "/v1/statement/") && path != "/v1/statement"
}

func TestTrinoClient_executeQuery_nextURIFollowsContext(t *testing.T) {
	t.Parallel()

	var nextCalls atomic.Int32
	nextPath := "/v1/statement/next/1"
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/statement":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(TrinoQueryResponse{
				NextURI: srv.URL + nextPath,
				Stats:   TrinoStats{State: "RUNNING"},
			})
		default:
			nextCalls.Add(1)
			<-r.Context().Done()
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(srv.Close)

	hc := srv.Client()
	hc.Timeout = 5 * time.Second
	client := newTrinoClientForTest(srv.URL, "memory", "default", hc)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := client.executeQuery(ctx, "INSERT INTO memory.default.t VALUES (1)")
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.GreaterOrEqual(t, nextCalls.Load(), int32(1))
}

func TestTrinoClient_executeQuery_multipleNextURIsCloseBodies(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	next1 := "/v1/statement/next/1"
	next2 := "/v1/statement/next/2"
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		n := requests.Add(1)
		switch n {
		case 1:
			_ = json.NewEncoder(w).Encode(TrinoQueryResponse{
				NextURI: srv.URL + next1,
				Stats:   TrinoStats{State: "RUNNING"},
			})
		case 2:
			_ = json.NewEncoder(w).Encode(TrinoQueryResponse{
				NextURI: srv.URL + next2,
				Stats:   TrinoStats{State: "RUNNING"},
			})
		default:
			_ = json.NewEncoder(w).Encode(TrinoQueryResponse{
				Stats: TrinoStats{State: "FINISHED"},
			})
		}
	}))
	t.Cleanup(srv.Close)

	client := newTrinoClientForTest(srv.URL, "memory", "default", srv.Client())
	_, err := client.executeQuery(context.Background(), "INSERT INTO memory.default.t VALUES (1)")
	require.NoError(t, err)
	assert.Equal(t, int32(3), requests.Load(), "expected POST + two nextURI GETs")
}

func TestTrinoClient_getNextURI_propagatesContextCancel(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	client := newTrinoClientForTest(srv.URL, "memory", "default", srv.Client())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	resp, err := client.getNextURI(ctx, srv.URL+"/next")
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestTrinoClient_executeQuery_retriesNextURIOnUnexpectedEOF(t *testing.T) {
	t.Parallel()

	var nextGETs atomic.Int32
	nextPath := "/v1/statement/next/1"
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/statement":
			_ = json.NewEncoder(w).Encode(TrinoQueryResponse{
				NextURI: srv.URL + nextPath,
				Stats:   TrinoStats{State: "RUNNING"},
			})
		default:
			nextGETs.Add(1)
			_ = json.NewEncoder(w).Encode(TrinoQueryResponse{
				Stats: TrinoStats{State: "FINISHED"},
			})
		}
	}))
	t.Cleanup(srv.Close)

	hc := &http.Client{
		Transport: &eofOnceRoundTripper{base: srv.Client().Transport},
		Timeout:   10 * time.Second,
	}
	client := newTrinoClientForTest(srv.URL, "memory", "default", hc)

	_, err := client.executeQuery(context.Background(), "INSERT INTO memory.default.t VALUES (1)")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, nextGETs.Load(), int32(1), "server should receive at least one successful nextURI GET after retry")
}
