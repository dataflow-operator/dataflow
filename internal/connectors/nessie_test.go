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
	"testing"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
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
