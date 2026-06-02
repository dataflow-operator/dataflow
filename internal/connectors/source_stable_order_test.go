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

	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestResolveOrderByColumn(t *testing.T) {
	assert.Equal(t, "id", ResolveOrderByColumn(""))
	assert.Equal(t, "price_id", ResolveOrderByColumn("price_id"))
}

func TestWrapQueryStableOrder(t *testing.T) {
	got := WrapQueryStableOrder("SELECT 1", "created_at", "price_id")
	assert.Contains(t, got, "SELECT * FROM (SELECT 1) AS __dataflow_src")
	assert.Contains(t, got, "ORDER BY created_at, price_id")

	assert.Equal(t, "SELECT 1", WrapQueryStableOrder("SELECT 1"))
}

func TestColumnIndex(t *testing.T) {
	names := []string{"a", "price_id", "b"}
	assert.Equal(t, 1, ColumnIndex(names, "price_id"))
	assert.Equal(t, -1, ColumnIndex(names, "missing"))
}

func TestSetSourceRowIDMetadata(t *testing.T) {
	msg := types.NewMessage([]byte(`{}`))
	SetSourceRowIDMetadata(msg, 42)
	assert.Equal(t, 42, msg.Metadata["id"])

	SetSourceRowIDMetadata(msg, nil)
	assert.Equal(t, 42, msg.Metadata["id"])
}
