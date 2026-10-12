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

package checkpoint

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileStore_SaveLoadClear(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nested", "cp.json")
	s, err := NewFileStore(path)
	require.NoError(t, err)

	missing, err := s.Load(ctx, "postgresql")
	require.NoError(t, err)
	assert.Nil(t, missing)

	payload := []byte(`{"orderByValue":"10"}`)
	require.NoError(t, s.Save(ctx, "postgresql", payload))

	reloaded, err := NewFileStore(path)
	require.NoError(t, err)
	got, err := reloaded.Load(ctx, "postgresql")
	require.NoError(t, err)
	assert.JSONEq(t, string(payload), string(got))

	require.NoError(t, reloaded.Save(ctx, "clickhouse", []byte(`{"orderByValue":"2"}`)))
	require.NoError(t, reloaded.Clear(ctx, "postgresql"))

	got, err = reloaded.Load(ctx, "postgresql")
	require.NoError(t, err)
	assert.Nil(t, got)
	got, err = reloaded.Load(ctx, "clickhouse")
	require.NoError(t, err)
	assert.JSONEq(t, `{"orderByValue":"2"}`, string(got))

	onDisk, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.NotContains(t, string(onDisk), "postgresql")
	assert.Contains(t, string(onDisk), "clickhouse")
}

func TestFileStore_ClearMissingFile(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cp.json")
	s, err := NewFileStore(path)
	require.NoError(t, err)
	require.NoError(t, s.Clear(ctx, "postgresql"))
	_, err = os.Stat(path)
	require.NoError(t, err)
}

func TestNewFileStore_EmptyPath(t *testing.T) {
	_, err := NewFileStore("")
	require.Error(t, err)
}
