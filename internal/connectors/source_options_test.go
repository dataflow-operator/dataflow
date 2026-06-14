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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadInitialCheckpoint_normalizesLegacy(t *testing.T) {
	store := &normalizeTestStore{data: map[string][]byte{
		"trino": []byte(`{"lastReadID":42}`),
	}}
	out, err := LoadInitialCheckpoint(context.Background(), store, "trino")
	require.NoError(t, err)
	assert.Contains(t, string(out), "lastReadOrderByValue")
	assert.NotContains(t, string(out), "lastReadID")
}

type normalizeTestStore struct {
	data map[string][]byte
}

func (s *normalizeTestStore) Load(_ context.Context, sourceType string) ([]byte, error) {
	return s.data[sourceType], nil
}

func (s *normalizeTestStore) Save(context.Context, string, []byte) error { return nil }
func (s *normalizeTestStore) Flush(context.Context) error                { return nil }
func (s *normalizeTestStore) Clear(context.Context, string) error        { return nil }
