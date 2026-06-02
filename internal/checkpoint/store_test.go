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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoopStore(t *testing.T) {
	ctx := context.Background()
	s := NoopStore{}

	data, err := s.Load(ctx, "postgresql")
	assert.NoError(t, err)
	assert.Nil(t, data)

	err = s.Save(ctx, "postgresql", []byte(`{"lastReadChangeTime":"2024-01-15T10:00:00Z"}`))
	assert.NoError(t, err)

	err = s.Flush(ctx)
	assert.NoError(t, err)
}

func TestExtractSourceCheckpoint(t *testing.T) {
	all := `{"postgresql":{"lastReadChangeTime":"2024-01-15T10:00:00Z"},"clickhouse":{"lastReadID":123}}`

	data, err := extractSourceCheckpoint(all, "postgresql")
	require.NoError(t, err)
	require.NotNil(t, data)
	var m map[string]string
	err = json.Unmarshal(data, &m)
	require.NoError(t, err)
	assert.Equal(t, "2024-01-15T10:00:00Z", m["lastReadChangeTime"])

	data, err = extractSourceCheckpoint(all, "clickhouse")
	require.NoError(t, err)
	require.NotNil(t, data)
	var m2 map[string]int
	err = json.Unmarshal(data, &m2)
	require.NoError(t, err)
	assert.Equal(t, 123, m2["lastReadID"])

	data, err = extractSourceCheckpoint(all, "trino")
	require.NoError(t, err)
	assert.Nil(t, data)
}

func TestMergeCheckpointData(t *testing.T) {
	existing := map[string]string{
		checkpointKey: `{"postgresql":{"lastReadChangeTime":"2024-01-15T09:00:00Z"}}`,
	}
	pending := map[string][]byte{
		"postgresql": []byte(`{"lastReadChangeTime":"2024-01-15T10:00:00Z","lastReadOrderByValue":5042}`),
		"clickhouse": []byte(`{"lastReadChangeTime":"2024-01-15T10:00:00Z","lastReadOrderByValue":456}`),
	}
	merged := mergeCheckpointData(existing, pending)
	var m map[string]json.RawMessage
	err := json.Unmarshal(merged, &m)
	require.NoError(t, err)
	assert.Len(t, m, 2)
	assert.Contains(t, m, "postgresql")
	assert.Contains(t, m, "clickhouse")
}
