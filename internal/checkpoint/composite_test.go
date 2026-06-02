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
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseComposite_canonical(t *testing.T) {
	c, err := ParseComposite([]byte(`{"lastReadChangeTime":"2024-06-01T12:00:00.123456789Z","lastReadOrderByValue":5042}`))
	require.NoError(t, err)
	require.NotNil(t, c.ChangeTime)
	assert.Equal(t, int64(5042), int64(c.OrderByValue.(float64)))
}

func TestParseComposite_trinoLegacy(t *testing.T) {
	c, err := ParseComposite([]byte(`{"lastReadID":100}`))
	require.NoError(t, err)
	assert.Nil(t, c.ChangeTime)
	assert.Equal(t, float64(100), c.OrderByValue)
}

func TestParseComposite_clickhouseLegacyDual(t *testing.T) {
	c, err := ParseComposite([]byte(`{"lastReadID":100,"lastReadTime":"2024-06-01 12:00:00"}`))
	require.NoError(t, err)
	require.NotNil(t, c.ChangeTime)
	assert.Equal(t, 2024, c.ChangeTime.Year())
	assert.Equal(t, float64(100), c.OrderByValue)
}

func TestNormalizeCheckpoint_trinoLegacy(t *testing.T) {
	out := NormalizeCheckpoint([]byte(`{"lastReadID":100}`))
	var m map[string]interface{}
	require.NoError(t, json.Unmarshal(out, &m))
	assert.Equal(t, float64(100), m["lastReadOrderByValue"])
	assert.NotContains(t, m, "lastReadID")
	assert.NotContains(t, m, "lastReadChangeTime")
}

func TestNormalizeCheckpoint_clickhouseLegacy(t *testing.T) {
	out := NormalizeCheckpoint([]byte(`{"lastReadID":100,"lastReadTime":"2024-06-01 12:00:00"}`))
	var m map[string]interface{}
	require.NoError(t, json.Unmarshal(out, &m))
	assert.Equal(t, float64(100), m["lastReadOrderByValue"])
	assert.Contains(t, m, "lastReadChangeTime")
	assert.NotContains(t, m, "lastReadID")
	assert.NotContains(t, m, "lastReadTime")
}

func TestShouldAdvance(t *testing.T) {
	t1 := time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)

	cur := Composite{ChangeTime: &t1, OrderByValue: int64(1)}
	assert.True(t, ShouldAdvance(cur, Composite{ChangeTime: &t2, OrderByValue: int64(1)}))
	assert.False(t, ShouldAdvance(cur, Composite{ChangeTime: &t1, OrderByValue: int64(1)}))
	assert.True(t, ShouldAdvance(cur, Composite{ChangeTime: &t1, OrderByValue: int64(99)}))
	assert.True(t, ShouldAdvance(Composite{}, Composite{OrderByValue: int64(1)}))
}

func TestCompareOrderBy(t *testing.T) {
	assert.Equal(t, 0, CompareOrderBy(int64(5), int64(5)))
	assert.Equal(t, 1, CompareOrderBy(int64(10), int64(5)))
	assert.Equal(t, -1, CompareOrderBy("a", "b"))
}

func TestCompositeMarshal(t *testing.T) {
	ts := time.Date(2024, 6, 1, 12, 0, 0, 123456789, time.UTC)
	data := Composite{ChangeTime: &ts, OrderByValue: int64(5042)}.Marshal()
	var m map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &m))
	assert.Contains(t, m["lastReadChangeTime"].(string), "2024-06-01T12:00:00.123456789Z")
	assert.Equal(t, float64(5042), m["lastReadOrderByValue"])
}
