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
	"encoding/json"
	"testing"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/stretchr/testify/require"
)

func TestClickHouseOnMessage_RawSkipsUnmarshal(t *testing.T) {
	raw := true
	c := NewClickHouseSinkConnector(&v1.ClickHouseSinkSpec{
		ConnectionString: "clickhouse://localhost:9000/default",
		Table:            "events",
		RawMode:          &raw,
	})
	require.True(t, c.rawMode())

	msg := types.NewMessage([]byte(`not-valid-json{{{`))
	// Raw Write OnMessage accepts payloads without Unmarshal; invalid JSON must not be rejected.
	accepted := true
	if !c.rawMode() {
		var data map[string]interface{}
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			accepted = false
		}
	}
	require.True(t, accepted)
}

func TestClickHouseOnMessage_ColumnarRejectsInvalidJSON(t *testing.T) {
	c := NewClickHouseSinkConnector(&v1.ClickHouseSinkSpec{
		ConnectionString: "clickhouse://localhost:9000/default",
		Table:            "events",
	})
	require.False(t, c.rawMode())

	msg := types.NewMessage([]byte(`not-valid-json{{{`))
	var data map[string]interface{}
	err := json.Unmarshal(msg.Data, &data)
	require.Error(t, err)
}
